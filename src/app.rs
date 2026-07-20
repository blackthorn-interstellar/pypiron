use std::{
    io::{IsTerminal, Write},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, Multipart, Path, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, Response, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use base64::engine::general_purpose::STANDARD as b64;
use base64::Engine;
use clap::{CommandFactory, FromArgMatches};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::{debug, info, warn};

// Sibling modules are declared at the crate root (src/lib.rs); import them by
// name so the bare `worker::`, `storage::`, … paths throughout this file (from
// its life as the crate root) keep resolving.
use crate::{
    advisories, bucket_health, buckets, cache, config, counters, metrics, names, node_region,
    observed_storage, origin, project_cache, proxy, render, replicate, sidecar, status, storage,
    sync, token, tombstone, transparency, upload, verify, web, wheel, worker,
};

use bucket_health::{HealthController, HealthPolicy};
use buckets::{BucketHandle, BucketSet, Pinned};
use names::{
    checked_pkg_name, infer_package_from_filename, infer_version_from_filename, is_normalized,
    normalize_pkg_name,
};
use sidecar::{
    frozen_key, metadata_key, mirror_quarantined_key, provenance_key, sidecar_key, tombstone_key,
    Sidecar, Yanked, METADATA_SUFFIX, PROVENANCE_SUFFIX,
};
use storage::Storage;

use crate::cli::{
    apply_maintenance_config, apply_nested_maintenance_config, arg_from_cli_or_env,
    merge_serve_file, run_buckets_migrate, run_create_token, run_healthcheck, run_origin_release,
    BucketsCommand, Cli, Commands, ConfigCommand, LogFormat, OriginCommand, RebuildIndexArgs,
    ServeArgs,
};

use crate::pages::{
    downloads_page, html_ok, page_context, project_page, project_version_page, projects_page,
    rank_packages, root,
};

pub const PACKAGES_PREFIX: &str = "packages/";
pub const SIMPLE_PREFIX: &str = "simple/";
pub const DIRTY_PREFIX: &str = "_dirty/";

/// The git commit baked in at build time (see `build.rs`); `unknown` when built
/// without git (e.g. from an sdist).
const GIT_HASH: &str = env!("PYPIRON_GIT_HASH");
/// Crate version plus the commit it was built from, e.g. `0.0.0 (abc1234)`.
/// One string for `--version`, the startup banner, and the web footer.
pub(crate) const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("PYPIRON_GIT_HASH"),
    ")"
);

/// One-shot deep audit against a storage backend, no server attached.
async fn run_rebuild_index(args: RebuildIndexArgs) -> Result<()> {
    let storage = args.storage.build().await?;
    let state = AppState::headless(storage);
    let pinned = state.pin();
    worker::audit(&state, &pinned, true).await
}

/// How artifact bytes reach clients. The tension: redirects move the
/// megabytes to S3, but a fresh presigned URL per request defeats any client
/// cache keyed by the final URL (pip's HTTP cache re-downloads every wheel),
/// while streaming keeps every cache effective at the cost of this node
/// serving the bytes. Index pages always carry stable `/files/` URLs; this
/// only governs what happens when a client GETs one.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactDelivery {
    /// Per-client: redirect clients verified immune to presigned-URL churn
    /// (uv keys its cache by index + filename), stream everyone else.
    Auto,
    /// Always 302 to presigned S3 URLs; this node never touches wheel bytes.
    Redirect,
    /// Always proxy bytes through this node with immutable cache headers.
    Stream,
}

/// User-Agent prefixes of clients whose artifact caches are keyed by package
/// filename rather than the URL that served the bytes, verified to follow
/// cross-host 302s. Only such clients may be redirected in `auto` mode —
/// anyone else (pip's CacheControl keys on the per-hop URL; unknown tools are
/// assumed to as well) gets streamed bytes under the stable `/files/` URL.
/// Grow this list by verified cache behavior, not by client popularity.
const REDIRECT_SAFE_UA_PREFIXES: &[&str] = &["uv/"];

/// Whole-request deadline for ordinary (small, fast) requests. Hyper 1.x has no
/// default body-read timeout, so without this a trickled or stalled request
/// (slowloris) pins a connection forever; this bounds it. Generous enough that
/// a slow `uv` resolve or a large index render never trips it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Deadline for the streaming routes — uploads and artifact downloads move
/// bodies up to the 1 GiB limit, so a real large wheel over a slow link needs
/// far longer than a read. Still finite: a trickled transfer can't hold a
/// connection (and an upload's spool fd) indefinitely.
const STREAMING_REQUEST_TIMEOUT: Duration = Duration::from_secs(3600);

/// On a graceful shutdown of a cloud-backed (multi-node) deployment, fail
/// `/health` for this long *before* the listener stops accepting, so a load
/// balancer pulls the node from rotation instead of routing new requests into
/// connection-refused. Skipped on disk (single-node) — see the shutdown path.
const PRE_DRAIN_PAUSE: Duration = Duration::from_secs(3);

/// How the per-request access log is rendered (only when `--access-log` is set).
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLogFormat {
    /// One structured `tracing` event per request on the `pypiron::access`
    /// target — key=value in text mode, a JSON object under `--log-format json`.
    Structured,
    /// Combined Log Format, written straight to stdout for log tooling
    /// (GoAccess/lnav/awstats). Bypasses the diagnostic log's timestamp+level
    /// prefix, which those parsers can't read.
    Clf,
}

/// CLF timestamp, e.g. `10/Oct/2000:13:55:36 +0000`. We always log in UTC, so
/// the offset is fixed.
const CLF_TIME: &[time::format_description::FormatItem<'_>] = time::macros::format_description!(
    "[day]/[month repr:short]/[year]:[hour]:[minute]:[second] +0000"
);

fn redirect_safe_client(headers: &HeaderMap) -> bool {
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    REDIRECT_SAFE_UA_PREFIXES.iter().any(|p| ua.starts_with(p))
}

/// Shared TTL cache of the ranked download leaderboard: `(computed_at, board)`,
/// or `None` until first populated.
type DownloadBoard = Arc<std::sync::Mutex<Option<(std::time::Instant, Vec<(String, u64)>)>>>;
type EmptyOriginObservations =
    Arc<tokio::sync::Mutex<std::collections::HashMap<(u64, String), (String, std::time::Instant)>>>;

#[derive(Clone)]
pub struct AppState {
    /// All configured buckets and the currently-selected one. There is no
    /// ambient storage handle: every operation calls [`AppState::pin`] once at
    /// entry (design §3) and threads that immutable context down, so a selection
    /// switch can never tear an in-flight operation. Single-bucket deployments
    /// hold exactly one bucket and the pin is always index 0, generation 0.
    pub buckets: Arc<BucketSet>,
    /// Multi-bucket-only per-node availability view. `None` preserves the
    /// single-bucket hot path: no observation wrapper, probes, or background
    /// selection work.
    pub bucket_health: Option<Arc<HealthController>>,
    /// A runtime topology mismatch is sticky: reads remain available, but every
    /// HTTP mutation is rejected until the operator fixes configuration and
    /// restarts. The worker is the only writer of this fence.
    pub writes_fenced: Arc<std::sync::atomic::AtomicBool>,
    // auth — two roles: uploader (publish) and admin (everything, incl. mirror,
    // delete, yank). Admin is a strict superset of uploader.
    pub uploader_user: Option<String>,
    pub uploader_pass: Option<String>,
    pub admin_user: Option<String>,
    pub admin_pass: Option<String>,
    // read credential — when configured, index and artifact reads require it
    // (or any stronger credential).
    pub read_user: Option<String>,
    pub read_pass: Option<String>,
    /// Secret for signing/verifying stateless install tokens. None disables
    /// token auth entirely (mint endpoint refuses, `__token__` never verifies).
    pub token_signing_key: Option<String>,
    pub private_prefix: Option<String>,
    pub artifact_delivery: ArtifactDelivery,
    /// Attach per-client `project` labels to `/metrics`. Off by default because
    /// `/metrics` carries no auth; see the flag of the same name.
    pub metrics_project_labels: bool,
    /// Widen the access log from mutations-only (the default) to every request.
    /// See `log_requests`.
    pub access_log: bool,
    pub access_log_format: AccessLogFormat,
    // worker cfg
    pub worker_interval: Duration,
    pub reconcile_interval: Duration,
    /// Periodic backstop cadence for the `_repl/` repair-note sweep, decoupled
    /// from `worker_interval` so the 1 s tick never drives `_repl/` LISTs. The
    /// sweep also fires immediately when an unhealthy bucket heals (see
    /// `repl_sweep_requested`).
    pub repl_sweep_interval: Duration,
    /// Set by the health loop on a bucket's unhealthy→healthy transition to make
    /// the worker sweep `_repl/` at once (drain starts seconds after heal),
    /// instead of waiting out `repl_sweep_interval`. Cleared when a sweep starts.
    pub repl_sweep_requested: Arc<std::sync::atomic::AtomicBool>,
    /// Unix seconds of the last client (non-health/metrics) request, touched
    /// lock-free by the request path. The multi-bucket health loop reads it to
    /// gate probe cadence: full speed with recent traffic, decayed when idle.
    pub last_request_unix: Arc<std::sync::atomic::AtomicU64>,
    /// Grace a synchronous pre-ack fan-out gives each secondary bucket, measured
    /// from the selected bucket's write. A secondary that misses it gets a
    /// `_repl/` repair note instead of blocking the ack (dev/MULTIBUCKET.md).
    pub fanout_grace: Duration,
    /// How long an unpaired intent marker may sit before the worker treats
    /// its writer as crashed and rebuilds anyway. time::Duration because it
    /// is compared against storage timestamps.
    pub intent_grace: time::Duration,
    pub audit_on_boot: bool,
    /// Whether the leader audit appends tamper-evident checkpoint links under
    /// `_transparency/`. Off stops new links; verify-chain still reads the chain.
    pub transparency: bool,
    pub lease_ttl: Duration,
    pub wait_on_upload: bool,
    pub wait_on_upload_timeout: Duration,
    /// RAM-served indexes with precomputed ETags; see cache.rs.
    pub index_cache: Arc<cache::IndexCache>,
    /// Reused presigned GET URLs for immutable artifacts; see cache.rs.
    pub presign_cache: Arc<cache::PresignCache>,
    /// RAM-served rendered `/project/<pkg>/` pages; see project_cache.rs. Spares
    /// the human project page a full package-prefix scan + sidecar parse per hit.
    pub project_cache: Arc<project_cache::ProjectCache>,
    /// Where upload spools live (must be real disk, not tmpfs).
    pub spool_dir: std::path::PathBuf,
    /// In-memory global-index name set + the lock serializing its writes.
    pub global_names: Arc<tokio::sync::Mutex<Option<worker::GlobalNames>>>,
    /// In-memory per-package inventory: the working set behind the storage view
    /// `_state/inventory.json`. The leader maintains it on every rebuild and
    /// re-baselines it each sweep; followers read the persisted view.
    pub inventory: Arc<tokio::sync::Mutex<worker::InventoryMap>>,
    /// Wakes the worker immediately after a write drops a dirty marker.
    pub worker_nudge: Arc<tokio::sync::Notify>,
    /// First empty-mirror observations retained across leader audits. A claim
    /// is reclaimable only when its exact nonce-bearing version is still empty
    /// on the same selection generation after the intent grace; process
    /// restarts and bucket switches restart the proof.
    pub empty_origin_observations: EmptyOriginObservations,
    /// Hand-rolled Prometheus counters served at /metrics.
    pub metrics: Arc<metrics::Metrics>,
    /// Distributed S3-backed event counters (per-package/version downloads per
    /// day). Self-contained engine; see counters.rs. Disabled => a no-op.
    pub counters: Arc<counters::Counters>,
    /// TTL cache of the global download leaderboard (ranked, top 500). Shared by
    /// the homepage marquee and `/downloads/` so a public homepage doesn't rescan
    /// the counter store on every hit; the numbers lag a flush interval anyway.
    pub download_board: DownloadBoard,
    /// On-demand upstream mirroring (None unless --proxy-upstream is set).
    pub proxy: Option<Arc<proxy::Proxy>>,
    /// Process start, for the homepage uptime readout.
    pub started: std::time::Instant,
    /// Set on graceful shutdown so `/health` reports 503 *before* the listener
    /// stops accepting, letting a load balancer drain the node cleanly.
    pub shutting_down: Arc<std::sync::atomic::AtomicBool>,
    /// Resolved advisory feed (a URL or a local path), post-empty-filter. `None`
    /// means the feature is disabled (`--advisory-feed ""`).
    pub advisory_feed: Option<String>,
    /// Whether malware blocking is armed (enforcement lands in a later rung).
    pub malware_block: bool,
    /// Per-node malware-probe interval. `Duration::ZERO` disables it; the worker
    /// also holds it inert unless blocking is armed and the feed is the OSV
    /// `all.zip` URL (the CSV/per-advisory siblings only exist there).
    pub malware_probe: Duration,
    /// The live advisory snapshot every request-path probe reads. A global
    /// truth-cache: NEVER reset on a bucket-generation change — a failover must
    /// not disarm blocking.
    pub advisories: Arc<std::sync::RwLock<Arc<advisories::AdvisoryState>>>,
    /// Set by `PUT /advisories/feed` so the worker runs the storage reload on its
    /// next tick regardless of the reconcile period, then clears it.
    pub advisory_reload_asap: Arc<std::sync::atomic::AtomicBool>,
}

impl AppState {
    /// Capture the storage context for one operation (design §3). Called exactly
    /// once at every entry point — request handler, worker tick, audit run, CLI
    /// one-shot — and the returned `(storage, generation)` is threaded down the
    /// whole call graph; helpers never re-resolve it. Cost is one `RwLock` read
    /// plus an `Arc` clone; single-bucket is byte-for-byte the old behavior.
    pub fn pin(&self) -> Arc<Pinned> {
        self.buckets.pin()
    }

    /// Capture the storage context for one operation's *reads*. Equals
    /// [`pin`](Self::pin) unless this node has an active region read pin, in which
    /// case reads are served from the near bucket while writes and every
    /// upstream-claim decision stay on [`pin`](Self::pin) (read affinity,
    /// dev/READ_AFFINITY_VISION.md).
    pub fn read_pin(&self) -> Arc<Pinned> {
        self.buckets.read_pin()
    }

    pub fn mutations_fenced(&self) -> bool {
        self.writes_fenced
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The current advisory snapshot: lock, clone the inner `Arc`, drop the
    /// guard. Cheap enough for the request path; recovers a poisoned lock.
    pub fn advisory_snapshot(&self) -> Arc<advisories::AdvisoryState> {
        advisories::AdvisoryState::read(&self.advisories)
    }

    /// Record that a client request just arrived (traffic signal for probe
    /// gating). Lock-free and I/O-free — safe to call on every request.
    fn note_request(&self) {
        self.last_request_unix.store(
            crate::clock::unix_now_secs(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Whether a client request arrived within [`TRAFFIC_PROBE_WINDOW`]. When
    /// true, the health loop keeps probes at full cadence so failover stays
    /// fast; when false (and every bucket is healthy) probes decay to idle.
    pub(crate) fn recent_request_traffic(&self) -> bool {
        let last = self
            .last_request_unix
            .load(std::sync::atomic::Ordering::Relaxed);
        last != 0
            && crate::clock::unix_now_secs().saturating_sub(last) <= TRAFFIC_PROBE_WINDOW.as_secs()
    }

    /// Whether any configured bucket is currently unhealthy or recovering
    /// (awaiting topology revalidation). Probing these is the only way heal-back
    /// happens, so their presence forces full probe cadence regardless of idle.
    pub(crate) fn any_bucket_unhealthy_or_recovering(&self) -> bool {
        let Some(health) = &self.bucket_health else {
            return false;
        };
        (0..self.buckets.len()).any(|index| !health.bucket_eligible(index).unwrap_or(true))
    }

    /// State for one-shot storage operations (rebuild-index) — no credentials, no
    /// server, default knobs. Only the storage-facing fields matter.
    pub fn headless(storage: Arc<dyn Storage>) -> Self {
        let buckets = Arc::new(BucketSet::single(storage));
        AppState {
            buckets,
            bucket_health: None,
            writes_fenced: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            uploader_user: None,
            uploader_pass: None,
            admin_user: None,
            admin_pass: None,
            read_user: None,
            read_pass: None,
            token_signing_key: None,
            private_prefix: None,
            artifact_delivery: ArtifactDelivery::Auto,
            metrics_project_labels: false,
            access_log: false,
            access_log_format: AccessLogFormat::Structured,
            worker_interval: Duration::from_secs(1),
            reconcile_interval: Duration::from_secs(86400),
            repl_sweep_interval: Duration::from_secs(300),
            repl_sweep_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_request_unix: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fanout_grace: Duration::from_secs(30),
            intent_grace: time::Duration::seconds(900),
            audit_on_boot: true,
            transparency: true,
            lease_ttl: Duration::from_secs(30),
            wait_on_upload: false,
            wait_on_upload_timeout: Duration::from_secs(10),
            index_cache: Arc::new(cache::IndexCache::new(cache::INDEX_CACHE_TTL)),
            project_cache: Arc::new(project_cache::ProjectCache::new(cache::INDEX_CACHE_TTL)),
            presign_cache: Arc::new(cache::PresignCache::new(cache::PRESIGN_CACHE_TTL)),
            spool_dir: std::env::temp_dir(),
            global_names: Arc::new(tokio::sync::Mutex::new(None)),
            inventory: Arc::new(tokio::sync::Mutex::new(worker::InventoryMap::default())),
            worker_nudge: Arc::new(tokio::sync::Notify::new()),
            empty_origin_observations: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            metrics: Arc::new(metrics::Metrics::new()),
            counters: Arc::new(counters::Counters::disabled()),
            download_board: Arc::new(std::sync::Mutex::new(None)),
            proxy: None,
            started: std::time::Instant::now(),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            advisory_feed: None,
            malware_block: false,
            malware_probe: Duration::ZERO,
            advisories: Arc::new(std::sync::RwLock::new(Arc::new(
                advisories::AdvisoryState::default(),
            ))),
            advisory_reload_asap: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The configured uploader credential, if any (both halves required).
    fn uploader_credential(&self) -> Option<(&str, &str)> {
        cred_pair(self.uploader_user.as_deref(), self.uploader_pass.as_deref())
    }

    /// The configured admin credential, if any. Its presence is what enables
    /// the privileged operations (mirror, delete, yank).
    fn admin_credential(&self) -> Option<(&str, &str)> {
        cred_pair(self.admin_user.as_deref(), self.admin_pass.as_deref())
    }

    /// The configured read credential, if any (both halves required).
    pub(crate) fn read_credential(&self) -> Option<(&str, &str)> {
        cred_pair(self.read_user.as_deref(), self.read_pass.as_deref())
    }

    /// No write credential configured: every write path is disabled and the
    /// server is read-only. Unauthenticated open writes were a footgun on the
    /// default 0.0.0.0 bind, not a dev convenience.
    fn uploads_disabled(&self) -> bool {
        self.uploader_credential().is_none() && self.admin_credential().is_none()
    }

    /// The role granted by a valid `__token__` bearer token, if token auth is
    /// configured and the presented token verifies and is unexpired. Returns
    /// None otherwise (no key, not a token request, bad/expired token) — fail
    /// closed. The `+tag` overlay is ignored here (`__token__+commit=…` still
    /// resolves to token mode); the token itself carries its attribution.
    fn token_role(&self, headers: &HeaderMap) -> Option<token::Role> {
        let key = nonempty(self.token_signing_key.as_deref())?;
        let (user, pass) = basic_credentials(headers)?;
        let base = user.split_once('+').map_or(user.as_str(), |(b, _)| b);
        if base != token::TOKEN_USERNAME {
            return None;
        }
        let now = OffsetDateTime::now_utc().unix_timestamp();
        token::verify(key, &pass, now).map(|c| c.role)
    }

    /// Does the request authenticate as admin?
    fn is_admin(&self, headers: &HeaderMap) -> bool {
        self.admin_credential()
            .is_some_and(|(u, p)| check_basic_auth(headers, u, p).is_ok())
            || self.token_role(headers) == Some(token::Role::Admin)
    }

    /// May the request publish? Admin ⊇ uploader.
    fn is_uploader(&self, headers: &HeaderMap) -> bool {
        self.is_admin(headers)
            || self
                .uploader_credential()
                .is_some_and(|(u, p)| check_basic_auth(headers, u, p).is_ok())
            || self.token_role(headers) >= Some(token::Role::Uploader)
    }

    /// May the request read indexes and artifacts? Public unless a read
    /// credential is configured; any stronger credential (or any valid token)
    /// also reads (admin ⊇ uploader ⊇ reader).
    pub(crate) fn is_reader(&self, headers: &HeaderMap) -> bool {
        match self.read_credential() {
            None => true,
            Some((u, p)) => {
                check_basic_auth(headers, u, p).is_ok()
                    || self.is_uploader(headers)
                    || self.token_role(headers).is_some()
            }
        }
    }

    /// Whether the request presents credentials that validly authenticate as some
    /// role *below* admin (reader or uploader). Distinguishes a wrong-role request
    /// (→ 403) from an anonymous or bad-credential one (→ 401) at an admin gate.
    /// Public-read (no read credential configured) is deliberately NOT counted:
    /// an anonymous request on such a server authenticates as nobody, so it still
    /// gets 401, never 403. Constant-time password compares throughout.
    fn authenticates_below_admin(&self, headers: &HeaderMap) -> bool {
        // A valid uploader — a real uploader credential, or an uploader/admin
        // token (admin is already excluded by the caller).
        if self.is_uploader(headers) {
            return true;
        }
        // A valid token of any configured role (covers a reader token).
        if self.token_role(headers).is_some() {
            return true;
        }
        // The configured read credential, actually presented and matching.
        match self.read_credential() {
            Some((u, p)) => check_basic_auth(headers, u, p).is_ok(),
            None => false,
        }
    }
}

/// A client request within this window keeps multi-bucket health probes at full
/// cadence. "The last few minutes" from dev/MULTIBUCKET.md §Health.
const TRAFFIC_PROBE_WINDOW: Duration = Duration::from_secs(120);

/// Idle probe cadence: one discovery probe per bucket about this often once
/// there has been no traffic and every bucket is healthy. The first request
/// after idle may pay one bounded discovery timeout (accepted, §Health).
pub(crate) const IDLE_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// Adapts pypiron's bucket selector to the counter engine. Selection happens
/// once at the counter operation boundary; every nested I/O then uses the same
/// captured storage handle.
struct CounterStore(Arc<BucketSet>);

impl counters::ObjectStoreSelector for CounterStore {
    fn pin(&self) -> Box<dyn counters::ObjectStore> {
        Box::new(PinnedCounterStore(self.0.pin().storage.clone()))
    }
}

struct PinnedCounterStore(Arc<dyn Storage>);

#[async_trait::async_trait]
impl counters::ObjectStore for PinnedCounterStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.0.supports_leases() {
            // Cloud: distinguishes a genuine miss (Ok(None)) from a transient
            // error (Err) — the engine must never freeze a day from a failed read.
            Ok(self.0.get_with_etag(key).await?.map(|(b, _)| b))
        } else {
            // Disk: get_bytes errors on a miss; a single-node disk store has no
            // compaction-safety stakes, so treat any error as absent.
            Ok(self.0.get_bytes(key).await.ok())
        }
    }
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.0.put_bytes(key, bytes, Some("application/json")).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .0
            .list_all(prefix)
            .await?
            .into_iter()
            .map(|o| o.key)
            .collect())
    }
    async fn delete(&self, keys: &[String]) -> Result<()> {
        self.0.delete_keys(keys).await
    }
}

/// Parse the CLI and dispatch the subcommand — the whole binary, callable
/// from the thin `src/main.rs`.
pub async fn cli_main() -> Result<()> {
    // Parse via ArgMatches (not just `Cli::parse()`) so `run_serve` can ask
    // clap whether each serve knob came from the CLI/env or is sitting at its
    // default — that's how the `[serve]` table layers under CLI/env without
    // losing the `[default: …]` hints clap prints in `--help`.
    let matches = Cli::command().get_matches();
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(e) => e.exit(),
    };

    // logging — format comes from the global --log-format/PYPIRON_LOG_FORMAT,
    // so every subcommand (serve, sync, verify-index, rebuild-index) logs consistently.
    let env_filter =
        std::env::var("RUST_LOG").unwrap_or_else(|_| "info,pypiron=info,object_store=warn".into());
    match cli.log_format {
        // ANSI only when stdout is a TTY, so color codes don't leak into files
        // or `docker logs`.
        LogFormat::Text => tracing_subscriber::fmt()
            .with_ansi(std::io::stdout().is_terminal())
            .with_env_filter(env_filter)
            .init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init(),
    }

    let config_path = cli.config.clone();
    match cli.command {
        Some(Commands::Sync(args)) => sync::run_sync(*args, config_path).await,
        Some(Commands::VerifyIndex(mut args)) => {
            apply_maintenance_config(
                &mut args.storage,
                config_path.as_deref(),
                &matches,
                "verify-index",
            )?;
            // grep/diff exit-code idiom: 0 converged, 1 diverged, 2 could-not-run.
            // Keep diverged off the error channel so CI can branch the three.
            // (clippy exempts `process::exit` only inside `fn main`; this
            // dispatcher moved to the library, so the exemption is explicit.)
            #[allow(clippy::exit)]
            match verify::run_verify(*args).await {
                Ok(true) => Ok(()),
                Ok(false) => std::process::exit(1),
                Err(e) => {
                    eprintln!("Error: {e:?}");
                    std::process::exit(2);
                }
            }
        }
        Some(Commands::RebuildIndex(mut args)) => {
            apply_maintenance_config(
                &mut args.storage,
                config_path.as_deref(),
                &matches,
                "rebuild-index",
            )?;
            run_rebuild_index(*args).await
        }
        Some(Commands::VerifyChain(mut args)) => {
            apply_maintenance_config(
                &mut args.storage,
                config_path.as_deref(),
                &matches,
                "verify-chain",
            )?;
            // Same grep/diff exit-code idiom as verify-index: 0 valid, 1 a
            // violation (rows already on stdout), 2 could-not-run.
            #[allow(clippy::exit)]
            match transparency::run_verify_chain(*args).await {
                Ok(true) => Ok(()),
                Ok(false) => std::process::exit(1),
                Err(e) => {
                    eprintln!("Error: {e:?}");
                    std::process::exit(2);
                }
            }
        }
        Some(Commands::Serve(args)) => {
            let serve_matches = matches
                .subcommand_matches("serve")
                .expect("serve subcommand matched");
            run_serve(*args, config_path, serve_matches, cli.log_format).await
        }
        Some(Commands::Healthcheck(args)) => run_healthcheck(args).await,
        Some(Commands::CreateToken(args)) => run_create_token(args).await,
        Some(Commands::Config(args)) => match args.command {
            // Pure stdout, no logging or config load — `config init > pypiron.toml`
            // must emit only the template.
            ConfigCommand::Init => {
                print!("{}", config::TEMPLATE);
                Ok(())
            }
        },
        Some(Commands::Origin(args)) => match args.command {
            OriginCommand::Release(mut release) => {
                apply_nested_maintenance_config(
                    &mut release.storage,
                    config_path.as_deref(),
                    &matches,
                    "origin",
                    "release",
                )?;
                run_origin_release(release).await
            }
        },
        Some(Commands::Buckets(args)) => match args.command {
            BucketsCommand::Migrate(mut migrate) => {
                apply_nested_maintenance_config(
                    &mut migrate.storage,
                    config_path.as_deref(),
                    &matches,
                    "buckets",
                    "migrate",
                )?;
                run_buckets_migrate(migrate).await
            }
        },
        None => {
            // A global flag (e.g. --log-format) but no subcommand: nothing to
            // run, so show help. Truly-bare `pypiron` never reaches here —
            // arg_required_else_help prints help before dispatch.
            Cli::command().print_help()?;
            println!();
            Ok(())
        }
    }
}

/// Build the download-counter engine from CLI config, failing closed on a bad
/// resolution. Disabled (`--download-stats=false`) yields a no-op store.
fn build_counters(cli: &ServeArgs, buckets: Arc<BucketSet>) -> Result<counters::Counters> {
    if !cli.download_stats {
        return Ok(counters::Counters::disabled());
    }
    let resolution_secs = parse_resolution_secs(&cli.counters_resolution)?;
    let cfg = counters::Config {
        resolution_secs,
        flush_interval: Duration::from_secs(cli.counters_flush_interval_secs),
        rollup_interval: Duration::from_secs(cli.counters_rollup_interval_secs),
        retention_days: cli.counters_retention_days,
        ..counters::Config::default()
    }
    .checked()
    .map_err(|e| anyhow::anyhow!("counter config: {e}"))?;
    Ok(counters::Counters::new(
        Box::new(CounterStore(buckets)),
        cfg,
    ))
}

/// Parse `1d` / `1h` / `30m` / `2h` into seconds. Minutes/hours/days only — the
/// counter buckets are minute-aligned, so smaller or calendar units are refused.
fn parse_resolution_secs(s: &str) -> Result<u32> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .filter(|&i| i > 0)
        .ok_or_else(|| anyhow::anyhow!("'{s}' is not a <number><unit> duration (e.g. 1d, 30m)"))?;
    let (num, unit) = s.split_at(split);
    let n: u32 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("'{s}': bad number"))?;
    let unit_secs = match unit.trim() {
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600,
        "d" | "day" | "days" => 86_400,
        other => anyhow::bail!("'{s}': unit '{other}' must be m, h, or d"),
    };
    n.checked_mul(unit_secs)
        .ok_or_else(|| anyhow::anyhow!("'{s}': duration too large"))
}

fn validate_intent_grace_secs(seconds: u64) -> Result<()> {
    if (3..=i64::MAX as u64).contains(&seconds) {
        Ok(())
    } else {
        anyhow::bail!(
            "--intent-grace-secs must be between 3 and {} seconds",
            i64::MAX
        )
    }
}

/// Synchronously obtain the startup snapshot for an *explicitly* configured
/// advisory setting. Fail-closed needs the answer before serving, so this blocks
/// and bails (AC7) when neither the source nor a stored `_advisories/` copy
/// yields a snapshot. The implicit default never calls this — it binds
/// immediately and lets the worker obtain in the background.
async fn advisory_obtain_explicit(
    storage: &dyn Storage,
    feed: Option<&str>,
) -> Result<(advisories::AdvisoryState, bool)> {
    if let Some(url) = feed {
        info!(feed = %url, "advisory feed: polling for the malware block set and org audit");
    }
    if let Some(state) = advisories::obtain_at_startup(storage, feed).await {
        return Ok((state, true));
    }
    anyhow::bail!(
        "advisory feed is configured but no snapshot could be obtained: the source was \
         unreachable and no stored _advisories/ snapshot exists. Point --advisory-feed at a \
         reachable URL or local zip, deliver one with `pypiron sync --advisory-feed`, or set \
         --advisory-feed \"\" to disable malware blocking."
    );
}

async fn run_serve(
    mut cli: ServeArgs,
    config_path: Option<std::path::PathBuf>,
    serve_matches: &clap::ArgMatches,
    log_format: LogFormat,
) -> Result<()> {
    // Layer pypiron.toml under CLI/env before anything reads the config: the
    // `[serve]` table fills in any knob the CLI/env left at its default, and the
    // top-level `private-prefix` + shared `[mirror]` reach the server here. The
    // mirror selection itself is resolved through sync's one shared path, so the proxy and
    // a sync run can never drift.
    let file = config::load(config_path.as_deref())?;
    // Capture whether the advisory knobs were set explicitly BEFORE the merge
    // folds file/default values in — startup posture is fail-closed only for an
    // explicit request (AC7), never for the always-on defaults.
    let explicit_advisory_feed =
        arg_from_cli_or_env(serve_matches, "advisory_feed") || file.serve.advisory_feed.is_some();
    let explicit_malware_block =
        arg_from_cli_or_env(serve_matches, "malware_block") || file.serve.malware_block.is_some();
    merge_serve_file(&mut cli, &file.serve, serve_matches)?;
    cli.private_prefix = cli.private_prefix.take().or(file.private_prefix.clone());
    validate_intent_grace_secs(cli.intent_grace_secs)?;

    // Supplying only `--admin-pass` is enough to enable admin: the password is
    // the secret, the username is conventional. Fill in the default username
    // only alongside a password, so the no-admin (read-only) configuration keeps
    // both halves unset rather than tripping the half-configured check below.
    cli.admin_user = resolve_admin_user(cli.admin_user.as_deref(), cli.admin_pass.as_deref());

    // Reject a half-configured credential before doing anything else: it can
    // never authenticate, and a half-set read credential would fail open and
    // serve every package publicly. Fail loudly at startup rather than silently.
    for (label, user, pass) in [
        (
            "admin (--admin-user/--admin-pass)",
            &cli.admin_user,
            &cli.admin_pass,
        ),
        (
            "uploader (--uploader-user/--uploader-pass)",
            &cli.uploader_user,
            &cli.uploader_pass,
        ),
        (
            "read (--read-user/--read-pass)",
            &cli.read_user,
            &cli.read_pass,
        ),
    ] {
        if let Some(msg) = credential_pair_error(label, user.as_deref(), pass.as_deref()) {
            anyhow::bail!(
                "{msg}. Configure both halves or neither: a half-configured credential cannot \
                 authenticate, and a half-configured read credential would serve every package \
                 without authentication."
            );
        }
    }

    // Fail closed on a per-bucket override that names a bucket outside the list
    // (typo protection), and warn on a --data-dir that --buckets makes moot —
    // both before any bucket is contacted.
    cli.storage.validate_override_keys()?;
    cli.storage.warn_if_data_dir_ignored();

    let storage_desc = cli.storage.describe();
    let raw_storages = cli.storage.build_all().await?;
    let names = cli.storage.bucket_names();
    debug_assert_eq!(raw_storages.len(), names.len());
    let bucket_health = if raw_storages.len() > 1 {
        Some(Arc::new(HealthController::new(
            raw_storages.len(),
            HealthPolicy::new(
                cli.bucket_leave_failures,
                Duration::from_secs(cli.bucket_return_healthy_secs),
            )?,
        )?))
    } else {
        None
    };
    let storages: Vec<Arc<dyn Storage>> = match &bucket_health {
        Some(health) => raw_storages
            .into_iter()
            .enumerate()
            .map(|(index, storage)| {
                Ok::<Arc<dyn Storage>, anyhow::Error>(Arc::new(
                    observed_storage::ObservedStorage::new(storage, index, health.clone())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
        None => raw_storages,
    };
    let handles: Vec<BucketHandle> = storages
        .into_iter()
        .zip(names)
        .map(|(storage, name)| BucketHandle { storage, name })
        .collect();
    let buckets = Arc::new(BucketSet::new(handles));
    // Fail closed on a mismatched bucket topology before serving a byte (no-op
    // unless more than one bucket is configured).
    let topology = buckets
        .verify_topology_with(|_, error| {
            bucket_health::classify(observed_storage::signal_for_error(error))
                == bucket_health::SignalClass::AvailabilityFailure
        })
        .await?;
    if let Some(health) = &bucket_health {
        for index in topology
            .verified_indices
            .iter()
            .chain(&topology.stamped_indices)
        {
            health.topology_revalidated(*index)?;
        }
        // Startup must work during the outage this feature exists for. Collapse
        // a confirmed-unreachable preferred bucket through the leave threshold
        // immediately; runtime observations retain normal hysteresis.
        for index in &topology.unreachable_indices {
            for _ in 0..cli.bucket_leave_failures {
                health.observe(*index, bucket_health::BucketSignal::ConnectionFailure)?;
            }
        }
        let initial = health.worker_tick();
        if let Some(change) = initial.selection_change {
            buckets.switch(change.to);
            health.selection_applied(change.to)?;
            warn!(
                from = change.from,
                to = change.to,
                "preferred bucket unavailable at startup; selected reachable bucket"
            );
        }
    }
    if buckets.is_multi() {
        info!(
            count = buckets.len(),
            "multi-bucket storage: replication and failover across configured buckets"
        );
    }
    // Learn this node's region once at startup (operator override, then platform
    // environment, then instance metadata) and, in a multi-bucket fleet, pin the
    // node's reads to its region bucket. Detection only labels the node; it never
    // moves a write (dev/READ_AFFINITY_VISION.md).
    let node_region = node_region::detect(cli.node_region.as_deref()).await;
    if let (Some(health), Some(node)) = (&bucket_health, &node_region) {
        let specs = cli.storage.bucket_specs()?;
        match specs
            .iter()
            .position(|spec| node_region::matches(node, spec))
        {
            Some(region) => {
                // A read pin is only worth seeding to a bucket that startup could
                // reach; otherwise reads follow the write pin until the worker
                // confirms the region bucket recovered and caught up.
                let write_index = buckets.pin().index;
                let reachable = !topology.unreachable_indices.contains(&region);
                let read_index = if reachable { region } else { write_index };
                health.configure_read_affinity(region, read_index)?;
                if reachable {
                    buckets.seed_read_pin(region);
                    info!(
                        region = %node.region,
                        bucket = %buckets.handles()[region].name,
                        "read affinity: serving reads from region bucket"
                    );
                } else {
                    warn!(
                        region = %node.region,
                        bucket = %buckets.handles()[region].name,
                        "read affinity: region bucket unreachable at startup; reads follow the write bucket until it recovers"
                    );
                }
            }
            None => info!(
                region = %node.region,
                "read affinity: no configured bucket is labeled for this node's region; reads follow the write bucket"
            ),
        }
    }
    let proxy = match cli.proxy_upstream.as_deref() {
        Some(upstream) => {
            let mirror = cli.mirror.resolve(Some(&file.mirror))?;
            Some(Arc::new(proxy::Proxy::new(
                upstream,
                mirror,
                cli.allow_insecure_upstream,
                &cli.proxy_allow_host,
                &cli.proxy_allow_cidr,
            )?))
        }
        None => None,
    };

    // The private prefix is the dependency-confusion control; a value that PEP
    // 503 normalization reduces to empty (e.g. `.`, `_`, `..`) would match no
    // package and silently protect nothing. Fail closed at startup instead.
    let private_prefix = match cli.private_prefix.as_deref() {
        Some(raw) => Some(checked_pkg_name(raw).ok_or_else(|| {
            anyhow::anyhow!("--private-prefix '{raw}' is not a valid package name")
        })?),
        None => None,
    };

    // Counters are derived and per-bucket. Each flush, compaction, or query pins
    // the selected handle once, so a switch applies only between operations.
    let counters = Arc::new(build_counters(&cli, buckets.clone())?);
    if counters.enabled() {
        info!(
            resolution = %cli.counters_resolution,
            flush_secs = cli.counters_flush_interval_secs,
            retention_days = cli.counters_retention_days,
            "download counters enabled (_counters/)"
        );
    }

    if cli.access_log {
        info!("access log enabled — logging every request decreases throughput ~7-10%");
    }

    // Advisory snapshot: resolve the feed (default OSV URL; `""` disables).
    let malware_block_flag = cli.malware_block.unwrap_or(true);
    let advisory_feed = cli
        .advisory_feed
        .take()
        .unwrap_or_else(|| advisories::DEFAULT_FEED_URL.to_string());
    let advisory_feed = {
        let trimmed = advisory_feed.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    };
    // Fully off only when the feed is empty AND blocking wasn't explicitly asked
    // for: an empty feed disables the whole feature, blocking included.
    let advisory_off = advisory_feed.is_none() && !(explicit_malware_block && malware_block_flag);
    let advisory_explicit = explicit_advisory_feed || explicit_malware_block;
    let (advisory_state, advisory_loaded) = if advisory_off {
        (advisories::AdvisoryState::default(), false)
    } else if advisory_explicit {
        // Explicit intent is fail-closed: block startup until we have the answer,
        // and refuse to serve if no snapshot can be obtained (AC7).
        let pinned = buckets.pin();
        advisory_obtain_explicit(pinned.storage.as_ref(), advisory_feed.as_deref()).await?
    } else {
        // The always-on default must never delay or brick a box that never asked:
        // bind immediately and let the worker's first (background) tick obtain the
        // snapshot — it self-arms on success and warns armed-but-unfed otherwise.
        if let Some(url) = &advisory_feed {
            info!(feed = %url, "advisory feed: polling for the malware block set and org audit (loading in background)");
        }
        (advisories::AdvisoryState::default(), false)
    };
    // Effective blocking is off whenever the feature is off.
    let malware_block = !advisory_off && malware_block_flag;

    let state = Arc::new(AppState {
        buckets,
        bucket_health,
        writes_fenced: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        uploader_user: cli.uploader_user,
        uploader_pass: cli.uploader_pass,
        admin_user: cli.admin_user,
        admin_pass: cli.admin_pass,
        read_user: cli.read_user,
        read_pass: cli.read_pass,
        token_signing_key: cli.token_signing_key,
        private_prefix,
        artifact_delivery: cli.artifact_delivery,
        metrics_project_labels: cli.metrics_project_labels,
        access_log: cli.access_log,
        access_log_format: cli.access_log_format,
        worker_interval: Duration::from_secs(cli.worker_interval_secs),
        reconcile_interval: Duration::from_secs(cli.reconcile_interval_secs),
        repl_sweep_interval: Duration::from_secs(cli.repl_sweep_interval_secs),
        repl_sweep_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        last_request_unix: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        fanout_grace: Duration::from_secs(cli.fanout_grace_secs),
        intent_grace: time::Duration::seconds(cli.intent_grace_secs as i64),
        audit_on_boot: cli.audit_on_boot,
        transparency: cli.transparency,
        lease_ttl: Duration::from_secs(cli.lease_ttl_secs),
        wait_on_upload: cli.wait_on_upload,
        wait_on_upload_timeout: Duration::from_secs(cli.wait_on_upload_secs),
        index_cache: Arc::new(cache::IndexCache::new(cache::INDEX_CACHE_TTL)),
        project_cache: Arc::new(project_cache::ProjectCache::new(cache::INDEX_CACHE_TTL)),
        presign_cache: Arc::new(cache::PresignCache::new(cache::PRESIGN_CACHE_TTL)),
        spool_dir: cli.spool_dir.unwrap_or_else(std::env::temp_dir),
        global_names: Arc::new(tokio::sync::Mutex::new(None)),
        inventory: Arc::new(tokio::sync::Mutex::new(worker::InventoryMap::default())),
        worker_nudge: Arc::new(tokio::sync::Notify::new()),
        empty_origin_observations: Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        metrics: Arc::new(metrics::Metrics::new()),
        counters,
        download_board: Arc::new(std::sync::Mutex::new(None)),
        proxy,
        started: std::time::Instant::now(),
        shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        advisory_feed,
        malware_block,
        malware_probe: Duration::from_secs(cli.malware_probe_secs),
        advisories: Arc::new(std::sync::RwLock::new(Arc::new(advisory_state))),
        advisory_reload_asap: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    // A snapshot loaded at startup is a successful refresh — arm the gauge.
    if advisory_loaded {
        state.metrics.advisory_refresh_ok();
    }

    // Genuine misconfiguration hazards warn in any log format. The benign
    // facts (read-only, no-admin, public reads, proxy upstream) are surfaced in
    // the startup banner (text) or the structured `listening` event (JSON).
    if !state.uploads_disabled()
        && state.admin_credential().is_some()
        && state.admin_credential() == state.uploader_credential()
    {
        warn!("uploader and admin credentials are identical: every uploader has admin powers");
    }
    if state.proxy.is_some() && state.private_prefix.is_none() {
        warn!("proxy enabled without --private-prefix: new private uploads race public names for first claim; a reserved prefix closes that hole");
    }

    // Arm the replication metric family only when multi-bucket is live (§G).
    if state.buckets.is_multi() {
        state.metrics.set_multi_bucket();
    }

    // Initialize empty index files if they don't exist
    initialize_indexes(&state).await?;

    // The streaming routes — uploads and artifact downloads — move large
    // bodies, so they get the longer deadline. Kept in their own router and
    // merged in *after* the short timeout is applied below, so they never
    // inherit it (axum's `.layer()` only wraps routes added before the call).
    let streaming = Router::new()
        // Legacy PyPI upload API (used by uv/twine).
        .route("/legacy", post(legacy_upload))
        .route("/legacy/", post(legacy_upload))
        // Artifact bytes (streamed through this node in `stream` mode).
        .route(
            "/files/:package/:filename",
            get(files_get).delete(files_delete),
        )
        .layer(tower_http::timeout::TimeoutLayer::new(
            STREAMING_REQUEST_TIMEOUT,
        ));

    // router
    let app = Router::new()
        // Human-facing pages. Root is the public front door (no secrets); its
        // inline activity panel and the package browser are gated by read auth
        // inside their handlers.
        .route("/", get(root))
        .route("/favicon.ico", get(favicon))
        .route("/projects", get(projects_page))
        .route("/projects/", get(projects_page))
        .route("/project/:package", get(project_page))
        .route("/project/:package/", get(project_page))
        .route("/project/:package/:version", get(project_version_page))
        .route("/project/:package/:version/", get(project_version_page))
        .route("/simple", get(simple_root))
        .route("/simple/", get(simple_root))
        .route("/simple/index.json", get(simple_root_json))
        .route("/simple/:package", get(simple_pkg))
        .route("/simple/:package/", get(simple_pkg))
        .route("/simple/:package/index.json", get(simple_pkg_json))
        .route(
            "/files/:package/:filename/yank",
            post(yank_set).delete(yank_clear),
        )
        // PEP 792 project status (admin): the project-level twin of file yank.
        // Mirror-over-HTTP `sync` relays upstream status through it.
        .route(
            "/project/:package/status",
            post(project_status_set).delete(project_status_clear),
        )
        // Mirror-over-HTTP sync cursors: the server-side memo of the last
        // upstream ETag each sync job saw, so a fresh/ephemeral sync host stays
        // conditional. Admin-gated; opaque JSON the server never interprets.
        .route("/sync/cursors", get(sync_cursors_get).put(sync_cursors_put))
        // The advisory snapshot push/pull path: a reader pulls the stored zip
        // (etag-conditioned), an admin pushes a delivered snapshot.
        .route(
            "/advisories/feed",
            get(advisories_feed_get).put(advisories_feed_put),
        )
        // The org audit report: the ranked list of hosted (package, version) rows
        // a known advisory affects. Admin-gated (attacker recon) — HTML + JSON.
        .route("/audit", get(audit_page))
        .route("/audit/", get(audit_page))
        .route("/audit.json", get(audit_json))
        // The locally-materialized PEP 691 index, bypassing the on-demand
        // proxy: a mirror-over-HTTP `sync` reconciles against the dest's own
        // truth (which files it holds, their yank state), not a proxied
        // upstream view that would hide a removed file from the reconcile.
        .route("/sync/local-index/:package", get(sync_local_index))
        // Per-package and global download counters (read-auth gated in-handler).
        .route("/stats/:metric", get(stats_summary_get))
        .route("/stats/:metric/:package", get(stats_get))
        // The human download leaderboard (read-auth gated in-handler).
        .route("/downloads", get(downloads_page))
        .route("/downloads/", get(downloads_page))
        // Mint a short-lived install token (gated in-handler: requires a signing
        // key and a credential that already grants the requested role).
        .route("/tokens", post(mint_token))
        // Operational endpoints: deliberately outside read auth — load
        // balancers and Prometheus scrapers don't carry package credentials.
        .route("/health", get(health))
        .route("/metrics", get(serve_metrics))
        // Catch-all for debugging unmatched routes
        .fallback(fallback_handler)
        // Short whole-request deadline on every route above (slowloris guard);
        // the streaming routes are merged in next, so they keep their own.
        .layer(tower_http::timeout::TimeoutLayer::new(REQUEST_TIMEOUT))
        .merge(streaming)
        .with_state(state.clone())
        // Axum's default 2 MB body limit would reject any real wheel.
        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), log_requests))
        .layer(middleware::from_fn(add_www_authenticate))
        .layer(middleware::from_fn_with_state(state.clone(), track_metrics));

    // spawn worker (with a shutdown handle so it can release the leader
    // lease on graceful exit)
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let worker_handle = tokio::spawn(worker::run_worker_until(state.clone(), shutdown_rx));

    // serve
    let listener = tokio::net::TcpListener::bind(&cli.bind_addr).await?;
    match log_format {
        LogFormat::Text => print_banner(&state, &cli.bind_addr, &storage_desc),
        // JSON consumers keep a single machine-readable readiness event.
        LogFormat::Json => info!(
            version = env!("CARGO_PKG_VERSION"),
            commit = GIT_HASH,
            storage = %storage_desc,
            read_only = state.uploads_disabled(),
            authed_reads = state.read_credential().is_some(),
            "listening on http://{}", cli.bind_addr
        ),
    }
    // We observe the shutdown signal ourselves (rather than handing it straight
    // to `with_graceful_shutdown`) so we can bound the wait. Axum's graceful
    // shutdown blocks until every *in-flight* request finishes; one slow or
    // stuck request (a hung `uv` resolve, a half-sent request, an interrupted
    // download) would otherwise pin Ctrl-C for as long as the client holds on.
    let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        // Hygiene default: set TCP_NODELAY on every accepted connection (axum
        // leaves it off, i.e. Nagle on). Standard for a latency-sensitive HTTP
        // server, and a measured no-regression. It does NOT address the
        // instance-dependent small-artifact keepalive stall (a receive-window ×
        // delayed-ACK interaction, unaffected by TCP_NODELAY) — see the
        // "FLEET ROOT-CAUSE" entry in dev/BENCHMARK_RESULTS.md for that story.
        .tcp_nodelay(true)
        .with_graceful_shutdown(async move {
            let _ = graceful_rx.await;
        })
        .await
    });

    shutdown_signal().await;
    info!("shutting down — press Ctrl-C again to force-quit");

    // Escape hatch. Installing our own SIGINT/SIGTERM handler replaces the OS
    // default (terminate the process), so while the bounded drain below runs,
    // every further Ctrl-C is a silent no-op — the "Ctrl-C does nothing" trap.
    // A second signal hard-exits now, whatever the drain is stuck on. 130 is
    // the conventional 128 + SIGINT exit code for a Ctrl-C'd process.
    tokio::spawn(async {
        shutdown_signal().await;
        warn!("second signal received — forcing immediate exit");
        // Hard-exit deliberately: returning to unwind the runtime could itself
        // block on whatever the drain is stuck on, defeating the escape hatch.
        #[allow(clippy::exit)]
        std::process::exit(130);
    });

    // Fail /health first, so a load balancer pulls this node out of rotation
    // before we stop accepting — otherwise new requests race straight into
    // connection-refused during the drain. Only cloud (multi-node, LB-fronted)
    // deployments need the pause; disk is single-node, so Ctrl-C stays instant.
    state
        .shutting_down
        .store(true, std::sync::atomic::Ordering::Relaxed);
    if state.pin().storage.supports_leases() {
        tokio::time::sleep(PRE_DRAIN_PAUSE).await;
    }

    let _ = graceful_tx.send(()); // begin draining in-flight requests
    let _ = shutdown_tx.send(true); // stop the worker

    // Give in-flight requests up to 10s to finish, then exit regardless.
    match tokio::time::timeout(Duration::from_secs(10), server).await {
        Err(_) => warn!("graceful shutdown timed out after 10s; forcing exit"),
        Ok(Err(e)) => warn!(error = %e, "server task failed to join"),
        Ok(Ok(Err(e))) => return Err(anyhow!("server error: {e}")),
        Ok(Ok(Ok(()))) => {}
    }

    // Give the worker a moment to release the leader lease — that hand-off is
    // what keeps a restart from being a lease-TTL write outage on the successor.
    if tokio::time::timeout(Duration::from_secs(5), worker_handle)
        .await
        .is_err()
    {
        warn!("worker did not stop within 5s; exiting without lease release");
    }
    Ok(())
}

/// A friendly, human-readable startup summary for the default text log format.
/// Not a log line on purpose — it's the first thing a developer sees, so it
/// reads as a greeting, not a trace event. (JSON mode keeps a structured
/// `listening` event instead.)
fn print_banner(state: &AppState, bind_addr: &str, storage: &str) {
    let uploads = if state.uploads_disabled() {
        "disabled — read-only (set --admin-user / --uploader-user to enable)".to_string()
    } else {
        let mut roles = Vec::new();
        if state.admin_credential().is_some() {
            roles.push("admin");
        }
        if state.uploader_credential().is_some() {
            roles.push("uploader");
        }
        let mut s = roles.join(", ");
        if state.admin_credential().is_none() {
            s.push_str("  (no admin: mirror, delete, yank disabled)");
        }
        s
    };
    let reads = if state.read_credential().is_some() {
        "require auth"
    } else {
        "public, no auth"
    };
    let proxy = state
        .proxy
        .as_ref()
        .map(|p| format!("\n     proxy     {}", p.upstream()))
        .unwrap_or_default();

    println!(
        "\n  🐍 pypiron {version} — ready\n\n     \
         url       http://{bind_addr}\n     \
         storage   {storage}\n     \
         uploads   {uploads}\n     \
         reads     {reads}{proxy}\n\n     \
         ctrl-c to stop\n",
        version = VERSION,
    );
}

async fn shutdown_signal() {
    // SIGTERM is what process managers (and our own bench scripts) send;
    // Ctrl-C covers interactive use.
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

/// Request logging. The default (no `--access-log`) is an *audit* log: it records
/// mutations — uploads, deletes, yanks, status changes (any non-GET/HEAD) — at
/// `info`, and stays silent on the high-volume reads (index listings, downloads)
/// so the log doesn't become the workload. `--access-log` widens it to every
/// request (the full access log). Either way, `/health` and `/metrics` are logged
/// only at `debug`: load balancers and Prometheus poll them constantly, so an
/// info-level access log would drown in them.
///
/// Records are `tracing` events on the `pypiron::access` target, so `RUST_LOG`
/// tunes them (`pypiron::access=warn` keeps only failures, `=off` silences them)
/// and they render as key=value or, under `--log-format json`, a JSON object.
/// With `--access-log --access-log-format clf` the line is Combined Log Format
/// written straight to stdout, bypassing the diagnostic log's timestamp+level
/// prefix that CLF parsers can't read.
async fn log_requests(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response<Body> {
    let method = req.method().clone();
    let is_read = method == Method::GET || method == Method::HEAD;
    let health_or_metrics = matches!(req.uri().path(), "/health" | "/metrics");

    if !is_read && state.mutations_fenced() {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from(
                r#"{"error":"bucket topology mismatch; writes are fenced"}"#,
            ))
            .unwrap_or_else(not_found);
    }

    // Decide up front whether this request could be logged, so the hot read path
    // does no timing or field work.
    let consider = if health_or_metrics {
        // Only ever at debug — checked here so frequent probes cost nothing
        // at the default info level.
        tracing::enabled!(target: "pypiron::access", tracing::Level::DEBUG)
    } else if state.access_log {
        true // firehose: every request
    } else {
        !is_read // default audit: mutations only
    };
    if !consider {
        return next.run(req).await;
    }

    let clf = state.access_log && matches!(state.access_log_format, AccessLogFormat::Clf);

    // Captured before `next.run` consumes the request.
    let target = req.uri().to_string();
    let project = project_tag(req.headers());
    let ua = header_str(req.headers(), header::USER_AGENT);
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());
    let host = client_ip(req.headers(), peer);
    // CLF-only fields: skip the work for the structured path.
    let version = req.version();
    let authuser = clf
        .then(|| basic_credentials(req.headers()).map(|(u, _)| u))
        .flatten();
    let referer = if clf {
        header_str(req.headers(), header::REFERER)
    } else {
        None
    };

    let start = std::time::Instant::now();
    let response = next.run(req).await;
    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = response.status();
    let bytes = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    if clf {
        let time = OffsetDateTime::now_utc()
            .format(CLF_TIME)
            .unwrap_or_default();
        let line = format_clf(
            &host,
            authuser.as_deref(),
            &time,
            &method,
            &target,
            &format!("{version:?}"),
            status.as_u16(),
            bytes,
            referer.as_deref(),
            ua.as_deref(),
        );
        // CLF bypasses tracing; a locked, whole-line write keeps it from
        // interleaving with the diagnostic log on the shared stdout.
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{line}");
        return response;
    }

    let bytes = bytes.map(|b| b.to_string());
    let bytes = bytes.as_deref().unwrap_or("-");
    macro_rules! access_event {
        ($level:ident) => {
            $level!(
                target: "pypiron::access",
                %method,
                path = %target,
                status = status.as_u16(),
                latency_ms,
                bytes = %bytes,
                project = project.as_deref(),
                client = %host,
                ua = ua.as_deref(),
                "request"
            )
        };
    }
    // /health & /metrics only reach here when debug is enabled, so keep them at
    // debug; otherwise 5xx at warn so failures surface under a warn filter.
    if health_or_metrics {
        access_event!(debug);
    } else if status.is_server_error() {
        access_event!(warn);
    } else {
        access_event!(info);
    }
    response
}

/// A header's value as an owned `String`, if present and valid UTF-8.
fn header_str(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// The client's address for logging: `X-Forwarded-For` (leftmost) or
/// `X-Real-IP` when set by a trusted proxy, else the direct peer, else `-`.
fn client_ip(headers: &HeaderMap, peer: Option<std::net::IpAddr>) -> String {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        let first = xff.split(',').next().unwrap_or("").trim();
        if !first.is_empty() {
            return first.to_string();
        }
    }
    if let Some(real) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        let real = real.trim();
        if !real.is_empty() {
            return real.to_string();
        }
    }
    peer.map(|ip| ip.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Render one Combined Log Format line. Pure (no clock) so it unit-tests; the
/// caller supplies the formatted timestamp. Missing fields render as `-`.
#[allow(clippy::too_many_arguments)]
fn format_clf(
    host: &str,
    authuser: Option<&str>,
    time: &str,
    method: &Method,
    target: &str,
    proto: &str,
    status: u16,
    bytes: Option<u64>,
    referer: Option<&str>,
    ua: Option<&str>,
) -> String {
    let dash = |s: Option<&str>| match s {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => "-".to_string(),
    };
    let host = if host.is_empty() { "-" } else { host };
    let bytes = bytes
        .map(|b| b.to_string())
        .unwrap_or_else(|| "-".to_string());
    // `authuser` is the one field decoded from base64 (`basic_credentials`), so —
    // unlike the header/URI-sourced fields, which hyper has already stripped of
    // control bytes — it can carry raw CR/LF/ESC. Drop control chars (Unicode Cc:
    // C0, DEL, C1) so a crafted username can't forge log lines, poison fail2ban,
    // or inject ANSI into an operator's terminal.
    let authuser = authuser.map(|u| u.chars().filter(|c| !c.is_control()).collect::<String>());
    format!(
        "{host} - {authuser} [{time}] \"{method} {target} {proto}\" {status} {bytes} \"{referer}\" \"{ua}\"",
        authuser = dash(authuser.as_deref()),
        referer = dash(referer),
        ua = dash(ua),
    )
}

/// RFC 7235: a 401 without `WWW-Authenticate` is malformed, and pip's keyring
/// integration and browsers rely on the header to prompt for credentials.
/// One layer covers every 401 return site, present and future.
async fn add_www_authenticate(req: Request, next: Next) -> Response<Body> {
    let mut resp = next.run(req).await;
    if resp.status() == StatusCode::UNAUTHORIZED
        && !resp.headers().contains_key(header::WWW_AUTHENTICATE)
    {
        resp.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(r#"Basic realm="PypIron""#),
        );
    }
    resp
}

/// Count every request by route group and status class (see metrics.rs).
async fn track_metrics(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response<Body> {
    let group = metrics::route_group(req.uri().path());
    // Traffic signal for multi-bucket probe gating: real client requests only.
    // /health and /metrics are infra polls (a load balancer hits /health every
    // second); counting them would pin probes at full cadence forever and
    // defeat the idle decay. Groups 3 and 4 are /health and /metrics.
    if group != 3 && group != 4 {
        state.note_request();
    }
    // Only read the project tag when labels are enabled; otherwise the
    // unauthenticated /metrics endpoint would expose internal project names.
    let project = state
        .metrics_project_labels
        .then(|| project_tag(req.headers()))
        .flatten();
    let resp = next.run(req).await;
    let status = resp.status().as_u16();
    state.metrics.record_request(group, status);
    // Attribute traffic to the client's project tag — except on auth
    // failures, where the tag never validated against anything.
    if let Some(tag) = project {
        if status != 401 && status != 403 {
            state.metrics.record_project(&tag, group);
        }
    }
    resp
}

/// Liveness + storage reachability. A storage error is the only failure mode:
/// `Ok(false)` (probe object missing) still proves storage answers. During a
/// graceful shutdown this reports 503 first, so a load balancer drains the node
/// before the listener stops accepting (see the shutdown path in `run_serve`).
async fn health(State(state): State<Arc<AppState>>) -> Response<Body> {
    if state
        .shutting_down
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from(r#"{"status":"draining"}"#))
            .unwrap_or_else(not_found);
    }
    let probe = format!("{SIMPLE_PREFIX}index.json");
    let (status, body) = match state.pin().storage.head_exists(&probe).await {
        Ok(_) => (StatusCode::OK, r#"{"status":"ok"}"#),
        Err(e) => {
            warn!(error=?e, "health: storage probe failed");
            (StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"degraded"}"#)
        }
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(body))
        .unwrap_or_else(not_found)
}

/// Prometheus text exposition of the process counters.
async fn serve_metrics(State(state): State<Arc<AppState>>) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(state.metrics.render()))
        .unwrap_or_else(not_found)
}

/// The site icon, carved from the logo. Static and immutable per build, so it's
/// served straight from the embedded bytes with a day-long cache and no auth —
/// browsers fetch it unprompted, before any credential is in play.
async fn favicon() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "image/x-icon")
        .header(header::CACHE_CONTROL, "public, max-age=86400")
        .body(Body::from(web::FAVICON_ICO))
        .unwrap_or_else(not_found)
}

/// Fallback handler for unmatched routes
async fn fallback_handler(req: Request) -> impl IntoResponse {
    let method = req.method();
    let uri = req.uri();
    warn!("No route matched: {} {}", method, uri);
    (
        StatusCode::NOT_FOUND,
        format!("No route found for {} {}", method, uri),
    )
}

/// Initialize empty index files if they don't exist
async fn initialize_indexes(state: &AppState) -> Result<()> {
    let storage = state.pin().storage.clone();
    let html_key = format!("{SIMPLE_PREFIX}index.html");
    let json_key = format!("{SIMPLE_PREFIX}index.json");

    // Check if global indexes exist
    let html_exists = storage.head_exists(&html_key).await.unwrap_or(false);
    let json_exists = storage.head_exists(&json_key).await.unwrap_or(false);

    if !html_exists || !json_exists {
        info!("Initializing empty global indexes");
        let empty_packages: Vec<String> = Vec::new();
        let html = render::pep503_global_html(&empty_packages);
        let json = render::pep691_global_json(&empty_packages);

        if !html_exists {
            storage
                .put_bytes(
                    &html_key,
                    html.into_bytes(),
                    Some("text/html; charset=utf-8"),
                )
                .await?;
        }

        if !json_exists {
            storage
                .put_bytes(
                    &json_key,
                    json.into_bytes(),
                    Some("application/vnd.pypi.simple.v1+json"),
                )
                .await?;
        }
    }

    Ok(())
}

/// --- Upload endpoint ------------------------------------------------------
/// Legacy PyPI upload endpoint compatible with uv/twine.
/// Multipart form with metadata text fields (name, version, sha256_digest,
/// requires_python, ...) and the file in field "content" (or "file").
/// Upper bound for the PEP 740 `provenance`/`attestations` form fields. These
/// JSON objects are KBs in practice; the cap only guards against a pathological
/// part buffering unbounded bytes in RAM.
const PROVENANCE_MAX_FIELD_BYTES: usize = 4 * 1024 * 1024;

/// Bound the non-file metadata parts as a whole. The per-field cap above doesn't
/// stop a flood of uniquely-named 64 KiB fields — ~16k of them fit under the
/// 1 GiB body limit and sit resident in the `fields` map at once, OOMing a small
/// box. Real uploads send a few dozen small fields (plus the two large JSON
/// ones), so these limits are generous headroom, not a functional constraint.
const MAX_METADATA_FIELDS: usize = 256;
const MAX_METADATA_TOTAL_BYTES: usize = 32 * 1024 * 1024;

/// The already-spooled or in-memory body an upload writes. `Spool` carries the
/// temp file (self-deleting on drop) the handler streamed the multipart into;
/// `Bytes` lets a deterministic simulator hand [`publish_record`] a body without
/// a filesystem. Either maps to a [`storage::ArtifactBody`] for the verified
/// store.
pub enum PublishBody {
    Spool(upload::TempPath),
    Bytes(Vec<u8>),
}

/// Everything [`publish_record`] needs once the handler has finished the HTTP
/// concerns (auth, multipart spool, filename/name/digest validation, mirror
/// gating, wheel-metadata extraction). Every field is already validated and
/// normalized; the core never re-reads the request or re-derives them.
pub struct PublishRequest {
    /// PEP 503-normalized package name, already validated as a storage segment.
    pub pkg: String,
    /// Artifact filename, already validated against path/sidecar collisions.
    pub filename: String,
    /// Spooled temp file (handler) or in-memory bytes (simulator).
    pub body: PublishBody,
    /// SHA-256 of `body`, already verified against any client-supplied digest.
    pub sha256: String,
    /// Byte length of `body`.
    pub size: u64,
    /// Version string as the handler derived it (form field or filename).
    pub version: String,
    /// `Requires-Python` for the sidecar, if the client sent one.
    pub requires_python: Option<String>,
    /// True for a mirror (`sync --to`, admin) upload; false for a private one.
    pub is_mirror: bool,
    /// Upload timestamp: mirror-provided (backdated) or `now_rfc3339`.
    pub upload_time: String,
    /// Yank state for the sidecar (mirror uploads can arrive pre-yanked).
    pub yanked: Yanked,
    /// PEP 658 wheel METADATA, pre-extracted off the async runtime.
    pub wheel_metadata: Option<Vec<u8>>,
    /// True when `filename` is a wheel (drives PEP 658 metadata handling).
    pub is_wheel: bool,
    /// PEP 740 provenance JSON relayed by a mirror upload, if present.
    pub provenance: Option<String>,
}

async fn legacy_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Mirror-ness lives in a form field, so whether *admin* is required can't
    // be decided until the body is parsed. But every upload needs at least
    // uploader rights, so reject that up front — preserving "never read the
    // body of an unauthorized request".
    let is_admin = state.is_admin(&headers);
    if !is_admin && !state.is_uploader(&headers) {
        return Err(if state.uploads_disabled() {
            (
                StatusCode::FORBIDDEN,
                "Uploads are disabled (no upload credential configured)".into(),
            )
        } else {
            (StatusCode::UNAUTHORIZED, "Unauthorized".into())
        });
    }

    let mut filename_opt: Option<String> = None;
    let mut spooled: Option<upload::FinishedSpool> = None;
    let mut fields: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    // Cumulative bytes across non-file parts — bounds the metadata map's RAM.
    let mut metadata_total_bytes: usize = 0;

    while let Some(mut field) = multipart.next_field().await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Invalid multipart form data".into(),
        )
    })? {
        let field_name = field.name().unwrap_or("").to_string();
        let part_filename = field.file_name().map(|s| s.to_string());

        match field_name.as_str() {
            "content" | "file" => {
                // Stream to a temp file, hashing as we go — memory stays
                // chunk-sized no matter how big the wheel is (see upload.rs).
                let mut spool = upload::UploadSpool::new(&state.spool_dir)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Could not open upload spool: {e}"),
                        )
                    })?;
                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => spool.write_chunk(&chunk).await.map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("Could not spool uploaded file: {e}"),
                            )
                        })?,
                        Ok(None) => break,
                        Err(_) => {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Could not read uploaded file".into(),
                            ))
                        }
                    }
                }
                spooled = Some(spool.finish().await.map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Could not finish upload spool: {e}"),
                    )
                })?);
                if filename_opt.is_none() {
                    filename_opt = part_filename;
                }
            }
            _ => {
                // Metadata fields are tiny (version, sha256_digest, ...). The
                // artifact is streamed to a disk spool; a non-content part must
                // not be the hole that buffers ~1 GiB in RAM and OOMs the box.
                // The PEP 740 provenance/attestations objects are larger JSON —
                // bounded higher, but still bounded.
                let max_field_bytes = match field_name.as_str() {
                    "provenance" | "attestations" => PROVENANCE_MAX_FIELD_BYTES,
                    _ => 64 * 1024,
                };
                let mut buf = Vec::new();
                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => {
                            if buf.len() + chunk.len() > max_field_bytes {
                                return Err((
                                    StatusCode::BAD_REQUEST,
                                    format!("Form field '{field_name}' is too large"),
                                ));
                            }
                            buf.extend_from_slice(&chunk);
                        }
                        Ok(None) => break,
                        Err(_) => {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Invalid multipart form data".into(),
                            ))
                        }
                    }
                }
                if let Ok(text) = String::from_utf8(buf) {
                    if !text.is_empty() {
                        metadata_total_bytes += text.len();
                        if metadata_total_bytes > MAX_METADATA_TOTAL_BYTES {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Metadata fields too large".into(),
                            ));
                        }
                        if !fields.contains_key(&field_name) && fields.len() >= MAX_METADATA_FIELDS
                        {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Too many metadata fields".into(),
                            ));
                        }
                        fields.insert(field_name, text);
                    }
                }
            }
        }
    }

    let filename = filename_opt
        .or_else(|| fields.get("filename").cloned())
        .ok_or((StatusCode::BAD_REQUEST, "Missing filename".to_string()))?;
    let spooled = spooled.ok_or((StatusCode::BAD_REQUEST, "Missing file content".to_string()))?;

    // No path separators, dotfiles, or names colliding with sidecar suffixes.
    if !valid_artifact_filename(&filename) {
        return Err((StatusCode::BAD_REQUEST, "Invalid filename".into()));
    }

    let pkg_norm = match fields.get("name") {
        Some(name) => normalize_pkg_name(name),
        None => infer_package_from_filename(&filename),
    };
    // Normalized names are storage path segments; anything else is hostile.
    if !is_normalized(&pkg_norm) {
        return Err((StatusCode::BAD_REQUEST, "Invalid package name".into()));
    }

    // The hash was computed incrementally during spooling. Zip extraction
    // reads the central directory + one entry from the spool file — it is
    // I/O + CPU bound, so off the async runtime.
    let is_wheel = filename.ends_with(".whl");
    let sha256 = spooled.sha256.clone();
    let wheel_metadata = if is_wheel {
        let path = spooled.path.path().to_path_buf();
        tokio::task::spawn_blocking(move || wheel::extract_metadata_from_file(&path))
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Metadata extraction task failed".to_string(),
                )
            })?
    } else {
        None
    };

    // Verify the client-supplied digest, and capture the hash for the sidecar.
    if let Some(claimed) = fields.get("sha256_digest") {
        if !claimed.eq_ignore_ascii_case(&sha256) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("sha256_digest mismatch: form says {claimed}, file is {sha256}"),
            ));
        }
    }

    // A claimed version must correspond to the filename (PEP 427/625 — every
    // standard build tool derives the name *from* the metadata, so a mismatch is
    // hand-crafted). Enforcing it here makes the filename authoritative by
    // construction, which the project page's cheap version check and the
    // advisory byte gate already rule on. Mirror uploads pass trivially: sync
    // has no version source but the filename (the Simple API carries none), so
    // it always sends the inferred value — and must keep doing so if it ever
    // learns true versions from the JSON API. Legacy binary formats infer no
    // version, so those still take the field's word for it.
    if let (Some(claimed), Some(from_name)) = (
        fields.get("version"),
        infer_version_from_filename(&filename),
    ) {
        if names::fold_version(claimed) != names::fold_version(&from_name) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("version '{claimed}' does not match filename '{filename}'"),
            ));
        }
    }
    let version = fields
        .get("version")
        .cloned()
        .or_else(|| infer_version_from_filename(&filename))
        .unwrap_or_default();

    // Mirror mode: `sync --to` sends mirror=true plus PyPI's historical
    // metadata. Backdating is an admin privilege — never reachable with plain
    // uploader rights, and never reinterpreted as a normal upload.
    let is_mirror = fields.get("mirror").map(String::as_str) == Some("true");
    if is_mirror {
        if !is_admin {
            // Distinguish "admin disabled here" from "you're not admin".
            return Err(if state.admin_credential().is_none() {
                (
                    StatusCode::FORBIDDEN,
                    "Mirror uploads are disabled (no admin credential configured)".into(),
                )
            } else {
                (
                    StatusCode::UNAUTHORIZED,
                    "Mirror uploads require the admin credential".into(),
                )
            });
        }
    } else if fields.contains_key("upload_time")
        || fields.contains_key("yanked")
        || fields.contains_key("yanked_reason")
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "upload_time/yanked fields require a mirror upload (mirror=true, admin credential)"
                .into(),
        ));
    }

    // PEP 740: pypiron relays PyPI's already-verified provenance through the
    // proxy/sync mirror paths, but is not itself a verifying authority and
    // cannot synthesize a valid provenance object from a bare `attestations`
    // array (it has no Trusted Publisher identity). Refuse first-party
    // attestations fail-closed rather than store something no verifier trusts.
    if !is_mirror && fields.contains_key("attestations") {
        return Err((
            StatusCode::BAD_REQUEST,
            "pypiron relays mirrored provenance (via the proxy and sync) but does not verify \
             first-party attestations; re-run the upload without --attestations"
                .into(),
        ));
    }

    let upload_time = match fields.get("upload_time") {
        Some(ts) => {
            if OffsetDateTime::parse(ts, &Rfc3339).is_err() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("upload_time is not RFC 3339: {ts}"),
                ));
            }
            ts.clone()
        }
        None => now_rfc3339(),
    };
    let yanked = if is_mirror {
        match (fields.get("yanked_reason"), fields.get("yanked")) {
            (Some(reason), _) if !reason.trim().is_empty() => {
                Yanked::Reason(reason.trim().to_string())
            }
            (_, Some(flag)) => Yanked::Flag(flag == "true"),
            _ => Yanked::Flag(false),
        }
    } else {
        Yanked::Flag(false)
    };

    // Hand off to the storage-protocol core. The handler has finished every HTTP
    // concern (auth, multipart spool, validation, digest, mirror gating); the
    // origin/claim/fence/commit machine below is what a deterministic simulator
    // drives without axum.
    let req = PublishRequest {
        pkg: pkg_norm,
        filename,
        body: PublishBody::Spool(spooled.path),
        sha256,
        size: spooled.size,
        version,
        requires_python: fields.get("requires_python").cloned(),
        is_mirror,
        upload_time,
        yanked,
        wheel_metadata,
        is_wheel,
        provenance: fields.get("provenance").cloned(),
    };
    // Pin the storage context once for the whole upload (design §3): the origin
    // claim, artifact/sidecar writes, and commit marker all land on this handle.
    let pinned = state.pin();
    publish_record(&state, &pinned, req).await
}

/// The storage-protocol core of an upload: origin observation → private-prefix
/// and cross-origin rejects → intent marker → origin claim (with the early
/// package-level fan-out) → mirror sidecar create → write fence → verified
/// artifact store → mirror post-publish claim re-check → tombstone/frozen
/// filename fence → PEP 658 metadata, PEP 740 provenance, sidecar → commit
/// marker → replication fan-out → read-your-writes index wait → ack. Split out
/// of [`legacy_upload`] so a deterministic simulator can exercise this state
/// machine directly; the handler owns every HTTP concern above it.
pub async fn publish_record(
    state: &Arc<AppState>,
    pinned: &buckets::Pinned,
    req: PublishRequest,
) -> Result<(StatusCode, &'static str), (StatusCode, String)> {
    let PublishRequest {
        pkg: pkg_norm,
        filename,
        body,
        sha256,
        size,
        version,
        requires_python,
        is_mirror,
        upload_time,
        yanked,
        wheel_metadata,
        is_wheel,
        provenance,
    } = req;
    let key = format!("{PACKAGES_PREFIX}{pkg_norm}/{filename}");

    // Origin exclusivity: each package belongs to exactly one world. A
    // mismatch is a hard error, never a merge — the dependency-confusion
    // defense. Storage errors are outages (503), never "unclaimed".
    let desired_origin = if is_mirror {
        origin::MIRROR
    } else {
        origin::PRIVATE
    };
    let storage = pinned.storage.as_ref();
    let observed_origin = origin::read_origin_observation(storage, &pkg_norm)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error reading origin: {e}"),
            )
        })?;
    let mut write_fence = observed_origin.as_ref().cloned();
    // The private namespace is off-limits to mirrors regardless of claim
    // state — checked here, not only at first write, so adopting a prefix
    // after a name was mirror-claimed still shuts the door.
    if is_mirror {
        if let Some(prefix) = &state.private_prefix {
            if names::matches_prefix(&pkg_norm, prefix) {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("'{pkg_norm}' is inside the private namespace '{prefix}'; mirrors may not touch it"),
                ));
            }
        }
    }
    if let Some(owner) = observed_origin.as_ref().map(|observed| observed.state) {
        if matches!(
            owner,
            origin::OriginState::Mirror | origin::OriginState::Private
        ) && owner.as_str() != desired_origin
        {
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "Package '{pkg_norm}' is {}-owned; {desired_origin} uploads are rejected",
                    owner.as_str()
                ),
            ));
        }
    }

    // The crash-recovery marker is correctness-critical in every mode: it is the
    // only durable signal that carries a global-index membership change (a new
    // name appearing) to the worker. Dropping it before touching truth — and
    // refusing the write if it fails — keeps the audit a safety net for external
    // change, never a substitute for pypiron's own bookkeeping.
    let intent_nonce = Some(worker::mark_intent(storage, &pkg_norm).await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("failed to reserve package write: {e}"),
        )
    })?);
    match observed_origin.as_ref().map(|observed| observed.state) {
        Some(origin::OriginState::Mirror) if desired_origin == origin::MIRROR => {}
        Some(origin::OriginState::Private) if desired_origin == origin::PRIVATE => {}
        Some(owner @ (origin::OriginState::Mirror | origin::OriginState::Private)) => {
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "Package '{pkg_norm}' is {}-owned; {desired_origin} uploads are rejected",
                    owner.as_str()
                ),
            ));
        }
        None | Some(origin::OriginState::Unclaimed) => {
            // A new private name must be inside the prefix; existing private
            // packages outside a newly-adopted prefix are grandfathered (only
            // first claims are gated, so adopting a prefix never bricks them).
            if let Some(prefix) = &state.private_prefix {
                if !is_mirror && !names::matches_prefix(&pkg_norm, prefix) {
                    return Err((
                        StatusCode::FORBIDDEN,
                        format!(
                            "Package '{pkg_norm}' does not match the private prefix '{prefix}'"
                        ),
                    ));
                }
            }
            // First write claims the package — atomically, so racing private
            // and mirror first-writes can't merge origins.
            let claim = origin::claim_origin(
                storage,
                &pkg_norm,
                origin::ClaimRequest::new(
                    desired_origin,
                    observed_origin
                        .as_ref()
                        .filter(|observed| observed.state == origin::OriginState::Unclaimed),
                ),
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to claim origin: {e}"),
                )
            })?;
            if claim.owner != desired_origin {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!(
                        "Package '{pkg_norm}' is {}-owned; {desired_origin} uploads are rejected",
                        claim.owner
                    ),
                ));
            }
            // A claim can survive even when the uploader dies before writing an
            // artifact. Fan the package-level claim out to every healthy bucket
            // before the artifact even lands locally, so the private name is
            // reserved fleet-wide ahead of its bytes (the dependency-confusion
            // boundary); the later artifact fan-out re-claims idempotently.
            if !is_mirror && claim.etag.is_some() && state.buckets.is_multi() {
                replicate::fanout_sync(state, pinned, &pkg_norm, replicate::ORIGIN_MARKER).await;
            }
            write_fence = Some(match claim.etag {
                Some(etag) => origin::OriginObservation {
                    state: if is_mirror {
                        origin::OriginState::Mirror
                    } else {
                        origin::OriginState::Private
                    },
                    etag,
                },
                None => origin::read_origin_observation(storage, &pkg_norm)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            format!("storage error re-reading origin claim: {e}"),
                        )
                    })?
                    .filter(|observed| observed.state.as_str() == desired_origin)
                    .ok_or_else(|| {
                        (
                            StatusCode::CONFLICT,
                            format!("Package '{pkg_norm}' changed origin while claiming"),
                        )
                    })?,
            });
        }
    }

    let sc = Sidecar {
        sha256,
        size,
        version,
        upload_time,
        requires_python,
        yanked,
        // Per-artifact origin (§4/§6.2): the replicator decides "private only"
        // from state, never from history.
        origin: Some(desired_origin.to_string()),
        upload_epoch_ms: (!is_mirror).then(now_epoch_millis),
        yank_epoch: 0,
    };
    let sc_bytes = serde_json::to_vec(&sc).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode sidecar".to_string(),
        )
    })?;
    if is_mirror {
        let sc_key = sidecar_key(&key);
        let created = storage
            .put_if_absent(&sc_key, sc_bytes.clone(), Some("application/json"))
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("Failed to store mirror sidecar: {e}"),
                )
            })?;
        if !created {
            let existing = storage.get_bytes(&sc_key).await.map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("Failed to verify mirror sidecar: {e}"),
                )
            })?;
            if existing != sc_bytes {
                let _ = commit_marker(state, storage, &pkg_norm, intent_nonce).await;
                return Err((
                    StatusCode::CONFLICT,
                    format!("File metadata already exists: {filename}"),
                ));
            }
        }
    }

    // Every multi-bucket writer consumes the exact origin observation it began
    // under, closing concurrent origin changes around its artifact write.
    if let Some(ref expected) = write_fence {
        match origin::read_origin_observation(storage, &pkg_norm).await {
            Ok(Some(current)) if current == *expected => {}
            Ok(_) => {
                if let Err(e) = commit_marker(state, storage, &pkg_norm, intent_nonce).await {
                    warn!(error=?e, "legacy: failed to close abandoned intent marker");
                }
                return Err((
                    StatusCode::CONFLICT,
                    format!("Package '{pkg_norm}' changed origin during upload"),
                ));
            }
            Err(e) => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error re-checking origin claim: {e}"),
                ));
            }
        }
    }

    // Ordering invariant: artifact, then sidecars, then index job.
    // The conditional create IS the immutability rule (pypi.org's): a plain
    // HEAD-then-PUT is a TOCTOU hole that lets concurrent uploads swap bytes.
    // The write is verified (D1) and bounded (D3): a 200 that landed zero bytes
    // never acks, and a wedged connection fails fast instead of parking on the
    // one-hour transport ceiling. Immutability is preserved — an existing body
    // is still a 409; only this writer's own corrupt debris is cleared so a
    // retry starts from a clean key.
    let artifact_body = match &body {
        PublishBody::Spool(temp) => storage::ArtifactBody::Spool(temp.path()),
        PublishBody::Bytes(bytes) => storage::ArtifactBody::Bytes(bytes.clone()),
    };
    match storage::store_artifact_verified(
        storage,
        &key,
        artifact_body,
        size,
        Some("application/octet-stream"),
        storage::Existing::Reject,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Err((
                StatusCode::CONFLICT,
                format!("File already exists: {filename}"),
            ));
        }
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Failed to store file: {e}"),
            ));
        }
    }

    // The mirror sidecar already exists. In multi-bucket mode, if demotion won
    // after the final fence, leave the typed loser in place for private-precedence
    // quarantine; deleting here would race a newer private body under the same
    // immutable key. Single-bucket mode cannot demote behind this writer, so the
    // shared helper returns without another storage read.
    if is_mirror {
        let Some(expected) = write_fence.as_ref() else {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "mirror upload lost its origin fence".to_string(),
            ));
        };
        match post_publish_mirror_claim_is_current(state, storage, &pkg_norm, expected).await {
            Ok(true) => {}
            Ok(false) => {
                if let Err(e) = commit_marker(state, storage, &pkg_norm, intent_nonce).await {
                    warn!(error=?e, "legacy: failed to close post-publish origin race");
                }
                return Err((
                    StatusCode::CONFLICT,
                    format!("Package '{pkg_norm}' changed origin during mirror upload"),
                ));
            }
            Err(e) => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error re-checking mirror claim after publish: {e}"),
                ));
            }
        }
    }

    // One post-create tombstone HEAD preserves the single-bucket write path.
    // Multi-bucket mode also checks `.frozen`, whose first-write ordering makes
    // every interrupted freeze a durable filename fence. A fenced multi-bucket
    // loser stays occupied and inert: deleting by key here could erase a private
    // replacement that landed after this writer's cross-object read.
    let filename_fenced = if state.buckets.is_multi() {
        futures::future::try_join(
            storage.head_exists(&tombstone_key(&key)),
            storage.head_exists(&frozen_key(&key)),
        )
        .await
        .map(|(tombstoned, frozen)| tombstoned || frozen)
    } else {
        storage.head_exists(&tombstone_key(&key)).await
    };
    match filename_fenced {
        Ok(false) => {}
        Ok(true) => {
            if !state.buckets.is_multi() {
                let _ = storage.delete_keys(std::slice::from_ref(&key)).await;
            }
            if let Err(e) = commit_marker(state, storage, &pkg_norm, intent_nonce).await {
                warn!(error=?e, "legacy: failed to close fenced upload intent");
            }
            return Err((
                StatusCode::CONFLICT,
                format!("File '{filename}' is frozen or deleted and cannot be reused"),
            ));
        }
        Err(e) => {
            if !state.buckets.is_multi() {
                let _ = storage.delete_keys(std::slice::from_ref(&key)).await;
            }
            if let Err(commit_error) = commit_marker(state, storage, &pkg_norm, intent_nonce).await
            {
                warn!(error=?commit_error, "legacy: failed to close filename-fence upload intent");
            }
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error checking filename fence: {e}"),
            ));
        }
    }

    // PEP 658: capture the wheel's METADATA as a static file next to it.
    if is_wheel {
        match wheel_metadata {
            Some(md) => {
                let write = if is_mirror {
                    storage
                        .put_if_absent(&metadata_key(&key), md, Some("text/plain; charset=utf-8"))
                        .await
                        .map(|_| ())
                } else {
                    storage
                        .put_bytes(&metadata_key(&key), md, Some("text/plain; charset=utf-8"))
                        .await
                };
                if let Err(e) = write {
                    warn!(error=?e, %filename, "failed to store PEP 658 metadata");
                }
            }
            None => warn!(%filename, "wheel has no extractable METADATA"),
        }
    }

    // PEP 740: store the relayed provenance object next to the artifact. Only
    // mirror uploads carry it (`sync --to` forwards PyPI's provenance verbatim);
    // first-party attestations were refused above. Best-effort, like metadata:
    // a missing companion only drops the supply-chain signal.
    if is_mirror {
        if let Some(prov) = provenance.as_ref() {
            if let Err(e) = storage
                .put_if_absent(
                    &provenance_key(&key),
                    prov.clone().into_bytes(),
                    Some("application/json"),
                )
                .await
            {
                warn!(error=?e, %filename, "failed to store PEP 740 provenance");
            }
        }
    }

    if !is_mirror {
        storage
            .put_bytes(&sidecar_key(&key), sc_bytes, Some("application/json"))
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to store sidecar: {e}"),
                )
            })?;
    }

    // Commit marker: truth changed, rebuild now. Pairs with the intent above
    // so the worker consumes both; if this write fails the intent still goes
    // stale and heals the package.
    if let Err(e) = commit_marker(state, storage, &pkg_norm, intent_nonce).await {
        warn!(error=?e, "legacy: failed to write commit marker");
    }

    // Stream the record to every other healthy bucket before the ack; any
    // bucket that misses gets a durable `_repl/` note for the sweep. Mirror
    // cache content is intentionally local and pays none of this cost.
    if !is_mirror {
        replicate::fanout_sync(state, pinned, &pkg_norm, &filename).await;
    }

    // Read-your-writes by waiting: poll our own index until the file shows
    // up, so publish-then-install pipelines never see a missing version.
    if state.wait_on_upload {
        wait_for_index_visibility(state, storage, &pkg_norm, &filename).await;
    }

    // Return a simple OK text body compatible with legacy clients.
    Ok((StatusCode::OK, "OK"))
}

/// Bounded wait for a freshly uploaded file to appear in the package index.
/// A timeout still returns success upstream — the artifact is durable and the
/// index will catch up; failing the upload would only provoke a client retry
/// into the 409 from immutability.
async fn wait_for_index_visibility(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
) {
    let key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
    let deadline = std::time::Instant::now() + state.wait_on_upload_timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(bytes) = storage.get_bytes(&key).await {
            #[derive(serde::Deserialize)]
            struct Index {
                files: Vec<File>,
            }
            #[derive(serde::Deserialize)]
            struct File {
                filename: String,
            }
            if let Ok(idx) = serde_json::from_slice::<Index>(&bytes) {
                if idx.files.iter().any(|f| f.filename == filename) {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    warn!(%pkg, %filename, "wait-on-upload: index visibility wait timed out");
}

/// Current time as RFC 3339 at whole-second precision.
fn now_rfc3339() -> String {
    crate::clock::now_utc()
        .replace_nanosecond(0)
        .unwrap_or_else(|_| crate::clock::now_utc())
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Current Unix epoch time in milliseconds for the private-upload conflict
/// tiebreak. A pre-epoch or unrepresentable system clock degrades to a value
/// that conflict reconciliation will quarantine rather than trusting blindly.
fn now_epoch_millis() -> u64 {
    crate::clock::now_epoch_millis()
}

/// Commit a truth change, pairing with `intent_nonce` when the intent marker
/// landed (so the worker consumes both), and wake the worker now instead of
/// letting the marker wait out the tick — upload→visible drops from
/// ~tick+rebuild to ~rebuild. Peer nodes still ride the marker/tick path;
/// the nudge is a same-process accelerant only.
pub(crate) async fn commit_marker(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    intent_nonce: Option<String>,
) -> Result<()> {
    match intent_nonce {
        Some(nonce) => worker::mark_commit(storage, pkg, &nonce).await?,
        None => worker::mark_dirty(storage, pkg).await?,
    }
    state.worker_nudge.notify_one();
    Ok(())
}

/// Re-check a mirror writer's exact claim after its artifact becomes visible.
/// Only multi-bucket replication can demote that claim concurrently. Keeping the
/// single-bucket branch I/O-free preserves the original serving-path cost.
pub(crate) async fn post_publish_mirror_claim_is_current(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    expected: &origin::OriginObservation,
) -> Result<bool> {
    if !state.buckets.is_multi() {
        return Ok(true);
    }
    Ok(origin::read_origin_observation(storage, pkg)
        .await?
        .as_ref()
        == Some(expected))
}

/// --- Simple index endpoints ----------------------------------------------
const CT_JSON: &str = render::SIMPLE_JSON_CONTENT_TYPE;
const CT_HTML: &str = render::SIMPLE_HTML_CONTENT_TYPE;
/// Indexes change on every rebuild: always revalidate, never stale.
const INDEX_CACHE_CONTROL: &str = "no-cache";
/// Filenames are immutable, so artifact bytes can be cached forever.
const ARTIFACT_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

pub(crate) async fn require_settled_package_read(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<origin::OriginObservation>> {
    if !state.buckets.is_multi() {
        return Ok(None);
    }
    origin::read_origin_observation(storage, pkg).await
}

/// Multi-bucket markers are visibility fences, not merely index hints. A
/// quarantined mirror body deliberately keeps its canonical key occupied, so
/// direct and presigned downloads must reject it too. A stale quarantine marker
/// becomes inert only after a private sidecar proves private precedence won.
async fn multi_bucket_file_visible(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_key: &str,
) -> Result<bool> {
    if !state.buckets.is_multi() {
        return Ok(true);
    }
    let Some(claim) = require_settled_package_read(state, storage, pkg).await? else {
        return Ok(false);
    };
    if claim.state == origin::OriginState::Unclaimed {
        return Ok(false);
    }
    let ((tombstoned, frozen), mirror_quarantined) = futures::future::try_join(
        futures::future::try_join(
            storage.head_exists(&tombstone_key(artifact_key)),
            storage.head_exists(&frozen_key(artifact_key)),
        ),
        storage.head_exists(&mirror_quarantined_key(artifact_key)),
    )
    .await?;
    if tombstoned || frozen {
        return Ok(false);
    }

    if mirror_quarantined || claim.state == origin::OriginState::Private {
        let sidecar = match storage.get_bytes(&sidecar_key(artifact_key)).await {
            Ok(bytes) => Some(serde_json::from_slice::<Sidecar>(&bytes)?),
            Err(error) if storage::is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        if mirror_quarantined
            && sidecar.as_ref().and_then(|value| value.origin.as_deref()) != Some(origin::PRIVATE)
        {
            return Ok(false);
        }
        if claim.state == origin::OriginState::Private
            && sidecar.as_ref().and_then(|value| value.origin.as_deref()) == Some(origin::MIRROR)
        {
            return Ok(false);
        }
    }
    if origin::read_origin_observation(storage, pkg)
        .await?
        .as_ref()
        != Some(&claim)
    {
        bail!("package '{pkg}' changed while checking artifact visibility");
    }
    Ok(true)
}

/// An unclaimed proxy companion may bypass local storage only when the package
/// has no local body, companion, or permanent visibility fence. The exact
/// claim recheck closes the LIST/HEAD window before the upstream fetch begins.
async fn unowned_companion_passthrough_safe(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_key: &str,
    companion_key: &str,
) -> Result<bool> {
    if !state.buckets.is_multi() {
        return Ok(true);
    }
    let before = require_settled_package_read(state, storage, pkg).await?;
    if before
        .as_ref()
        .is_some_and(|claim| claim.state != origin::OriginState::Unclaimed)
    {
        return Ok(false);
    }
    for key in [
        artifact_key,
        companion_key,
        &tombstone_key(artifact_key),
        &frozen_key(artifact_key),
        &mirror_quarantined_key(artifact_key),
    ] {
        if storage.head_exists(key).await? {
            return Ok(false);
        }
    }
    Ok(require_settled_package_read(state, storage, pkg).await? == before)
}

async fn companion_passthrough_visible(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_key: &str,
    companion_key: &str,
    expected_claim: &Option<origin::OriginObservation>,
) -> Result<bool> {
    let visible = if multi_bucket_file_visible(state, storage, pkg, artifact_key).await? {
        true
    } else {
        unowned_companion_passthrough_safe(state, storage, pkg, artifact_key, companion_key).await?
    };
    if !visible || !state.buckets.is_multi() {
        return Ok(visible);
    }
    Ok(require_settled_package_read(state, storage, pkg).await? == *expected_claim)
}

/// Visibility fence with read-through. Presence on the read pin is trusted; a
/// negative read-pin result is re-checked on the write pin before it can 404 (a
/// lagging region bucket must never make a client miss an acked file). The write
/// pin is authoritative — deletions and tombstones serialize there — so its
/// answer is final. Identical to a single [`multi_bucket_file_visible`] when the
/// two pins are the same bucket (dev/READ_AFFINITY_VISION.md).
async fn file_visible_read_through(
    state: &AppState,
    read: &Pinned,
    write: &Pinned,
    pkg: &str,
    artifact_key: &str,
) -> Result<bool> {
    if multi_bucket_file_visible(state, read.storage.as_ref(), pkg, artifact_key).await? {
        return Ok(true);
    }
    if read.index == write.index {
        return Ok(false);
    }
    multi_bucket_file_visible(state, write.storage.as_ref(), pkg, artifact_key).await
}

async fn simple_root(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response<Body> {
    serve_root_index(&state, accepts_json(&headers), &headers).await
}

async fn simple_root_json(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_root_index(&state, true, &headers).await
}

/// The global `/simple/` index, in JSON or HTML.
async fn serve_root_index(state: &AppState, json: bool, headers: &HeaderMap) -> Response<Body> {
    if !state.is_reader(headers) {
        return unauthorized();
    }
    let pinned = state.pin();
    let (key, ct) = if json {
        (format!("{SIMPLE_PREFIX}index.json"), CT_JSON)
    } else {
        (format!("{SIMPLE_PREFIX}index.html"), CT_HTML)
    };
    serve_index(state, &pinned, key, ct, INDEX_CACHE_CONTROL, headers).await
}

async fn simple_pkg(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_pkg_index(&state, &raw, false, &headers).await
}

async fn simple_pkg_json(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_pkg_index(&state, &raw, true, &headers).await
}

/// A package's `/simple/<pkg>/` page. `force_json` is the explicit-`index.json`
/// route (otherwise the representation is content-negotiated); it also pins the
/// canonical-redirect target so URL-keyed caches never split entries.
async fn serve_pkg_index(
    state: &AppState,
    raw: &str,
    force_json: bool,
    headers: &HeaderMap,
) -> Response<Body> {
    if !state.is_reader(headers) {
        return unauthorized();
    }
    let Some(pkg) = checked_pkg_name(raw) else {
        return not_found("invalid package name");
    };
    // PEP 503: the canonical URL is the normalized one; everything else 301s
    // there, so URL-keyed caches (CDNs, edge proxies) never split entries.
    if raw != pkg {
        let target = if force_json {
            format!("/simple/{pkg}/index.json")
        } else {
            format!("/simple/{pkg}/")
        };
        return moved_permanently(&target);
    }
    // Reads come from the region-local pin, and the origin claim that gates
    // serving is observed on whichever bucket actually serves the bytes — so a
    // region catch-up landing the `.origin` mid-serve can never trip the
    // coherence recheck. Any decision that could reach upstream — the proxy
    // index, and denying an "unclaimed" name — is settled on the write pin
    // (dev/READ_AFFINITY_VISION.md).
    let read_pinned = state.read_pin();
    let write_pinned = state.pin();
    let same_pin = read_pinned.index == write_pinned.index;
    let json = force_json || accepts_json(headers);
    let (key, ct) = if json {
        (format!("{SIMPLE_PREFIX}{pkg}/index.json"), CT_JSON)
    } else {
        (format!("{SIMPLE_PREFIX}{pkg}/index.html"), CT_HTML)
    };

    // Upstream (proxy) path: eligibility, render, and the mid-serve coherence
    // recheck all run on the write pin inside `proxy_package_index`.
    if let Some(resp) =
        proxy_package_index(state, write_pinned.storage.as_ref(), &pkg, json, headers).await
    {
        return resp;
    }

    // Local index. In multi-bucket, an "unclaimed" read-pin observation is
    // confirmed on the write pin before denying the name; a write-owned claim the
    // region bucket has not seen yet is served through from the write home.
    if state.buckets.is_multi() {
        let read_claim =
            match require_settled_package_read(state, read_pinned.storage.as_ref(), &pkg).await {
                Ok(claim) => claim,
                Err(error) => return read_error(error),
            };
        if read_claim
            .as_ref()
            .is_none_or(|value| value.state == origin::OriginState::Unclaimed)
        {
            match unclaimed_confirmed_absent(state, &write_pinned, same_pin, &pkg).await {
                Ok(true) => return not_found("no such package"),
                Ok(false) => {}
                Err(error) => return read_error(error),
            }
        }
        return serve_local_index_fenced(
            state,
            &read_pinned,
            &write_pinned,
            same_pin,
            &pkg,
            key,
            ct,
            headers,
            read_claim,
        )
        .await;
    }
    serve_local_index_fenced(
        state,
        &read_pinned,
        &write_pinned,
        same_pin,
        &pkg,
        key,
        ct,
        headers,
        None,
    )
    .await
}

/// Serve a package index from the read pin, reading through to the write home on
/// a miss — with the origin coherence recheck pinned to whichever bucket actually
/// serves the bytes. Served locally from the read pin → the pre-observation
/// (`read_baseline`, already read by the caller) and the recheck are the read
/// pin's; served through from the write home → both are the write pin's, so a
/// region catch-up mid-serve never 503s (dev/READ_AFFINITY_VISION.md).
#[allow(clippy::too_many_arguments)]
async fn serve_local_index_fenced(
    state: &AppState,
    read: &Pinned,
    write: &Pinned,
    same_pin: bool,
    pkg: &str,
    key: String,
    content_type: &'static str,
    headers: &HeaderMap,
    read_baseline: Option<origin::OriginObservation>,
) -> Response<Body> {
    let resp = serve_index(
        state,
        read,
        key.clone(),
        content_type,
        INDEX_CACHE_CONTROL,
        headers,
    )
    .await;
    if resp.status() != StatusCode::NOT_FOUND {
        // Served from the read pin: recheck the read pin against its baseline.
        if state.buckets.is_multi() {
            match require_settled_package_read(state, read.storage.as_ref(), pkg).await {
                Ok(after) if after == read_baseline => {}
                Ok(_) => {
                    return read_error(anyhow!("package '{pkg}' changed while serving its index"))
                }
                Err(error) => return read_error(error),
            }
        }
        return resp;
    }
    if same_pin {
        return resp; // 404 on the only bucket; nothing to read through to.
    }
    // Read-through to the write home: baseline and recheck are both the write
    // pin's, so a repair sweep landing the region bucket's `.origin` never trips.
    let write_baseline =
        match require_settled_package_read(state, write.storage.as_ref(), pkg).await {
            Ok(claim) => claim,
            Err(error) => return read_error(error),
        };
    let resp = serve_index_uncached(
        write.storage.as_ref(),
        &key,
        content_type,
        INDEX_CACHE_CONTROL,
        headers,
    )
    .await;
    match require_settled_package_read(state, write.storage.as_ref(), pkg).await {
        Ok(after) if after == write_baseline => {}
        Ok(_) => return read_error(anyhow!("package '{pkg}' changed while serving its index")),
        Err(error) => return read_error(error),
    }
    resp
}

/// Confirm a read-pin "no claim" against the write pin before it can deny a
/// package. Returns true when the write pin also holds no real claim (a genuine
/// 404), false when the write home owns a claim the region bucket has not yet
/// seen (serve it through). Fail-closed on names (dev/READ_AFFINITY_VISION.md).
async fn unclaimed_confirmed_absent(
    state: &AppState,
    write: &Pinned,
    same_pin: bool,
    pkg: &str,
) -> Result<bool> {
    if same_pin {
        return Ok(true);
    }
    Ok(
        require_settled_package_read(state, write.storage.as_ref(), pkg)
            .await?
            .is_none_or(|value| value.state == origin::OriginState::Unclaimed),
    )
}

/// Resolve the proxy for `pkg`, enforcing the eligibility gate (the
/// dependency-confusion defense) in one place. `None` = no proxy configured or
/// the name is ineligible (private / reserved prefix), so fall through to local
/// serving; `Some(Err)` = origin unreadable, an outage to surface rather than
/// answer "who owns this name" optimistically; `Some(Ok)` = serve upstream.
async fn eligible_proxy<'a>(
    state: &'a AppState,
    storage: &dyn Storage,
    pkg: &str,
) -> Option<Result<&'a Arc<proxy::Proxy>, Response<Body>>> {
    let proxy = state.proxy.as_ref()?;
    match proxy::eligible(state, storage, pkg).await {
        Ok(true) => Some(Ok(proxy)),
        Ok(false) => None,
        Err(e) => Some(Err(read_error(e))),
    }
}

/// Proxy hook for package pages: `Some(response)` when the page is served
/// from upstream metadata, `None` to fall through to the local materialized
/// index (proxy off, package ineligible, or upstream unavailable).
async fn proxy_package_index(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    json: bool,
    headers: &HeaderMap,
) -> Option<Response<Body>> {
    let proxy = state.proxy.as_ref()?;
    // Coherence baseline on this (write) pin, taken before the eligibility read so
    // the fence covers the whole eligible→render span: serving an upstream index
    // for a name that gains a local claim mid-serve would be a dependency-confusion
    // leak, so the origin is rechecked on the same pin before the page is returned.
    // Free in single-bucket mode (no I/O) and skipped by the recheck below.
    let before = match require_settled_package_read(state, storage, pkg).await {
        Ok(claim) => claim,
        Err(e) => return Some(read_error(e)),
    };
    match proxy::eligible(state, storage, pkg).await {
        Ok(true) => {}
        Ok(false) => return None,
        // Origin unreadable is an outage: never answer "what owns this name"
        // questions optimistically (the dependency-confusion direction).
        Err(e) => return Some(read_error(e)),
    }
    let rendered = match proxy.package_index(state, storage, pkg, json).await {
        Ok(Some(rendered)) => rendered,
        Ok(None) => return None,
        Err(error) => return Some(read_error(error)),
    };
    let revalidated = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "*" || v.contains(&*rendered.etag))
        .unwrap_or(false);
    let builder = Response::builder()
        .header(header::ETAG, &*rendered.etag)
        .header(header::CACHE_CONTROL, INDEX_CACHE_CONTROL);
    let response = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        builder
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, if json { CT_JSON } else { CT_HTML })
            .header(header::CONTENT_LENGTH, rendered.body.len())
            .body(Body::from(rendered.body.clone()))
    };
    if state.buckets.is_multi() {
        match require_settled_package_read(state, storage, pkg).await {
            Ok(after) if after == before => {}
            Ok(_) => {
                return Some(read_error(anyhow!(
                    "package '{pkg}' changed while serving its index"
                )))
            }
            Err(e) => return Some(read_error(e)),
        }
    }
    Some(response.unwrap_or_else(not_found))
}

/// Serve a materialized index file with a content-hash ETag; conditional GETs
/// revalidate to 304. Bytes and ETag come from the in-memory cache — the hot
/// path costs zero storage calls and zero hashing (see cache.rs).
async fn serve_index(
    state: &AppState,
    pinned: &Pinned,
    key: String,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    let (identity, gzip) = match state
        .index_cache
        .get(pinned.storage.as_ref(), &key, pinned.generation)
        .await
    {
        Ok(Some(hit)) => hit,
        Ok(None) => return not_found("no such index"),
        Err(e) => return read_error(e),
    };
    render_index_variant(&identity, &gzip, content_type, cache_control, headers)
}

/// Serve `key` straight from a storage handle without touching the read-pin
/// index cache — the read-through fallback for a page missing on the region
/// bucket. Bounded to the rare region-lag case; the hot path stays the cached
/// read-pin [`serve_index`] (dev/READ_AFFINITY_VISION.md).
async fn serve_index_uncached(
    storage: &dyn Storage,
    key: &str,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    match storage.get_bytes(key).await {
        Ok(bytes) => {
            let (identity, gzip) = cache::build_variants(bytes);
            render_index_variant(&identity, &gzip, content_type, cache_control, headers)
        }
        Err(e) if storage::is_not_found(&e) => not_found("no such index"),
        Err(e) => read_error(e),
    }
}

/// Serve an index/companion from the read pin, reading through to the write pin
/// on a miss so a lagging region bucket never 404s a page the write home holds
/// (dev/READ_AFFINITY_VISION.md). Identical to [`serve_index`] when the two pins
/// are the same bucket. The write-pin fallback renders uncached, keeping package
/// keys populated only from the read pin.
async fn serve_index_local(
    state: &AppState,
    read: &Pinned,
    write: &Pinned,
    key: String,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    let resp = serve_index(
        state,
        read,
        key.clone(),
        content_type,
        cache_control,
        headers,
    )
    .await;
    if read.index == write.index || resp.status() != StatusCode::NOT_FOUND {
        return resp;
    }
    serve_index_uncached(
        write.storage.as_ref(),
        &key,
        content_type,
        cache_control,
        headers,
    )
    .await
}

/// Render one cached index representation, negotiating gzip and conditional GETs.
fn render_index_variant(
    identity: &cache::Variant,
    gzip: &Option<cache::Variant>,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    // Content negotiation against the precompressed variant: zero per-request
    // CPU — big indexes were NIC-bound, and gzip is a ~5-7x cut in bytes.
    // Each representation carries its own strong ETag (hence Vary).
    let accepts_gzip = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("gzip"))
        .unwrap_or(false);
    let (variant, encoding) = match (gzip, accepts_gzip) {
        (Some(gz), true) => (gz, Some("gzip")),
        _ => (identity, None),
    };

    let revalidated = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "*" || v.contains(&*variant.etag) || v.contains(&*identity.etag))
        .unwrap_or(false);

    let mut builder = Response::builder()
        .header(header::ETAG, &*variant.etag)
        .header(header::VARY, "Accept-Encoding")
        .header(header::CACHE_CONTROL, cache_control);
    if let Some(enc) = encoding {
        builder = builder.header(header::CONTENT_ENCODING, enc);
    }

    let result = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        builder
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, variant.body.len())
            // Bytes clone = refcount bump; hyper streams the shared buffer.
            .body(Body::from(variant.body.clone()))
    };
    result.unwrap_or_else(not_found)
}

/// --- Artifact download endpoint ------------------------------------------
/// Serves artifacts and their PEP 658 `<filename>.metadata` companions; both
/// are immutable. Sidecar JSON and dotfiles are not served.
async fn files_get(
    State(state): State<Arc<AppState>>,
    method: Method,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    // A request is for an artifact or one of its served companions
    // (`.metadata`, `.provenance`); the sidecar JSON and dotfiles never serve.
    let servable = match filename
        .strip_suffix(METADATA_SUFFIX)
        .or_else(|| filename.strip_suffix(PROVENANCE_SUFFIX))
    {
        Some(base) => sidecar::is_artifact(base),
        None => sidecar::is_artifact(&filename),
    };
    let Some(pkg) = checked_pkg_name(&package)
        .filter(|_| servable && !filename.contains('/') && !filename.contains('\\'))
    else {
        return not_found("not an artifact");
    };
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let artifact_filename = filename
        .strip_suffix(METADATA_SUFFIX)
        .or_else(|| filename.strip_suffix(PROVENANCE_SUFFIX))
        .unwrap_or(&filename);
    let artifact_key = format!("{PACKAGES_PREFIX}{pkg}/{artifact_filename}");

    // Pin both selections once for the whole download (design §3). Reads —
    // fences, companion cache, presign, streaming — run against the region-local
    // read pin; the proxy fill and every upstream-claim decision run against the
    // write pin (bytes from the near bucket, judgment from the write home,
    // dev/READ_AFFINITY_VISION.md). In the common mode the two are one context.
    let read_pinned = state.read_pin();
    let write_pinned = state.pin();
    let same_pin = read_pinned.index == write_pinned.index;

    // Download attribution key, computed once: a real artifact only (companions
    // and the ranged-companion fall-through below parse to None), keyed
    // `<pkg>/<filename>` so the counter store rolls files up to versions. Counted
    // at the two delivery exits (302 redirect, 200 stream) — see counters.rs. A
    // HEAD transfers no body (axum routes it to this GET handler), so it is not a
    // download: gate on GET so a bodiless probe never inflates the count.
    let dl_key = (method == Method::GET && sidecar::is_artifact(&filename))
        .then(|| format!("{pkg}/{filename}"));

    // PEP 658 metadata is immutable, tiny, and hammered by resolvers (uv
    // fetches one per candidate wheel) — serve it from the same RAM cache as
    // the indexes instead of one storage GET per request. Range requests
    // fall through to storage; nobody range-reads a METADATA file.
    if filename.ends_with(METADATA_SUFFIX) && headers.get(header::RANGE).is_none() {
        match file_visible_read_through(&state, &read_pinned, &write_pinned, &pkg, &artifact_key)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                // Upstream passthrough is judged on the write pin: an unclaimed
                // origin must be authoritative before any fall-through.
                match unowned_companion_passthrough_safe(
                    &state,
                    write_pinned.storage.as_ref(),
                    &pkg,
                    &artifact_key,
                    &key,
                )
                .await
                {
                    Ok(true) => {
                        if let Some(upstream) = proxy_metadata_passthrough(
                            &state,
                            write_pinned.storage.as_ref(),
                            &pkg,
                            &filename,
                        )
                        .await
                        {
                            return upstream;
                        }
                    }
                    Ok(false) => {}
                    Err(error) => return read_error(error),
                }
                return not_found("artifact is fenced");
            }
            Err(error) => return read_error(error),
        }
        let resp = serve_index_local(
            &state,
            &read_pinned,
            &write_pinned,
            key,
            "text/plain; charset=utf-8",
            ARTIFACT_CACHE_CONTROL,
            &headers,
        )
        .await;
        // Not stored yet (wheel not cached): pass upstream metadata through
        // without writing anything — a resolver probing dozens of candidate
        // wheels must not stampede gigabytes into storage. The companion is
        // stored when the wheel itself is downloaded.
        if resp.status() == StatusCode::NOT_FOUND {
            if let Some(upstream) =
                proxy_metadata_passthrough(&state, write_pinned.storage.as_ref(), &pkg, &filename)
                    .await
            {
                return upstream;
            }
        }
        return resp;
    }

    // PEP 740 provenance companion: same RAM-cache + passthrough story as
    // metadata, served as JSON. A mirror snapshot is point-in-time, so it is
    // cached as immutably as the artifact it describes.
    if filename.ends_with(PROVENANCE_SUFFIX) && headers.get(header::RANGE).is_none() {
        match file_visible_read_through(&state, &read_pinned, &write_pinned, &pkg, &artifact_key)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                match unowned_companion_passthrough_safe(
                    &state,
                    write_pinned.storage.as_ref(),
                    &pkg,
                    &artifact_key,
                    &key,
                )
                .await
                {
                    Ok(true) => {
                        if let Some(upstream) = proxy_provenance_passthrough(
                            &state,
                            write_pinned.storage.as_ref(),
                            &pkg,
                            &filename,
                        )
                        .await
                        {
                            return upstream;
                        }
                    }
                    Ok(false) => {}
                    Err(error) => return read_error(error),
                }
                return not_found("artifact is fenced");
            }
            Err(error) => return read_error(error),
        }
        let resp = serve_index_local(
            &state,
            &read_pinned,
            &write_pinned,
            key,
            "application/json",
            ARTIFACT_CACHE_CONTROL,
            &headers,
        )
        .await;
        if resp.status() == StatusCode::NOT_FOUND {
            if let Some(upstream) =
                proxy_provenance_passthrough(&state, write_pinned.storage.as_ref(), &pkg, &filename)
                    .await
            {
                return upstream;
            }
        }
        return resp;
    }

    // On-demand mirroring: make sure the artifact is in storage before the
    // presign/stream logic runs (a presigned redirect never observes a 404,
    // so the fetch can't be triggered by one). The fill runs entirely on the
    // write pin — origin claims and the 409-serialized PUT stay on the write home.
    if let Some(resp) = proxy_ensure_artifact(
        &state,
        write_pinned.storage.as_ref(),
        &pkg,
        &filename,
        write_pinned.generation,
    )
    .await
    {
        return resp;
    }
    // Malware byte gate: the single enforcement chokepoint, before the
    // presign/stream split so a cached signed URL is gated too. Origin is judged
    // on the write home. A no-op unless blocking is armed and a snapshot is fed.
    if let Some(resp) = advisory_byte_gate(
        &state,
        write_pinned.storage.as_ref(),
        &pkg,
        artifact_filename,
    )
    .await
    {
        return resp;
    }
    match file_visible_read_through(&state, &read_pinned, &write_pinned, &pkg, &artifact_key).await
    {
        Ok(true) => {}
        Ok(false) => return not_found("artifact is fenced"),
        Err(error) => return read_error(error),
    }

    // S3 serves the megabytes, this node serves kilobytes of index: redirect
    // artifact downloads to a presigned URL — but only for clients whose
    // caches survive URL churn (see ArtifactDelivery). Metadata companions
    // are tiny and resolution-critical, so they always stream. The redirect
    // itself must not be cached — the signature expires.
    let redirect = match state.artifact_delivery {
        ArtifactDelivery::Stream => false,
        ArtifactDelivery::Redirect => true,
        ArtifactDelivery::Auto => redirect_safe_client(&headers),
    };
    if redirect && !filename.ends_with(METADATA_SUFFIX) && !filename.ends_with(PROVENANCE_SUFFIX) {
        // No artifact-existence check: presigning itself is local HMAC math.
        // Multi-bucket mode already paid its origin/marker visibility reads
        // above; single-bucket mode still adds no storage round trip here. A
        // signed URL to a missing key gets S3's own 404 (the server's
        // credentials carry s3:ListBucket —
        // required for index rebuilds — which is what makes S3 say 404
        // rather than 403). Existence is the index's job, not this path's.
        // Immutability also makes signed URLs reusable across clients: serve
        // a cached one while it has plenty of validity left (see cache.rs).
        // The presign cache is keyed by the (shared) read-pin generation and
        // populated only from this read-pin-routed path.
        if let Some(url) = state.presign_cache.fresh(&key, read_pinned.generation) {
            if let Some(k) = &dl_key {
                state.counters.record("downloads", k);
                state.metrics.record_download();
            }
            return found_redirect(&url);
        }
        // Presign the bucket that actually holds the bytes: the read pin when the
        // object is present there, otherwise the write pin — never hand out a URL
        // that will 404. The HEAD is skipped when the two pins are one bucket, so
        // single-region and no-affinity nodes add no round trip here.
        let presign_storage = if same_pin {
            read_pinned.storage.clone()
        } else {
            match read_pinned.storage.head_exists(&key).await {
                Ok(true) => read_pinned.storage.clone(),
                Ok(false) => write_pinned.storage.clone(),
                Err(e) => {
                    warn!(error=?e, %key, "read-pin existence check failed; presigning the write pin");
                    write_pinned.storage.clone()
                }
            }
        };
        match presign_storage
            .presign_get(&key, cache::PRESIGN_EXPIRY)
            .await
        {
            Ok(Some(url)) => {
                let url: Arc<str> = url.into();
                state
                    .presign_cache
                    .put(&key, url.clone(), read_pinned.generation);
                if let Some(k) = &dl_key {
                    state.counters.record("downloads", k);
                    state.metrics.record_download();
                }
                return found_redirect(&url);
            }
            Ok(None) => {} // disk backend: fall through to streaming
            Err(e) => warn!(error=?e, %key, "presign failed; falling back to streaming"),
        }
    }

    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    // Stream from the read pin; on any failure (a not-found on a lagging region
    // bucket, or an error) read through to the write pin once before mapping to
    // 404/503 (dev/READ_AFFINITY_VISION.md).
    let mut resp = match read_pinned.storage.serve_artifact(&key, range).await {
        Ok(resp) => resp,
        Err(read_err) => {
            if same_pin {
                return read_error(read_err);
            }
            match write_pinned.storage.serve_artifact(&key, range).await {
                Ok(resp) => resp,
                Err(e) => return read_error(e),
            }
        }
    };
    // Count only a full delivered body (200): a 206 range read is a partial of
    // one logical download, a 416 is none. (A whole-file range served as 206 —
    // rare, e.g. `curl -C-`/`wget -c` — is undercounted; download stats are
    // best-effort, so we don't parse Content-Range.)
    if resp.status() == StatusCode::OK {
        if let Some(k) = &dl_key {
            state.counters.record("downloads", k);
            state.metrics.record_download();
        }
    }
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(ARTIFACT_CACHE_CONTROL),
    );
    resp
}

/// Per-package counter series: `GET /stats/:metric/:package` (read-auth gated).
/// Up to the last 30 days of daily counts, filenames rolled up to versions, plus
/// a grand total. Frozen days are exact; today is best-effort. Deliberately a
/// separate surface from `/metrics`, which stays low-cardinality.
async fn stats_get(
    State(state): State<Arc<AppState>>,
    Path((metric, package)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let Some(pkg) = checked_pkg_name(&package) else {
        return not_found("not a package");
    };
    let to = OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    let series = state.counters.query_package(&metric, &pkg, from, to).await;

    let mut days: std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>> =
        std::collections::BTreeMap::new();
    let mut total: u64 = 0;
    for (day, files) in series {
        let by_ver = days.entry(day).or_default();
        for (filename, count) in files {
            total += count;
            let ver = infer_version_from_filename(&filename).unwrap_or_else(|| "unknown".into());
            *by_ver.entry(ver).or_insert(0) += count;
        }
    }
    json_response(serde_json::json!({
        "metric": metric,
        "package": pkg,
        "total": total,
        "days": days,
    }))
}

/// Global counter summary: `GET /stats/:metric` (read-auth gated). The last 30
/// days of per-day totals and the busiest packages, from the leader-written
/// per-day summaries (top keys are rolled up to packages — approximate at the
/// tail, fine for a dashboard glance).
async fn stats_summary_get(
    State(state): State<Arc<AppState>>,
    Path(metric): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let to = OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    let summaries = state.counters.query_summaries(&metric, from, to).await;

    let mut total: u64 = 0;
    let mut days: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for (day, s) in &summaries {
        total += s.total;
        days.insert(day.clone(), s.total);
    }
    let mut ranked = rank_packages(&summaries);
    ranked.truncate(20);
    json_response(serde_json::json!({
        "metric": metric,
        "total": total,
        "days": days,
        "top": ranked.into_iter().collect::<std::collections::BTreeMap<_, _>>(),
    }))
}

/// A `200 application/json` response with no-store caching, or a 404 if the body
/// can't be built. Shared by the `/stats` endpoints.
fn json_response(value: serde_json::Value) -> Response<Body> {
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(bytes))
        .unwrap_or_else(not_found)
}

/// A `403 application/json` refusal with no-store caching — the malware byte
/// gate's response shape ([`json_response`]'s 403 sibling; that one is 200-only).
fn blocked_response(value: serde_json::Value) -> Response<Body> {
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(bytes))
        .unwrap_or_else(not_found)
}

/// The malware byte gate: the single enforcement chokepoint where advisory-blocked
/// bytes are refused. Runs once in [`files_get`] before the presign/stream split,
/// so a cached signed URL is gated too. `Some(403)` refuses; `None` allows.
///
/// The common path is a pure hash probe with zero I/O: disabled, unfed, or no hit
/// all return `None` before any storage read. Only a genuine advisory/quarantine
/// hit pays the origin read that proves the name isn't private — OSV names live in
/// PyPI's namespace, and origin exclusivity is the proof that a same-named private
/// package is not that package. `storage` is the write-pin (origin claims are
/// judged on the write home). Fail-closed throughout: an unclaimed/mirror origin
/// or a storage error on a hit blocks.
async fn advisory_byte_gate(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_filename: &str,
) -> Option<Response<Body>> {
    if !state.malware_block {
        return None;
    }
    let snap = state.advisory_snapshot();
    if !snap.has_block_data() && snap.quarantined.is_empty() {
        return None; // armed but unfed: nothing to block yet
    }
    let version = infer_version_from_filename(artifact_filename);
    // Baseline block set ∪ the per-node probe overlay.
    let ids: Vec<String> = snap.blocking(pkg, version.as_deref());
    let quarantined = snap.quarantined.contains(pkg);
    if ids.is_empty() && !quarantined {
        return None; // the common path: no origin read, no I/O
    }

    // A hit — but a private-origin name never consults either set. Fast pre-check
    // on the configured private prefix (no I/O), then the authoritative claim.
    if let Some(prefix) = &state.private_prefix {
        if names::matches_prefix(pkg, prefix) {
            return None;
        }
    }
    match origin::read_origin_claim(storage, pkg).await {
        Ok(Some(origin::OriginState::Private)) => return None,
        // Mirror, the unclaimed sentinel, or no claim at all: not proven private,
        // so a same-named blocked artifact is refused (fail-closed).
        Ok(_) => {}
        Err(e) => {
            // Only reachable on a probe hit; a storage read error fails closed.
            warn!(error = ?e, %pkg, "advisory gate: origin read failed; blocking fail-closed");
        }
    }

    state.metrics.record_blocked_download();
    if ids.is_empty() {
        warn!(%pkg, "blocked download: project quarantined upstream");
        return Some(blocked_response(serde_json::json!({
            "error": "project quarantined upstream",
            "package": pkg,
        })));
    }
    warn!(%pkg, version = ?version, advisories = ?ids, "blocked download: malware advisory");
    Some(blocked_response(serde_json::json!({
        "error": "blocked by malware advisory",
        "package": pkg,
        "version": version,
        "advisories": ids,
    })))
}

/// Proxy hook for artifact downloads: fetch-and-commit on a local miss.
/// `None` means fall through to normal serving (the file is now in storage,
/// was already there, or doesn't exist upstream either); `Some` is a hard
/// failure response (storage outage, upstream verification failure).
async fn proxy_ensure_artifact(
    state: &Arc<AppState>,
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    generation: u64,
) -> Option<Response<Body>> {
    let proxy = state.proxy.as_ref()?;
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    // Warm-hit fast path: an artifact already in local storage is always safe to
    // serve as-is. The origin/eligibility fence exists to gate *upstream fetches*,
    // not local delivery, so a cached file skips it entirely — and skips the
    // origin read it would cost — and falls through to normal serving. This is the
    // whole optimization: a warm proxied download drops from three storage ops
    // (origin read + existence HEAD + serve) to one (serve).
    match proxy
        .artifact_cached_locally(storage, &key, generation)
        .await
    {
        Ok(true) => return None,
        Ok(false) => {}
        Err(e) => return Some(read_error(e)),
    }
    // Local miss: the full fence applies before any upstream contact. A private or
    // out-of-scope name stops here and never reaches upstream. `eligible` has no
    // side effects, so gating it behind the existence check changes nothing for a
    // name that does fall through — it just no longer pays on the warm path.
    match proxy::eligible(state, storage, pkg).await {
        Ok(true) => {}
        Ok(false) => return None,
        Err(e) => return Some(read_error(e)),
    }
    match proxy
        .ensure_artifact_cached(state, storage, pkg, filename)
        .await
    {
        Ok(()) => None,
        Err(e) => Some(read_error(e)),
    }
}

/// Serve a PEP 658 companion straight from upstream, no storage writes.
async fn proxy_metadata_passthrough(
    state: &Arc<AppState>,
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
) -> Option<Response<Body>> {
    let before = match require_settled_package_read(state, storage, pkg).await {
        Ok(claim) => claim,
        Err(error) => return Some(read_error(error)),
    };
    let proxy = match eligible_proxy(state, storage, pkg).await {
        Some(Ok(proxy)) => proxy,
        Some(Err(resp)) => return Some(resp),
        None => return None,
    };
    let bytes = proxy.fetch_metadata(state, pkg, filename).await?;
    let artifact_filename = filename.strip_suffix(METADATA_SUFFIX).unwrap_or(filename);
    let artifact_key = format!("{PACKAGES_PREFIX}{pkg}/{artifact_filename}");
    let companion_key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    match companion_passthrough_visible(state, storage, pkg, &artifact_key, &companion_key, &before)
        .await
    {
        Ok(true) => {}
        Ok(false) => return Some(not_found("artifact is fenced")),
        Err(error) => return Some(read_error(error)),
    }
    Some(
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(header::CACHE_CONTROL, ARTIFACT_CACHE_CONTROL)
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(Body::from(bytes))
            .unwrap_or_else(not_found),
    )
}

/// Serve a PEP 740 provenance companion straight from upstream, no storage
/// writes — the mirror equivalent of metadata passthrough.
async fn proxy_provenance_passthrough(
    state: &Arc<AppState>,
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
) -> Option<Response<Body>> {
    let before = match require_settled_package_read(state, storage, pkg).await {
        Ok(claim) => claim,
        Err(error) => return Some(read_error(error)),
    };
    let proxy = match eligible_proxy(state, storage, pkg).await {
        Some(Ok(proxy)) => proxy,
        Some(Err(resp)) => return Some(resp),
        None => return None,
    };
    let bytes = proxy.fetch_provenance(state, pkg, filename).await?;
    let artifact_filename = filename.strip_suffix(PROVENANCE_SUFFIX).unwrap_or(filename);
    let artifact_key = format!("{PACKAGES_PREFIX}{pkg}/{artifact_filename}");
    let companion_key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    match companion_passthrough_visible(state, storage, pkg, &artifact_key, &companion_key, &before)
        .await
    {
        Ok(true) => {}
        Ok(false) => return Some(not_found("artifact is fenced")),
        Err(error) => return Some(read_error(error)),
    }
    Some(
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, ARTIFACT_CACHE_CONTROL)
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(Body::from(bytes))
            .unwrap_or_else(not_found),
    )
}

fn found_redirect(url: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, url)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::empty())
        .unwrap_or_else(not_found)
}

pub(crate) fn moved_permanently(location: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header(header::LOCATION, location)
        .body(Body::empty())
        .unwrap_or_else(not_found)
}

pub(crate) fn unauthorized() -> Response<Body> {
    // The WWW-Authenticate header rides in via middleware.
    let mut resp = Response::new(Body::from("Unauthorized"));
    *resp.status_mut() = StatusCode::UNAUTHORIZED;
    resp
}

pub(crate) fn not_found<E: std::fmt::Debug>(err: E) -> Response<Body> {
    warn!(error=?err, "read miss");
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::NOT_FOUND;
    resp
}

/// 404 only when storage says the object does not exist; everything else is
/// an outage and must surface as 503 — telling pip "no such package" during
/// an S3 blip is the dependency-confusion direction.
pub(crate) fn read_error(err: anyhow::Error) -> Response<Body> {
    if storage::is_not_found(&err) {
        return not_found(err);
    }
    tracing::error!(error=?err, "storage error on read path");
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    resp
}

// --- Deletion + yank (PEP 592) ----------------------------------------------

/// Delete an artifact. Ordering invariant: the file leaves the index first,
/// then the artifact goes, then its sidecars — a listed-but-missing file is
/// the only harmful state, and this order never produces one.
async fn files_delete(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    // Artifacts only: .origin, sidecars, and metadata companions are managed
    // by the server, not deletable handles.
    let Some(pkg) = checked_pkg_name(&package).filter(|_| valid_artifact_filename(&filename))
    else {
        return Err((StatusCode::NOT_FOUND, "No such file".into()));
    };
    // Pin once (design §3): the existence check, index rewrite, and artifact +
    // sidecar deletes all run against this handle.
    let pinned = state.pin();
    delete_record(&state, &pinned, &pkg, &filename).await
}

/// The storage-protocol core of an artifact delete: existence check → intent
/// marker → origin checks (refuse a delete without a live claim; multi-bucket
/// refuses mirror eviction) → index rewrite dropping the file → origin re-check
/// → tombstone-before-delete for private → artifact delete → presign-cache
/// invalidation → companion/sidecar deletes (`.origin` retained) → commit marker
/// → replication fan-out → 204. Split out of [`files_delete`] so a deterministic
/// simulator can drive it without axum.
pub async fn delete_record(
    state: &Arc<AppState>,
    pinned: &buckets::Pinned,
    pkg: &str,
    filename: &str,
) -> Result<StatusCode, (StatusCode, String)> {
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let storage = pinned.storage.as_ref();
    match storage.head_exists(&key).await {
        Ok(true) => {}
        Ok(false) => return Err((StatusCode::NOT_FOUND, "No such file".into())),
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error: {e}"),
            ));
        }
    }
    // Correctness-critical in every mode: deleting a package's last file prunes
    // it from the global index, and the intent marker is the only durable signal
    // that carries that removal to the worker. Fail the delete before touching
    // truth if the marker can't be written, rather than mutate truth with no
    // breadcrumb and leave the prune to the external-change audit.
    let intent_nonce = Some(worker::mark_intent(storage, pkg).await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("failed to reserve delete: {e}"),
        )
    })?);

    // A mirror cache eviction cannot be made atomic with a concurrent
    // mirror->private package demotion: the claim and artifact are separate S3
    // objects. Manufacturing a private tombstone after any uncertain claim
    // movement would propagate a cache eviction over real private truth.
    // Refuse this unnecessary admin operation instead of pretending a
    // cross-object transaction exists. Broad lifecycle expiry is not a safe
    // substitute because private and mirror records share `packages/`.
    let origin_before = match origin::read_origin_observation(storage, pkg).await {
        Ok(Some(observed))
            if matches!(
                observed.state,
                origin::OriginState::Private | origin::OriginState::Mirror
            ) =>
        {
            observed
        }
        Ok(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("artifact '{filename}' has no live origin claim; refusing delete"),
            ));
        }
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error reading origin: {e}"),
            ));
        }
    };
    if state.buckets.is_multi() && origin_before.state == origin::OriginState::Mirror {
        let _ = commit_marker(state, storage, pkg, intent_nonce).await;
        return Err((
            StatusCode::CONFLICT,
            "Mirror cache eviction is disabled with multiple buckets".into(),
        ));
    }

    worker::rebuild_package_excluding(state, storage, pkg, Some(filename))
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("index rewrite failed: {e}"),
            )
        })?;

    if state.buckets.is_multi() {
        let current = origin::read_origin_observation(storage, pkg)
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error re-checking origin: {e}"),
                )
            })?;
        if current.as_ref() != Some(&origin_before) {
            let _ = commit_marker(state, storage, pkg, intent_nonce).await;
            return Err((
                StatusCode::CONFLICT,
                format!("Package '{pkg}' changed origin during delete"),
            ));
        }
    }

    // Tombstone a private delete BEFORE the artifact goes (dev/MULTIBUCKET.md
    // §6.4): the filename is barred from reuse, and a crash between here and the
    // artifact delete converges to "gone" (the index rebuild already drops
    // tombstoned files) instead of resurrecting it. Mirror deletes are local
    // cache management — a cached upstream file stays re-fillable forever — so
    // they are never tombstoned. A read outage fails the delete rather than risk
    // a silent-reuse gap.
    let replicate_delete = origin_before.state == origin::OriginState::Private;
    if replicate_delete {
        tombstone::write(storage, &key, filename)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("tombstone write failed: {e}"),
                )
            })?;
    }

    storage
        .delete_keys(std::slice::from_ref(&key))
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("artifact delete failed: {e}"),
            )
        })?;

    // Stop handing out the dead URL immediately (same node; peers age out).
    state.presign_cache.invalidate(&key);
    // Same for the proxy's warm-hit presence proof: without this, a re-request
    // inside PRESENCE_TTL would hit the stale "present" and serve a local 404
    // instead of re-mirroring the file from upstream (peers age out via the TTL).
    if let Some(proxy) = &state.proxy {
        proxy.invalidate_presence(&key);
    }
    // The `.origin` claim is durable on purpose: deleting every artifact must
    // not release the name for the *opposite* world to re-claim. Otherwise a
    // credentialed client could empty a mirror-owned public name and re-upload
    // it as a private package (the dependency-confusion direction). Re-purposing
    // a name from private to mirror is an operator action gated on storage
    // access — `pypiron origin release <package>` performs a checked CAS.
    let _ = storage
        .delete_keys(&[
            sidecar_key(&key),
            sidecar::metadata_key(&key),
            sidecar::provenance_key(&key),
        ])
        .await;

    // Worker confirms from truth and prunes global membership if needed.
    if let Err(e) = commit_marker(state, storage, pkg, intent_nonce).await {
        warn!(error=?e, "delete: failed to write commit marker");
    }
    // A private delete carries a tombstone. Fan it out to every healthy bucket
    // before the ack; mirror cache eviction remains local and unreplicated.
    if replicate_delete {
        replicate::fanout_sync(state, pinned, pkg, filename).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Yank a file (PEP 592). The request body, if any, is the reason.
async fn yank_set(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let reason = body.trim().to_string();
    let yanked = if reason.is_empty() {
        Yanked::Flag(true)
    } else {
        Yanked::Reason(reason)
    };
    set_yanked(&state, &headers, &package, &filename, yanked).await
}

/// Un-yank a file.
async fn yank_clear(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    set_yanked(&state, &headers, &package, &filename, Yanked::Flag(false)).await
}

/// Yank state lives in the sidecar — it is truth, so the system can heal.
async fn set_yanked(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    package: &str,
    filename: &str,
    yanked: Yanked,
) -> Result<StatusCode, (StatusCode, String)> {
    require_admin(state, headers)?;
    let Some(pkg) = checked_pkg_name(package).filter(|_| valid_artifact_filename(filename)) else {
        return Err((StatusCode::NOT_FOUND, "No such file".to_string()));
    };
    let pinned = state.pin();
    set_yank(state, &pinned, &pkg, filename, yanked).await
}

/// The storage-protocol core of a yank/unyank (PEP 592): the sidecar is truth,
/// so the flip is a bounded compare-and-set loop that bumps the yank epoch on
/// every real change, pairs an intent/commit marker so the derived index heals,
/// and fans a private flip out to every healthy bucket. Split out of
/// [`set_yanked`] so a deterministic simulator can drive it without axum.
pub async fn set_yank(
    state: &Arc<AppState>,
    pinned: &buckets::Pinned,
    pkg: &str,
    filename: &str,
    yanked: Yanked,
) -> Result<StatusCode, (StatusCode, String)> {
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let sc_key = sidecar_key(&key);
    let storage = pinned.storage.as_ref();

    let desired = yanked.normalized();
    let mut intent_nonce = if state.buckets.is_multi() {
        Some(worker::mark_intent(storage, pkg).await.map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to reserve yank: {e}"),
            )
        })?)
    } else {
        None
    };
    let mut wrote = false;
    let mut record_origin = None;
    for _ in 0..8 {
        let Some((bytes, etag)) = storage.get_with_etag(&sc_key).await.map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("sidecar read failed: {e}"),
            )
        })?
        else {
            return Err((StatusCode::NOT_FOUND, "No such file".to_string()));
        };
        let mut sc: Sidecar = serde_json::from_slice(&bytes).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("bad sidecar: {e}"),
            )
        })?;
        if sc.yanked.normalized() == desired {
            if let Some(nonce) = intent_nonce {
                let _ = worker::mark_commit(storage, pkg, &nonce).await;
            }
            return Ok(StatusCode::OK);
        }

        // Every real flip consumes the exact sidecar version it observed. Two
        // nodes yanking during a partition may produce equal epochs (the merge
        // has a deterministic tie-break), but two writers on one bucket cannot
        // silently lose an increment through a blind overwrite.
        sc.yank_epoch = sc.yank_epoch.saturating_add(1);
        sc.yanked = desired.clone();
        record_origin = sc.origin.clone();
        if intent_nonce.is_none() {
            intent_nonce = worker::mark_intent(storage, pkg).await.ok();
        }
        let out = serde_json::to_vec(&sc)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")))?;
        match storage.put_if_match(&sc_key, &etag, out).await {
            Ok(Some(_)) => {
                wrote = true;
                break;
            }
            Ok(None) => continue,
            Err(e) => {
                return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")));
            }
        }
    }
    if !wrote {
        return Err((
            StatusCode::CONFLICT,
            "sidecar changed repeatedly; retry the yank".to_string(),
        ));
    }

    if let Err(e) = commit_marker(state, storage, pkg, intent_nonce).await {
        warn!(error=?e, "yank: failed to write commit marker");
    }
    let replicate_private = if !state.buckets.is_multi() {
        false
    } else if let Some(owner) = record_origin.as_deref() {
        owner == origin::PRIVATE
    } else {
        origin::read_origin(storage, pkg)
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error reading origin for replication: {e}"),
                )
            })?
            .as_deref()
            == Some(origin::PRIVATE)
    };
    if replicate_private {
        replicate::fanout_sync(state, pinned, pkg, filename).await;
    }
    Ok(StatusCode::OK)
}

/// Set a project's PEP 792 status (admin). The body is the status doc, e.g.
/// `{"status":"quarantined","reason":"..."}`. An `active` target is a logical
/// clear, retained as an epoch-bearing event for cross-bucket convergence. This
/// is how mirror-over-HTTP `sync` relays an upstream freeze.
async fn project_status_set(
    State(state): State<Arc<AppState>>,
    Path(package): Path<String>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Authenticate before parsing the body — an unauthenticated caller must not
    // be able to probe well-formed vs malformed JSON (400 vs 401/403).
    require_admin(&state, &headers)?;
    let doc: status::ProjectStatusDoc = serde_json::from_str(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid status doc: {e}")))?;
    write_project_status(&state, &package, doc).await
}

/// Clear a project's status, reverting it to the default `active` (admin).
async fn project_status_clear(
    State(state): State<Arc<AppState>>,
    Path(package): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    write_project_status(&state, &package, status::ProjectStatusDoc::default()).await
}

/// Record a project-status event, then rebuild the index — status changes what
/// the listing renders (a quarantine serves no files). Active clears remain as
/// epoch-bearing truth so an older status on another bucket cannot resurrect.
/// The intent/commit pair keeps the derived index crash-safe.
/// Callers MUST enforce admin auth first.
async fn write_project_status(
    state: &Arc<AppState>,
    package: &str,
    doc: status::ProjectStatusDoc,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(pkg) = checked_pkg_name(package) else {
        return Err((StatusCode::NOT_FOUND, "no such package".to_string()));
    };

    let pinned = state.pin();
    let storage = pinned.storage.as_ref();
    let intent_nonce = if state.buckets.is_multi() {
        Some(worker::mark_intent(storage, &pkg).await.map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to reserve status write: {e}"),
            )
        })?)
    } else {
        worker::mark_intent(storage, &pkg).await.ok()
    };
    let status_origin = if state.buckets.is_multi() {
        let observed = origin::read_origin_observation(storage, &pkg)
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error reading origin for replication: {e}"),
                )
            })?;
        match observed.as_ref().map(|value| value.state) {
            Some(origin::OriginState::Private) => Some(status::StatusOrigin::Private),
            Some(origin::OriginState::Mirror) => Some(status::StatusOrigin::Mirror),
            _ => None,
        }
    } else {
        None
    };
    let replicate_private = status_origin == Some(status::StatusOrigin::Private);
    let result = if state.buckets.is_multi() {
        status::advance_status(storage, &pkg, &doc, status_origin)
            .await
            .map(|_| ())
    } else if doc.status.is_active() {
        status::clear_status(storage, &pkg).await
    } else {
        status::write_status(storage, &pkg, &doc).await
    };
    result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;

    if let Err(e) = commit_marker(state, storage, &pkg, intent_nonce).await {
        warn!(error=?e, "status: failed to write commit marker");
    }
    if replicate_private {
        replicate::fanout_sync(state, &pinned, &pkg, replicate::PROJECT_STATUS_MARKER).await;
    }
    Ok(StatusCode::OK)
}

/// Read the sync-cursor blob (the server-side memo a mirror-over-HTTP sync
/// reads to stay conditional). Admin-gated; an absent blob is an empty object,
/// not a 404 — a first-ever sync run is the normal case.
async fn sync_cursors_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let bytes = match state.pin().storage.get_bytes(sync::CURSORS_KEY).await {
        Ok(b) => b,
        Err(e) if storage::is_not_found(&e) => b"{}".to_vec(),
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("read: {e}"))),
    };
    Ok(([(header::CONTENT_TYPE, "application/json")], bytes))
}

/// Replace the sync-cursor blob. Admin-gated. The body must be a JSON object
/// (sync's own format); we validate that much so a malformed PUT can't poison
/// the next sync's reads, but the contents are otherwise opaque to the server.
async fn sync_cursors_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    if serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&body).is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            "cursors body must be a JSON object".into(),
        ));
    }
    state
        .pin()
        .storage
        .put_bytes(
            sync::CURSORS_KEY,
            body.into_bytes(),
            Some("application/json"),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Serve the stored advisory-snapshot zip (reader-gated), so a mirror-over-HTTP
/// `sync` can pull it from an upstream pypiron. The ETag is the zip's sha256;
/// `If-None-Match` short-circuits to 304 and HEAD sends headers only (each sync
/// poll is one of these). 404 when no snapshot has been delivered yet.
async fn advisories_feed_get(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let storage = state.pin().storage.clone();
    // The current storage etag (a 1-key LIST, no body) is the cheap currency
    // check. When it matches the loaded snapshot, serve the ETag and bytes from
    // memory — a sync poll (HEAD/304) then costs no 32 MB read and no re-hash.
    let storage_etag = match advisories::feed_storage_etag(storage.as_ref()).await {
        Ok(Some(e)) => e,
        Ok(None) => return not_found("no advisory snapshot"),
        Err(e) => return read_error(e),
    };
    let snap = state.advisory_snapshot();
    if snap.storage_etag.as_deref() == Some(storage_etag.as_str()) {
        if let (Some(sha), Some(zip)) = (&snap.zip_sha256, &snap.zip) {
            return serve_advisory_bytes(&method, &headers, &format!("\"{sha}\""), zip);
        }
    }
    // Slow path: storage moved under us (or this node hasn't loaded it) — read the
    // bytes and hash them so the ETag always matches the body served.
    let bytes = match advisories::stored_feed_bytes(storage.as_ref()).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return not_found("no advisory snapshot"),
        Err(e) => return read_error(e),
    };
    let etag = format!("\"{}\"", crate::hash::sha256_hex(&bytes));
    serve_advisory_bytes(&method, &headers, &etag, &bytes)
}

/// Build the advisory-feed response: 304 on a matching `If-None-Match`, headers
/// only for HEAD, else the full zip body. Shared by the fast (in-memory) and slow
/// (read-through) paths so both negotiate identically.
fn serve_advisory_bytes(
    method: &Method,
    headers: &HeaderMap,
    etag: &str,
    bytes: &[u8],
) -> Response<Body> {
    let revalidated = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "*" || v.contains(etag))
        .unwrap_or(false);
    let builder = Response::builder().header(header::ETAG, etag);
    let response = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        let builder = builder
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/zip")
            .header(header::CONTENT_LENGTH, bytes.len());
        // HEAD advertises the size without materializing (or sending) the body.
        if method == Method::HEAD {
            builder.body(Body::empty())
        } else {
            builder.body(Body::from(bytes.to_vec()))
        }
    };
    response.unwrap_or_else(not_found)
}

/// Accept a pushed advisory snapshot (admin) — the sync-delivery path for
/// air-gapped destinations. Authenticate before parsing (a client must not probe
/// well-formed vs malformed via the status), validate it parses, persist it
/// verbatim, then arm an immediate worker reload so blocking self-arms without a
/// restart.
async fn advisories_feed_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    // Validate before persisting — the stored copy is what every node loads, so
    // a garbage PUT must never overwrite a good snapshot. Parse off the runtime.
    let bytes = body.to_vec();
    let for_parse = bytes.clone();
    match tokio::task::spawn_blocking(move || advisories::parse_feed(&for_parse)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("not a valid advisory feed: {e}"),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "advisory parse task failed".into(),
            ))
        }
    }
    state
        .pin()
        .storage
        .put_bytes(advisories::FEED_KEY, bytes, Some("application/zip"))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;
    // Load it this worker tick regardless of the reconcile period, and wake the
    // worker now so the load is ~immediate rather than up to a tick away.
    state
        .advisory_reload_asap
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state.worker_nudge.notify_one();
    Ok(StatusCode::NO_CONTENT)
}

/// The note surfaced (JSON field + HTML banner) when no audit report has been
/// materialized yet — a feed is set but no leader sweep has run, or no snapshot
/// is loaded. Distinct from an empty-but-materialized report (a loaded feed that
/// nothing hosted matches), which renders as an empty row set with no note.
const AUDIT_ABSENT_NOTE: &str = "no advisory snapshot loaded yet";

/// The org audit as JSON (admin-gated): the ranked hosted (package, version) rows
/// a known advisory affects — ids, fixed-in, 30-day downloads, blocked flag.
/// Served byte-verbatim from the stored report (admin-only, low traffic, so no
/// cache). An unmaterialized report is an empty-rows body with a note, never a 404
/// — the endpoint always exists. An org's ranked vulnerability list is attacker
/// recon, so it rides the strongest credential.
async fn audit_json(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response<Body>, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let json_response = |bytes: Vec<u8>| {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .unwrap_or_else(not_found)
    };
    match advisories::stored_report_bytes(state.pin().storage.as_ref()).await {
        Ok(Some(bytes)) => Ok(json_response(bytes)),
        Ok(None) => {
            let empty = serde_json::json!({
                "generated_unix": 0,
                "feed_sha256": "",
                "rows": [],
                "note": AUDIT_ABSENT_NOTE,
            });
            Ok(json_response(
                serde_json::to_vec(&empty).unwrap_or_default(),
            ))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("reading audit report: {e}"),
        )),
    }
}

/// The org audit as server-rendered HTML (admin-gated): the same ranked rows as
/// `/audit.json`, rendered the house way. Reads the stored report per request (no
/// cache — admin-only, low traffic); an unmaterialized report renders an empty
/// table under a banner note, never a 404.
async fn audit_page(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response<Body> {
    if let Err((code, msg)) = require_admin(&state, &headers) {
        return (code, msg).into_response();
    }
    let report = match advisories::stored_report(state.pin().storage.as_ref()).await {
        Ok(report) => report,
        Err(e) => return read_error(e),
    };
    html_ok(web::audit_html(
        &page_context(&state, &headers),
        report.as_ref(),
        AUDIT_ABSENT_NOTE,
    ))
}

/// The locally-materialized PEP 691 index for a package, read straight from
/// storage so the on-demand proxy never shadows it. Admin-gated; a package with
/// no local index yet is an empty listing, not a 404 (so the caller treats it
/// as "nothing mirrored", not "endpoint missing").
async fn sync_local_index(
    State(state): State<Arc<AppState>>,
    Path(package): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let Some(pkg) = checked_pkg_name(&package) else {
        return Err((StatusCode::NOT_FOUND, "no such package".to_string()));
    };
    let pinned = state.pin();
    let before = require_settled_package_read(&state, pinned.storage.as_ref(), &pkg)
        .await
        .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
    if state.buckets.is_multi()
        && before
            .as_ref()
            .is_none_or(|claim| claim.state == origin::OriginState::Unclaimed)
    {
        return Ok((
            [(header::CONTENT_TYPE, "application/json")],
            br#"{"files":[]}"#.to_vec(),
        ));
    }
    let key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
    let bytes = match pinned.storage.get_bytes(&key).await {
        Ok(b) => b,
        Err(e) if storage::is_not_found(&e) => br#"{"files":[]}"#.to_vec(),
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("read: {e}"))),
    };
    if state.buckets.is_multi() {
        let after = require_settled_package_read(&state, pinned.storage.as_ref(), &pkg)
            .await
            .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
        if after != before {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("package '{pkg}' changed while reading its local index"),
            ));
        }
    }
    Ok(([(header::CONTENT_TYPE, "application/json")], bytes))
}

/// --- Helpers --------------------------------------------------------------
/// Check if the client accepts JSON response (PEP 691)
fn accepts_json(headers: &HeaderMap) -> bool {
    if let Some(accept) = headers.get(header::ACCEPT) {
        if let Ok(accept_str) = accept.to_str() {
            // Check for PEP 691 media type or generic application/json
            return accept_str.contains("application/vnd.pypi.simple.v1+json")
                || accept_str.contains("application/json");
        }
    }
    false
}

/// Treat an empty string as an unset value. An empty environment variable
/// (e.g. `PYPIRON_ADMIN_PASS=`) parses as `Some("")`, not `None` — a common
/// container/helm footgun (an unset secret, `value: ""`, `$UNSET`).
fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

/// Pair a credential's two halves, treating an empty half as unconfigured so
/// the role disables (fail closed) instead of enabling a bypassable credential.
/// Because `ct_eq("", "")` is true, an empty password half would otherwise
/// authenticate any client that sends an empty password.
fn cred_pair<'a>(user: Option<&'a str>, pass: Option<&'a str>) -> Option<(&'a str, &'a str)> {
    nonempty(user).zip(nonempty(pass))
}

/// The conventional admin username supplied when only `--admin-pass` is given.
const DEFAULT_ADMIN_USER: &str = "admin";

/// Default the admin username to `admin` when a password was given without one —
/// the password is the secret; the username need not be repeated. The default
/// applies *only* alongside a password, so the no-admin (read-only)
/// configuration keeps both halves unset and never trips the half-configured
/// startup error. A password-less username is returned unchanged, so a stray
/// `--admin-user` still fails closed.
fn resolve_admin_user(user: Option<&str>, pass: Option<&str>) -> Option<String> {
    if nonempty(pass).is_some() && nonempty(user).is_none() {
        Some(DEFAULT_ADMIN_USER.to_string())
    } else {
        user.map(str::to_string)
    }
}

/// A half-configured credential pair — exactly one of username/password set
/// (an empty value counts as unset) — can never authenticate anyone, and a
/// half-configured *read* credential silently serves every index and artifact
/// publicly. Returns the error message to fail startup with, or None if the
/// pair is whole (both set) or absent (neither set).
fn credential_pair_error(label: &str, user: Option<&str>, pass: Option<&str>) -> Option<String> {
    match (nonempty(user).is_some(), nonempty(pass).is_some()) {
        (true, false) => Some(format!(
            "{label} username is set but its password is empty/unset"
        )),
        (false, true) => Some(format!(
            "{label} password is set but its username is empty/unset"
        )),
        _ => None,
    }
}

/// A filename usable as an artifact key: no path separators, not a dotfile,
/// and not a sidecar/metadata companion. The backslash guard matters on the
/// upload, delete, and yank paths alike — keep them consistent.
fn valid_artifact_filename(filename: &str) -> bool {
    !filename.contains('/') && !filename.contains('\\') && sidecar::is_artifact(filename)
}

/// Gate the privileged routes (delete, yank, status, feed push, audit) behind the
/// admin credential, with RFC 7235/7231-correct status codes:
/// - no admin credential configured at all → 403 (the operation is disabled for
///   everyone, not an authentication challenge);
/// - credentials that validly authenticate as a lower role (reader or uploader)
///   but not admin → 403 (understood, insufficient — never re-challenge a
///   credential that already worked);
/// - no credentials, or credentials that authenticate as nobody → 401 (with the
///   `WWW-Authenticate` challenge added by middleware).
fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if state.is_admin(headers) {
        return Ok(());
    }
    if state.admin_credential().is_none() {
        return Err((
            StatusCode::FORBIDDEN,
            "This operation is disabled (no admin credential configured)".into(),
        ));
    }
    if state.authenticates_below_admin(headers) {
        return Err((StatusCode::FORBIDDEN, "Admin credential required".into()));
    }
    Err((StatusCode::UNAUTHORIZED, "Admin credential required".into()))
}

/// How long a minted install token is valid. Deliberately short: tokens are
/// single-session, basically single-use, so a leaked one is dead within
/// minutes — which is also why they need no revocation list (and no storage).
const TOKEN_TTL_SECS: i64 = 300;

/// Hold a gathered attribution value to something sane before it is signed into
/// a token: trim, drop control chars (so it can't later forge a log line), cap
/// length, and treat empty as absent. Charset is otherwise unrestricted — what
/// we gather is independent of where it is later routed.
fn clip_meta(value: Option<String>) -> Option<String> {
    let v: String = value?
        .trim()
        .chars()
        .filter(|c| !c.is_control())
        .take(256)
        .collect();
    (!v.is_empty()).then_some(v)
}

#[derive(serde::Deserialize, Default)]
struct MintRequest {
    /// Requested role; defaults to `reader`. Cannot exceed what the presented
    /// credential already grants.
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    repo: Option<String>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default)]
    user: Option<String>,
}

#[derive(serde::Serialize)]
struct MintResponse {
    token: String,
    username: &'static str,
    role: &'static str,
    expires_in: i64,
    expires_at: String,
}

/// Mint a short-lived install token. Fail-closed: token auth must be configured
/// (a signing key), and the presenting credential must already grant the
/// requested role — a token can never escalate beyond the credential that
/// minted it. On an open (public-read) server, a reader token needs no
/// credential, since reader access is already public.
async fn mint_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let key = nonempty(state.token_signing_key.as_deref()).ok_or((
        StatusCode::FORBIDDEN,
        "token minting is disabled (no --token-signing-key configured)".to_string(),
    ))?;
    // A token cannot mint tokens. Minting requires a base (username/password)
    // credential, so a leaked token can't refresh itself into a fresh full TTL
    // indefinitely — the short expiry stays meaningful, which is the whole basis
    // for carrying no revocation list (see TOKEN_TTL_SECS). token_role is Some
    // only when the presented credential is itself a valid __token__ bearer.
    if state.token_role(&headers).is_some() {
        return Err((
            StatusCode::FORBIDDEN,
            "a token cannot mint tokens; authenticate with a configured credential".to_string(),
        ));
    }
    let req: MintRequest = if body.trim().is_empty() {
        MintRequest::default()
    } else {
        serde_json::from_str(&body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")))?
    };
    let role = match req.role.as_deref() {
        None => token::Role::Reader,
        Some(r) => {
            token::Role::parse(r).ok_or((StatusCode::BAD_REQUEST, format!("unknown role: {r}")))?
        }
    };
    let granted = match role {
        token::Role::Reader => state.is_reader(&headers),
        token::Role::Uploader => state.is_uploader(&headers),
        token::Role::Admin => state.is_admin(&headers),
    };
    if !granted {
        return Err((
            StatusCode::UNAUTHORIZED,
            format!("the supplied credential does not grant {role} (cannot mint a {role} token)"),
        ));
    }

    let now = OffsetDateTime::now_utc().unix_timestamp();
    let claims = token::Claims {
        role,
        repo: clip_meta(req.repo),
        commit: clip_meta(req.commit),
        user: clip_meta(req.user),
        iat: now,
        exp: now + TOKEN_TTL_SECS,
    };
    let token = token::mint(key, &claims).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("minting token: {e}"),
        )
    })?;
    let expires_at = OffsetDateTime::from_unix_timestamp(claims.exp)
        .ok()
        .and_then(|t| t.format(&Rfc3339).ok())
        .unwrap_or_default();
    Ok(Json(MintResponse {
        token,
        username: token::TOKEN_USERNAME,
        role: role.as_str(),
        expires_in: TOKEN_TTL_SECS,
        expires_at,
    }))
}

/// Length-independent constant-time byte equality, so credential checks don't
/// leak the secret one prefix-byte at a time (CWE-208). The length may leak;
/// the bytes do not.
pub(crate) fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

fn check_basic_auth(headers: &HeaderMap, user: &str, pass: &str) -> Result<()> {
    let (u, p) = basic_credentials(headers).ok_or_else(|| anyhow!("missing basic auth"))?;
    // Gmail-style subaddressing: `ci+billing-api` authenticates as `ci`; the
    // suffix is a project attribution tag, not part of the identity.
    let base = u.split_once('+').map_or(u.as_str(), |(b, _)| b);
    // Username is not a secret; the password is — compare it in constant time.
    if (u == user || base == user) && ct_eq(&p, pass) {
        Ok(())
    } else {
        Err(anyhow!("bad credentials"))
    }
}

/// Decode the `Authorization: Basic` header into (username, password).
/// None when absent or malformed — callers decide whether that matters.
fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let auth = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let encoded = auth.strip_prefix("Basic ")?;
    let decoded = b64.decode(encoded).ok()?;
    let s = String::from_utf8(decoded).ok()?;
    let (u, p) = s.split_once(':').unwrap_or((s.as_str(), ""));
    Some((u.to_string(), p.to_string()))
}

/// Project attribution tag from the Basic-auth username: the part after `+`
/// (`ci+billing-api` → `billing-api`), or the whole username when untagged.
/// Deliberately works without any credential check — open servers still get
/// attribution from whatever username the client volunteers. The value is
/// client-supplied, so it is held to a label-safe charset and length; anything
/// else is dropped rather than escaped.
fn project_tag(headers: &HeaderMap) -> Option<String> {
    let (user, _) = basic_credentials(headers)?;
    let tag = user.split_once('+').map_or(user.as_str(), |(_, t)| t);
    let ok = !tag.is_empty()
        && tag.len() <= 64
        && tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    ok.then(|| tag.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_bucket_post_publish_mirror_check_is_storage_io_free() {
        let storage = Arc::new(storage::test_support::InMemStorage::default());
        // If the helper accidentally reads this malformed claim, parsing fails.
        storage.insert(&origin::origin_key("pkg"), b"not an origin claim".to_vec());
        let state = AppState::headless(storage.clone());
        let expected = origin::OriginObservation {
            state: origin::OriginState::Mirror,
            etag: "unused-single-bucket-etag".to_string(),
        };

        assert!(
            post_publish_mirror_claim_is_current(&state, storage.as_ref(), "pkg", &expected,)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn multi_bucket_post_publish_mirror_check_reads_the_exact_claim() {
        let first = Arc::new(storage::test_support::InMemStorage::default());
        let second = Arc::new(storage::test_support::InMemStorage::default());
        origin::claim_origin(first.as_ref(), "pkg", origin::MIRROR)
            .await
            .unwrap();
        let expected = origin::read_origin_observation(first.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        let mut state = AppState::headless(first.clone());
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: first.clone(),
                name: "first".to_string(),
            },
            BucketHandle {
                storage: second,
                name: "second".to_string(),
            },
        ]));

        assert!(
            post_publish_mirror_claim_is_current(&state, first.as_ref(), "pkg", &expected,)
                .await
                .unwrap()
        );
        assert!(
            origin::demote_observed_mirror(first.as_ref(), "pkg", &expected)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            !post_publish_mirror_claim_is_current(&state, first.as_ref(), "pkg", &expected,)
                .await
                .unwrap()
        );
    }

    #[test]
    fn probe_gating_tracks_traffic_and_unhealthy_buckets() {
        use bucket_health::{BucketSignal, HealthController, HealthPolicy};

        let first = Arc::new(storage::test_support::InMemStorage::default());
        let second = Arc::new(storage::test_support::InMemStorage::default());
        let mut state = AppState::headless(first.clone());

        // Traffic signal: cold until the request path notes a request, then warm.
        assert!(!state.recent_request_traffic());
        state.note_request();
        assert!(state.recent_request_traffic());
        // A request stamped outside the window no longer counts as recent.
        state.last_request_unix.store(
            crate::clock::unix_now_secs().saturating_sub(TRAFFIC_PROBE_WINDOW.as_secs() + 5),
            std::sync::atomic::Ordering::Relaxed,
        );
        assert!(!state.recent_request_traffic());

        // Single-bucket / no health view: nothing is ever unhealthy.
        assert!(!state.any_bucket_unhealthy_or_recovering());

        // Attach a two-bucket health view. All buckets start Unknown (eligible),
        // so probes may decay when idle.
        let health = Arc::new(
            HealthController::new(2, HealthPolicy::new(1, Duration::from_secs(60)).unwrap())
                .unwrap(),
        );
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: first,
                name: "east".to_string(),
            },
            BucketHandle {
                storage: second,
                name: "west".to_string(),
            },
        ]));
        state.bucket_health = Some(health.clone());
        assert!(!state.any_bucket_unhealthy_or_recovering());

        // One availability failure (leave-after-1) drives a bucket Unhealthy,
        // which must force full probe cadence — re-probing it is the only path
        // back to healthy, so idle gating never applies while any bucket is down.
        health.observe(1, BucketSignal::HttpStatus(503)).unwrap();
        assert!(state.any_bucket_unhealthy_or_recovering());
    }

    #[tokio::test]
    async fn multi_bucket_download_rejects_freeze_and_delete_markers() {
        let first = Arc::new(storage::test_support::InMemStorage::default());
        let second = Arc::new(storage::test_support::InMemStorage::default());
        origin::claim_origin(first.as_ref(), "pkg", origin::PRIVATE)
            .await
            .unwrap();
        let mut state = AppState::headless(first.clone());
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: first.clone(),
                name: "first".to_string(),
            },
            BucketHandle {
                storage: second,
                name: "second".to_string(),
            },
        ]));
        let key = format!("{PACKAGES_PREFIX}pkg/pkg-1.whl");
        first.insert(&key, b"bytes".to_vec());
        first.insert(&frozen_key(&key), b"{}".to_vec());
        assert!(
            !multi_bucket_file_visible(&state, first.as_ref(), "pkg", &key)
                .await
                .unwrap()
        );

        first.delete_keys(&[frozen_key(&key)]).await.unwrap();
        first.insert(&tombstone_key(&key), b"{}".to_vec());
        assert!(
            !multi_bucket_file_visible(&state, first.as_ref(), "pkg", &key)
                .await
                .unwrap()
        );
    }

    fn basic_headers(user: &str, pass: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        let v = format!("Basic {}", b64.encode(format!("{user}:{pass}")));
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(&v).unwrap());
        h
    }

    #[test]
    fn intent_grace_rejects_unsafe_or_unrepresentable_values() {
        assert!(validate_intent_grace_secs(3).is_ok());
        assert!(validate_intent_grace_secs(900).is_ok());
        assert!(validate_intent_grace_secs(0).is_err());
        assert!(validate_intent_grace_secs(2).is_err());
        assert!(validate_intent_grace_secs(i64::MAX as u64 + 1).is_err());
    }

    #[test]
    fn empty_credential_half_is_unconfigured() {
        // An empty password env var (`PYPIRON_ADMIN_PASS=`) must not enable a
        // credential: ct_eq("", "") is true, so it would accept any client.
        assert_eq!(
            cred_pair(Some("admin"), Some("secret")),
            Some(("admin", "secret"))
        );
        assert_eq!(cred_pair(Some("admin"), Some("")), None);
        assert_eq!(cred_pair(Some("admin"), None), None);
        assert_eq!(cred_pair(Some(""), Some("secret")), None);
        assert_eq!(cred_pair(None, Some("secret")), None);
        assert_eq!(cred_pair(None, None), None);
    }

    #[test]
    fn half_configured_credential_is_rejected() {
        // Exactly one half set (empty counts as unset) is a fatal misconfig.
        assert!(credential_pair_error("read", Some("reader"), None).is_some());
        assert!(credential_pair_error("read", None, Some("secret")).is_some());
        assert!(credential_pair_error("read", Some("reader"), Some("")).is_some());
        assert!(credential_pair_error("read", Some(""), Some("secret")).is_some());
        // Both halves set, or neither: accepted.
        assert!(credential_pair_error("read", Some("reader"), Some("secret")).is_none());
        assert!(credential_pair_error("read", None, None).is_none());
        assert!(credential_pair_error("read", Some(""), Some("")).is_none());
    }

    #[test]
    fn admin_username_defaults_only_with_a_password() {
        // Password given, username omitted (or empty) -> conventional default.
        assert_eq!(
            resolve_admin_user(None, Some("secret")).as_deref(),
            Some("admin")
        );
        assert_eq!(
            resolve_admin_user(Some(""), Some("secret")).as_deref(),
            Some("admin")
        );
        // An explicit username is preserved.
        assert_eq!(
            resolve_admin_user(Some("root"), Some("secret")).as_deref(),
            Some("root")
        );
        // No password -> no admin: the username is NOT defaulted, so the
        // read-only configuration keeps both halves unset and the half-configured
        // check stays quiet.
        assert_eq!(resolve_admin_user(None, None), None);
        assert_eq!(resolve_admin_user(None, Some("")), None);
        // A password-less username is left untouched so it still fails closed via
        // the half-configured check.
        assert_eq!(
            resolve_admin_user(Some("root"), None).as_deref(),
            Some("root")
        );
    }

    #[test]
    fn basic_auth_exact_match() {
        assert!(check_basic_auth(&basic_headers("ci", "tok"), "ci", "tok").is_ok());
        assert!(check_basic_auth(&basic_headers("ci", "nope"), "ci", "tok").is_err());
        assert!(check_basic_auth(&basic_headers("other", "tok"), "ci", "tok").is_err());
        assert!(check_basic_auth(&HeaderMap::new(), "ci", "tok").is_err());
    }

    #[test]
    fn basic_auth_accepts_subaddressed_username() {
        assert!(check_basic_auth(&basic_headers("ci+billing-api", "tok"), "ci", "tok").is_ok());
        // The password still has to be right.
        assert!(check_basic_auth(&basic_headers("ci+billing-api", "nope"), "ci", "tok").is_err());
        // The base has to match exactly — no prefix matching.
        assert!(check_basic_auth(&basic_headers("cif+billing-api", "tok"), "ci", "tok").is_err());
        // A configured username containing '+' still matches itself exactly.
        assert!(check_basic_auth(&basic_headers("ci+team", "tok"), "ci+team", "tok").is_ok());
    }

    #[test]
    fn project_tag_extraction() {
        assert_eq!(
            project_tag(&basic_headers("ci+billing-api", "tok")).as_deref(),
            Some("billing-api")
        );
        // Untagged username: the username itself is the attribution.
        assert_eq!(
            project_tag(&basic_headers("etl", "tok")).as_deref(),
            Some("etl")
        );
        // No credentials, empty tags, oversized or label-unsafe tags: dropped.
        assert_eq!(project_tag(&HeaderMap::new()), None);
        assert_eq!(project_tag(&basic_headers("ci+", "tok")), None);
        assert_eq!(project_tag(&basic_headers("ci+bad\"label", "tok")), None);
        assert_eq!(
            project_tag(&basic_headers(&format!("ci+{}", "x".repeat(65)), "tok")),
            None
        );
    }

    #[test]
    fn clf_line_full_and_missing() {
        // All fields present.
        assert_eq!(
            format_clf(
                "10.0.0.5",
                Some("ci"),
                "10/Oct/2000:13:55:36 +0000",
                &Method::GET,
                "/simple/flask/",
                "HTTP/1.1",
                200,
                Some(1532),
                Some("http://ref"),
                Some("uv/0.4.0"),
            ),
            "10.0.0.5 - ci [10/Oct/2000:13:55:36 +0000] \"GET /simple/flask/ HTTP/1.1\" 200 1532 \"http://ref\" \"uv/0.4.0\""
        );
        // Missing host/user/bytes/referer/ua all collapse to `-`.
        assert_eq!(
            format_clf(
                "",
                None,
                "10/Oct/2000:13:55:36 +0000",
                &Method::POST,
                "/legacy/",
                "HTTP/1.1",
                503,
                None,
                None,
                None,
            ),
            "- - - [10/Oct/2000:13:55:36 +0000] \"POST /legacy/ HTTP/1.1\" 503 - \"-\" \"-\""
        );
    }

    #[test]
    fn clf_authuser_drops_control_chars() {
        // A base64 username can decode to arbitrary UTF-8, including CR/LF/ESC.
        // Those must not survive into the line: no forged second record, no ANSI.
        let line = format_clf(
            "10.0.0.5",
            Some("evil\r\n10.0.0.6 - - [x] \"GET /forged\" 200 0 \"\" \"\"\x1b[31m"),
            "10/Oct/2000:13:55:36 +0000",
            &Method::GET,
            "/simple/flask/",
            "HTTP/1.1",
            200,
            Some(1532),
            None,
            None,
        );
        assert!(!line.contains('\n'), "no embedded newline: {line:?}");
        assert!(!line.contains('\r'), "no embedded CR: {line:?}");
        assert!(!line.contains('\x1b'), "no ANSI escape: {line:?}");
        // The non-control text is preserved on the single rendered line.
        assert_eq!(
            line,
            "10.0.0.5 - evil10.0.0.6 - - [x] \"GET /forged\" 200 0 \"\" \"\"[31m [10/Oct/2000:13:55:36 +0000] \"GET /simple/flask/ HTTP/1.1\" 200 1532 \"-\" \"-\""
        );
    }
}
