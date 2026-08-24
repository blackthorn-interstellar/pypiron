use std::{
    io::{IsTerminal, Write},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, Response, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use clap::{CommandFactory, FromArgMatches};
use time::OffsetDateTime;
use tracing::{debug, info, warn};

// Sibling modules are declared at the crate root (src/lib.rs); import them by
// name so the bare `worker::`, `storage::`, … paths throughout this file (from
// its life as the crate root) keep resolving.
use crate::{
    advisories, bucket_health, buckets, cache, config, counted_storage, counters, html, metrics,
    names, node_region, observed_storage, origin, project_cache, proxy, render, replicate, storage,
    sync, token, transparency, verify, worker,
};

use bucket_health::{HealthController, HealthPolicy};
use buckets::{BucketHandle, BucketSet, Pinned};
use names::checked_pkg_name;
use storage::Storage;

use crate::cli::{
    apply_maintenance_config, apply_nested_maintenance_config, arg_from_cli_or_env,
    merge_serve_file, run_buckets_migrate, run_create_token, run_healthcheck, run_origin_release,
    BucketsCommand, Cli, Commands, ConfigCommand, LogFormat, OriginCommand, RebuildIndexArgs,
    ServeArgs,
};

use crate::admin::{
    advisories_feed_get, advisories_feed_put, audit_json, audit_page, health, mint_token, ready,
    serve_metrics, stats_get, stats_summary_get, sync_cursors_get, sync_cursors_put,
    sync_local_index,
};
use crate::auth::{
    basic_credentials, check_basic_auth, cred_pair, credential_pair_error, nonempty, project_tag,
    resolve_admin_user, LoginThrottle,
};
use crate::pages::{downloads_page, project_page, project_version_page, projects_page, root};
use crate::publish::{
    files_delete, legacy_upload, project_status_clear, project_status_set, yank_clear, yank_set,
};
use crate::serve::{files_get, simple_pkg, simple_pkg_json, simple_root, simple_root_json};

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
    let storage = args.storage.build_for_write().await?;
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

/// Shared TTL cache of the ranked download leaderboard: `(computed_at, board)`,
/// or `None` until first populated.
type DownloadBoard = Arc<std::sync::Mutex<Option<(std::time::Instant, Vec<(String, u64)>)>>>;
/// Metric-keyed TTL cache of the `/stats/:metric` day-summaries: `metric ->
/// (computed_at, summaries)`. The global-stats twin of [`DownloadBoard`] — same
/// bounded staleness — but keyed by the request's `:metric`, so the map bounds
/// itself with a wholesale clear (in practice only `downloads` is ever recorded).
type SummaryCache = Arc<
    std::sync::Mutex<
        std::collections::HashMap<
            String,
            (
                std::time::Instant,
                Arc<std::collections::BTreeMap<String, counters::DaySummary>>,
            ),
        >,
    >,
>;
/// `(metric, package)`-keyed TTL cache of the `/stats/:metric/:package` daily
/// series: `(metric, package) -> (computed_at, day -> sub-key -> count)`. The
/// per-package twin of [`SummaryCache`] — same bounded staleness — but keyed by
/// the request's `(:metric, :package)`, a high-cardinality read-gated path, so
/// the map bounds itself with a wholesale clear and the worker drops it after
/// each counter flush (same-node read-your-writes; the TTL bounds other nodes).
type PackageStatsCache = Arc<
    std::sync::Mutex<
        std::collections::HashMap<
            (String, String),
            (
                std::time::Instant,
                Arc<std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>>>,
            ),
        >,
    >,
>;
/// Single-slot cache of the fully-rendered empty-query `/projects/` browser page:
/// `(rendered_at, generation, bytes)`, or `None` until first populated. The page
/// is a host-independent render of the whole name set — identical bytes for every
/// request — so one refcounted `Bytes` serves them all; the worker drops it when
/// the name set changes and the TTL bounds cross-node staleness.
type ProjectsPageCache = Arc<std::sync::Mutex<Option<(std::time::Instant, u64, bytes::Bytes)>>>;
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
    /// Honor `X-Forwarded-For`/`X-Real-IP` for the logged client IP. Off by
    /// default: those headers are client-settable, so trusting them ungated lets
    /// any direct caller forge its audit-logged address. Enable only behind a
    /// reverse proxy that sets them.
    pub trusted_proxy: bool,
    /// Per-address failed-login throttle; see `--login-cooldown-secs`.
    pub login_throttle: LoginThrottle,
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
    /// `_repl/` repair note instead of blocking the ack.
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
    /// Metric-keyed TTL cache of the global `/stats/:metric` day-summaries, so a
    /// repeated global-stats poll doesn't rescan the counter store every hit —
    /// the same idiom [`download_board`](Self::download_board) applies to the
    /// homepage marquee. Bounded by a wholesale clear (`:metric` is a read-gated
    /// path param); the numbers lag a counter flush anyway.
    pub summary_cache: SummaryCache,
    /// `(metric, package)`-keyed TTL cache of the `/stats/:metric/:package` daily
    /// series, so a repeatedly-polled dashboard endpoint doesn't rescan a
    /// package's 30-day counter window on every hit. The per-package twin of
    /// [`summary_cache`](Self::summary_cache); high-cardinality `:package` keys
    /// are bounded by a wholesale clear, and the worker drops it after each
    /// counter flush so a same-node reader sees its own writes (TTL bounds
    /// other nodes) — the [`project_cache`](Self::project_cache) invalidation model.
    pub package_stats_cache: PackageStatsCache,
    /// Single-slot TTL cache of the rendered empty-query `/projects/` browser
    /// page. That page re-renders the whole name set — a sort plus a multi-MB
    /// escape/concat — on every hit for bytes identical across requests; caching
    /// the finished page collapses a warm hit to a refcounted clone. Only the
    /// empty query is cached (a `?q=` search is rare and per-query keys would be
    /// unbounded); the worker drops it when the name set changes, the TTL bounds
    /// other nodes.
    pub projects_page_cache: ProjectsPageCache,
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
    /// upstream-claim decision stay on [`pin`](Self::pin) (read affinity).
    pub fn read_pin(&self) -> Arc<Pinned> {
        self.buckets.read_pin()
    }

    pub fn mutations_fenced(&self) -> bool {
        self.writes_fenced
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The eligible peer buckets a leader-authored control singleton (the advisory
    /// feed, the quarantined set) writes through to and reseeds onto, excluding
    /// the selected `primary_index`. Empty for a single-bucket node, a fenced
    /// topology, or when no peer is currently healthy — callers then write only
    /// the primary and let reseed-if-absent heal the rest. Mirrors the
    /// replicator's eligibility gate (background writes never touch a bucket known
    /// unhealthy or awaiting topology revalidation).
    pub(crate) fn singleton_replicas(
        &self,
        primary_index: usize,
    ) -> Vec<crate::layout::ReplicaTarget<'_>> {
        if !self.buckets.is_multi() || self.mutations_fenced() {
            return Vec::new();
        }
        self.buckets
            .handles()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != primary_index)
            .filter(|(index, _)| {
                self.bucket_health
                    .as_ref()
                    .is_none_or(|health| health.bucket_eligible(*index).unwrap_or(false))
            })
            .map(|(_, handle)| crate::layout::ReplicaTarget {
                storage: handle.storage.as_ref(),
                name: handle.name.as_str(),
            })
            .collect()
    }

    /// The eligible peer buckets a leader compacts and mirrors counter day-rollups
    /// onto ([`crate::counters::Counters::compact`],
    /// [`crate::counters::Counters::reseed_rollups`]), excluding the write pin at
    /// `primary_index`. Same eligibility as [`AppState::singleton_replicas`] — a
    /// leader-authored write fan-out, so it is gated on the topology write-fence —
    /// but wrapped as the counter engine's own store so it never sees pypiron's
    /// `Storage`. Empty for a single-bucket or fenced node; each bucket's segments
    /// are then simply frozen by a later pass.
    pub(crate) fn counter_rollup_peers(
        &self,
        primary_index: usize,
    ) -> Vec<Box<dyn counters::ObjectStore>> {
        if !self.buckets.is_multi() || self.mutations_fenced() {
            return Vec::new();
        }
        self.buckets
            .handles()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != primary_index)
            .filter(|(index, _)| {
                self.bucket_health
                    .as_ref()
                    .is_none_or(|health| health.bucket_eligible(*index).unwrap_or(false))
            })
            .map(|(_, handle)| {
                Box::new(PinnedCounterStore::new(handle)) as Box<dyn counters::ObjectStore>
            })
            .collect()
    }

    /// The current advisory snapshot: lock, clone the inner `Arc`, drop the
    /// guard. Cheap enough for the request path; recovers a poisoned lock.
    pub fn advisory_snapshot(&self) -> Arc<advisories::AdvisoryState> {
        advisories::AdvisoryState::read(&self.advisories)
    }

    /// Serve the cached empty-query `/projects/` page when it is warm and was
    /// built under the current selection generation; `None` on a cold slot, past
    /// the TTL, or after a bucket switch (generation mismatch), when the caller
    /// renders and [`store_projects_page`](Self::store_projects_page)s it.
    pub(crate) fn projects_page_cached(&self, generation: u64) -> Option<bytes::Bytes> {
        let guard = self
            .projects_page_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (at, gen, body) = guard.as_ref()?;
        (*gen == generation && at.elapsed() < self.index_cache.ttl()).then(|| body.clone())
    }

    /// Cache the freshly rendered empty-query `/projects/` page under the current
    /// selection generation.
    pub(crate) fn store_projects_page(&self, generation: u64, body: bytes::Bytes) {
        *self
            .projects_page_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner()) =
            Some((std::time::Instant::now(), generation, body));
    }

    /// Drop the cached `/projects/` page after the global name set changes, so a
    /// same-node reader sees the new listing at once (the TTL bounds other nodes).
    /// A hard drop, mirroring the index cache's invalidation.
    pub(crate) fn invalidate_projects_page(&self) {
        *self
            .projects_page_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Drop the per-package stats cache after a counter flush, so a same-node
    /// reader sees its own just-flushed download counts at once (the TTL bounds
    /// other nodes). Mirrors [`invalidate_projects_page`](Self::invalidate_projects_page).
    pub(crate) fn invalidate_package_stats(&self) {
        self.package_stats_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
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
            trusted_proxy: false,
            login_throttle: LoginThrottle::default(),
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
            summary_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            package_stats_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            projects_page_cache: Arc::new(std::sync::Mutex::new(None)),
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
    pub(crate) fn admin_credential(&self) -> Option<(&str, &str)> {
        cred_pair(self.admin_user.as_deref(), self.admin_pass.as_deref())
    }

    /// The configured read credential, if any (both halves required).
    pub(crate) fn read_credential(&self) -> Option<(&str, &str)> {
        cred_pair(self.read_user.as_deref(), self.read_pass.as_deref())
    }

    /// No write credential configured: every write path is disabled and the
    /// server is read-only. Unauthenticated open writes were a footgun on the
    /// default 0.0.0.0 bind, not a dev convenience.
    pub(crate) fn uploads_disabled(&self) -> bool {
        self.uploader_credential().is_none() && self.admin_credential().is_none()
    }

    /// The role granted by a valid `__token__` bearer token, if token auth is
    /// configured and the presented token verifies and is unexpired. Returns
    /// None otherwise (no key, not a token request, bad/expired token) — fail
    /// closed. The `+tag` overlay is ignored here (`__token__+commit=…` still
    /// resolves to token mode); the token itself carries its attribution.
    pub(crate) fn token_role(&self, headers: &HeaderMap) -> Option<token::Role> {
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
    pub(crate) fn is_admin(&self, headers: &HeaderMap) -> bool {
        self.admin_credential()
            .is_some_and(|(u, p)| check_basic_auth(headers, u, p).is_ok())
            || self.token_role(headers) == Some(token::Role::Admin)
    }

    /// May the request publish? Admin ⊇ uploader.
    pub(crate) fn is_uploader(&self, headers: &HeaderMap) -> bool {
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
    pub(crate) fn authenticates_below_admin(&self, headers: &HeaderMap) -> bool {
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
/// cadence — "the last few minutes."
const TRAFFIC_PROBE_WINDOW: Duration = Duration::from_secs(120);

/// Idle probe cadence: one discovery probe per bucket about this often once
/// there has been no traffic and every bucket is healthy. The first request
/// after idle may pay one bounded discovery timeout (accepted, §Health).
pub(crate) const IDLE_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// Adapts pypiron's bucket selector to the counter engine. Selection happens
/// once at the counter operation boundary; every nested I/O then uses the same
/// captured storage handle. In a multi-bucket fleet it also exposes the eligible
/// peer buckets so a query can sum the current day's live segments fleet-wide
/// (rolled-up history reads from the pin — it is replicated).
struct CounterStore {
    buckets: Arc<BucketSet>,
    health: Option<Arc<HealthController>>,
}

impl counters::ObjectStoreSelector for CounterStore {
    fn pin(&self) -> Box<dyn counters::ObjectStore> {
        let pinned = self.buckets.pin();
        let handle = self.buckets.handles().get(pinned.index);
        Box::new(PinnedCounterStore {
            storage: pinned.storage.clone(),
            bucket: counters::bucket_tag(handle.map_or("", |handle| handle.name.as_str())),
        })
    }

    /// Every configured bucket's tag, healthy or not: a finished day's rollups are
    /// replicated, so the pin holds a down bucket's variant and a read that
    /// dropped it would report a short day.
    fn bucket_tags(&self) -> Vec<String> {
        self.buckets
            .handles()
            .iter()
            .map(|handle| counters::bucket_tag(&handle.name))
            .collect()
    }

    /// The eligible peer buckets (excluding the write pin) a current-day read
    /// also sums, so an open day split by a selection change stays whole. Empty
    /// for a single bucket. Best-effort and lossy by design — a down peer's share
    /// of the open day is the declared loss — so, unlike a write fan-out, this
    /// read path does not gate on the topology write-fence.
    fn reachable_peers(&self) -> Vec<Box<dyn counters::ObjectStore>> {
        if !self.buckets.is_multi() {
            return Vec::new();
        }
        let primary = self.buckets.pin().index;
        self.buckets
            .handles()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != primary)
            .filter(|(index, _)| {
                self.health
                    .as_ref()
                    .is_none_or(|h| h.bucket_eligible(*index).unwrap_or(false))
            })
            .map(|(_, handle)| {
                Box::new(PinnedCounterStore::new(handle)) as Box<dyn counters::ObjectStore>
            })
            .collect()
    }
}

/// One bucket as the counter engine sees it: its storage handle plus the stable
/// tag the engine stamps into the rollups it authors there.
struct PinnedCounterStore {
    storage: Arc<dyn Storage>,
    bucket: String,
}

impl PinnedCounterStore {
    fn new(handle: &crate::buckets::BucketHandle) -> Self {
        Self {
            storage: handle.storage.clone(),
            bucket: counters::bucket_tag(&handle.name),
        }
    }
}

#[async_trait::async_trait]
impl counters::ObjectStore for PinnedCounterStore {
    fn bucket(&self) -> &str {
        &self.bucket
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.storage.supports_leases() {
            // Cloud: distinguishes a genuine miss (Ok(None)) from a transient
            // error (Err) — the engine must never freeze a day from a failed read.
            Ok(self.storage.get_with_etag(key).await?.map(|(b, _)| b))
        } else {
            // Disk: `get_bytes` returns a typed NotFound for a genuine miss and
            // propagates everything else. The stake is the same as the cloud
            // arm's, not smaller: `sum_segments` aborts a freeze on `Err` so a
            // day is never frozen from a partial read, and `compact_bucket`
            // DELETES the segments it summed. Collapsing a read error to
            // "absent" made that abort arm dead code here, freezing a short
            // total and destroying the segment it could not read.
            match self.storage.get_bytes(key).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(e) if crate::storage::is_not_found(&e) => Ok(None),
                Err(e) => Err(e),
            }
        }
    }
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.storage
            .put_bytes(key, bytes, Some("application/json"))
            .await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .storage
            .list_all(prefix)
            .await?
            .into_iter()
            .map(|o| o.key)
            .collect())
    }
    async fn delete(&self, keys: &[String]) -> Result<()> {
        self.storage.delete_keys(keys).await
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

/// The counter metric downloads are recorded under (see `serve.rs`), and the
/// only one [`ArtifactVerifier`] knows how to check.
const DOWNLOADS_METRIC: &str = "downloads";

/// Proves a download-counter key names an artifact that is really in storage.
///
/// A presigned redirect is local HMAC math: it hands back a URL without asking
/// storage anything, so the download it counts may name a file nobody ever
/// uploaded — and on a public-read index any unauthenticated client can mint
/// those keys as fast as it can issue GETs. The counter engine calls this once
/// per never-before-seen key at flush time, so the check costs nothing on the
/// request path. See [`counters::KeyVerifier`].
struct ArtifactVerifier {
    buckets: Arc<BucketSet>,
}

#[async_trait::async_trait]
impl counters::KeyVerifier for ArtifactVerifier {
    async fn exists(&self, metric: &str, key: &str) -> Option<bool> {
        if metric != DOWNLOADS_METRIC {
            return Some(true); // not a key this gate knows how to check
        }
        let object = format!("{PACKAGES_PREFIX}{key}");
        let handles = self.buckets.handles();
        let pinned = self.buckets.pin().index;
        // The write pin first — where an upload or a proxy fill lands — then
        // every other configured bucket, because a node with read affinity
        // counts downloads it served from its region bucket, and a fresh
        // artifact may not have replicated everywhere yet. Only a key absent
        // from *all* of them is a forgery; honest traffic hits the first check
        // and never reaches the rest.
        let mut unreachable = false;
        for index in std::iter::once(pinned).chain((0..handles.len()).filter(|i| *i != pinned)) {
            let Some(handle) = handles.get(index) else {
                continue;
            };
            match handle.storage.head_exists(&object).await {
                Ok(true) => return Some(true),
                Ok(false) => {}
                Err(e) => {
                    debug!(error=?e, %object, bucket=%handle.name, "download-counter key check failed; retried at the next flush");
                    unreachable = true;
                }
            }
        }
        // A bucket we could not reach is not evidence of forgery: report
        // "unknown" so the count survives this flush and is re-checked later.
        (!unreachable).then_some(false)
    }

    /// One HEAD per configured bucket — what a *miss* costs, which is the case
    /// an attacker picks. Declaring the worst case is what keeps the engine's
    /// per-flush bound denominated in real storage operations.
    fn ops_per_check(&self) -> usize {
        self.buckets.handles().len().max(1)
    }
}

/// Build the download-counter engine from CLI config, failing closed on a bad
/// resolution. Disabled (`--download-stats=false`) yields a no-op store.
fn build_counters(
    cli: &ServeArgs,
    buckets: Arc<BucketSet>,
    health: Option<Arc<HealthController>>,
) -> Result<counters::Counters> {
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
    let engine = counters::Counters::new(
        Box::new(CounterStore {
            buckets: buckets.clone(),
            health,
        }),
        cfg,
    );
    // Gate the flush only where a download can be counted without the bytes
    // ever being proved to exist — a redirecting delivery mode on a backend
    // that actually presigns. Streaming 404s before it counts, and a disk
    // backend never presigns (it falls through to streaming even when redirect
    // is configured), so both keep exactly the I/O they had. `supports_leases`
    // is the object-store backends' marker and stands in for "can presign"; the
    // one deployment it over-reads — a cloud bucket with no signer configured —
    // pays checks it did not strictly need, which is the harmless direction.
    let can_redirect = cli.artifact_delivery != ArtifactDelivery::Stream
        && buckets
            .handles()
            .iter()
            .any(|h| h.storage.supports_leases());
    Ok(if can_redirect {
        engine.with_verifier(Box::new(ArtifactVerifier { buckets }))
    } else {
        engine
    })
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

    // Load the operator's extra upstream trust roots (a corporate MITM CA) before
    // any outbound client is built — the advisory obtain, the proxy upstream, and
    // the worker probe all read them. Fail-closed: a bad --upstream-ca-cert bundle
    // refuses to start rather than surfacing on the first upstream fetch.
    crate::upstream_tls::init(cli.upstream_ca_cert.as_deref())?;

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
    // Built before storage so every backend call — boot-time index init
    // included — lands in `pypiron_storage_ops_total`.
    let metrics = Arc::new(metrics::Metrics::new());
    let raw_storages: Vec<Arc<dyn Storage>> = cli
        .storage
        .build_all()
        .await?
        .into_iter()
        .map(|s| {
            Arc::new(counted_storage::CountedStorage::new(s, metrics.clone())) as Arc<dyn Storage>
        })
        .collect();
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
    // Fail closed on a storage format newer than this binary writes BEFORE the
    // topology stamp is parsed: an older binary must refuse a newer tree rather
    // than front-run a topology error against a layout it cannot read. Same
    // handles, same availability classifier; runs single-bucket too (topology
    // no-ops there, the format gate does not). Fold the one-second control bound
    // in like topology, so a bucket too slow to answer is skipped as an outage
    // rather than refused — multi-bucket startup must survive that.
    let format_availability = |_: usize, error: &anyhow::Error| {
        bucket_health::classify(observed_storage::signal_for_error(error))
            == bucket_health::SignalClass::AvailabilityFailure
    };
    let format_skipped = crate::format::verify_format(buckets.handles(), |index, error| {
        crate::buckets::topology_error_is_availability(index, error, &format_availability)
    })
    .await?;
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
        // A bucket the format gate skipped is unverified for the same reason —
        // it was unreachable — and must not be selected until it re-clears the
        // gate. Mark it unhealthy through the same threshold so the worker's
        // recovery path re-gates it (verify_format on recovery) before it can be
        // picked. Idempotent with the topology loop above when a bucket is in both.
        for index in &format_skipped {
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
        // Compute the server-side-copy transport matrix from the reachable,
        // stamp-verified buckets, and log it one line per pair. A pair that can
        // copy moves replicated bytes provider-side (zero bytes through this
        // node); every other pair streams. Off the request path, at boot only.
        let mut reachable: Vec<usize> = topology
            .verified_indices
            .iter()
            .chain(&topology.stamped_indices)
            .copied()
            .collect();
        reachable.sort_unstable();
        reachable.dedup();
        let matrix = crate::buckets::build_copy_matrix(buckets.handles(), &reachable).await;
        info!(
            copyable_pairs = matrix.copyable_pairs(),
            "replication transport matrix computed"
        );
        buckets.install_copy_matrix(matrix);
    }
    // Learn this node's region once at startup (operator override, then platform
    // environment, then instance metadata) and, in a multi-bucket fleet, pin the
    // node's reads to its region bucket. Detection only labels the node; it never
    // moves a write.
    let node_region = node_region::detect(cli.node_region.as_deref()).await;
    if let (Some(health), Some(node)) = (&bucket_health, &node_region) {
        let specs = cli.storage.bucket_specs()?;
        match specs
            .iter()
            .position(|spec| node_region::matches(node, spec))
        {
            Some(region) => {
                // The gate that returns reads to a recovered region bucket runs
                // on a caught-up verdict refreshed once per worker cycle. A
                // return window no longer than one cycle can therefore mature
                // entirely inside one, on a verdict that is already stale.
                let count = buckets.handles().len();
                if let Some(floor) = worker::read_return_window_under_floor(
                    count,
                    Duration::from_secs(cli.bucket_return_healthy_secs),
                ) {
                    let (configured, floor) = (cli.bucket_return_healthy_secs, floor.as_secs());
                    warn!(
                        "read affinity: --bucket-return-healthy-secs {configured} is at or below the {floor}s a health check can take over {count} buckets, so a recovered region bucket can take reads back on a check up to one cycle old; a file it missed in the meantime is served from the write bucket instead — slower, never a 404. Raise --bucket-return-healthy-secs above {floor} to close the window."
                    );
                }
                // A read pin is only worth seeding to a bucket that startup could
                // reach AND that holds the whole corpus. A freshly-added region
                // bucket seeds a backfill sentinel under its peers'
                // `_repl/<region>/`, and a lagging one carries real repair notes
                // there; until they drain the region bucket is missing content,
                // so reads follow the write pin and the worker returns them once
                // it confirms the region caught up.
                let write_index = buckets.pin().index;
                let reachable = !topology.unreachable_indices.contains(&region);
                let converged =
                    reachable && replicate::region_owed_no_notes(buckets.handles(), region).await;
                let read_index = if converged { region } else { write_index };
                health.configure_read_affinity(region, read_index, converged)?;
                if converged {
                    buckets.seed_read_pin(region);
                    info!(
                        region = %node.region,
                        bucket = %buckets.handles()[region].name,
                        "read affinity: serving reads from region bucket"
                    );
                } else if reachable {
                    // `read_index` is the write pin here, which may itself BE the
                    // region bucket when the write home was down at boot. Reads
                    // are on it either way only as the write pin's loan, so the
                    // sentence holds in both shapes: nothing is pinned to the
                    // region bucket on its own account until the gate says so.
                    info!(
                        region = %node.region,
                        bucket = %buckets.handles()[region].name,
                        write_bucket = %buckets.handles()[buckets.pin().index].name,
                        "read affinity: region bucket still converging (peer unreachable, backfill, or repair notes outstanding); reads follow the write bucket until it catches up"
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
            // State the served scope out loud at startup: the difference between
            // "open to all of upstream" and "these N packages" is the whole
            // security posture of the proxy, and it is otherwise invisible.
            info!(
                %upstream,
                scope = %{
                    let mut scope = if mirror.include_packages.is_empty() {
                        "open (any non-private package)".to_owned()
                    } else {
                        format!("{} package(s)", mirror.include_packages.len())
                    };
                    // A denylist-only proxy is still "open", so say how many
                    // names are carved out of it — otherwise an erased or
                    // mistyped exclude set looks exactly like a correct one.
                    if !mirror.exclude_packages.is_empty() {
                        scope.push_str(&format!(", {} excluded", mirror.exclude_packages.len()));
                    }
                    scope
                },
                "proxy enabled"
            );
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

    // Counter day-rollups are replicated truth (the leader mirrors them to every
    // bucket); the current day's live tallies are per-bucket, declared loss. Each
    // flush, compaction, or query pins the selected handle once, so a switch
    // applies only between operations; the health view lets a query reach peers.
    let counters = Arc::new(build_counters(
        &cli,
        buckets.clone(),
        bucket_health.clone(),
    )?);
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
        let mut st = advisories::AdvisoryState::default();
        if let Some((floor, built_unix)) = advisories::embedded_floor() {
            let age_days = crate::clock::unix_now_secs().saturating_sub(built_unix) / 86_400;
            info!(
                projects = floor.len(),
                floor_age_days = age_days,
                "malware blocking armed from the embedded floor snapshot; the first live feed supersedes it"
            );
            st.overlay = Arc::new(floor);
        }
        (st, false)
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
        trusted_proxy: cli.trusted_proxy,
        login_throttle: LoginThrottle::new(Duration::from_secs(cli.login_cooldown_secs)),
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
        index_cache: Arc::new(cache::IndexCache::new(Duration::from_secs(
            cli.index_cache_ttl_secs,
        ))),
        project_cache: Arc::new(project_cache::ProjectCache::new(Duration::from_secs(
            cli.index_cache_ttl_secs,
        ))),
        presign_cache: Arc::new(cache::PresignCache::new(cache::PRESIGN_CACHE_TTL)),
        spool_dir: cli.spool_dir.unwrap_or_else(std::env::temp_dir),
        global_names: Arc::new(tokio::sync::Mutex::new(None)),
        inventory: Arc::new(tokio::sync::Mutex::new(worker::InventoryMap::default())),
        worker_nudge: Arc::new(tokio::sync::Notify::new()),
        empty_origin_observations: Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        metrics,
        counters,
        download_board: Arc::new(std::sync::Mutex::new(None)),
        summary_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        package_stats_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        projects_page_cache: Arc::new(std::sync::Mutex::new(None)),
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
        // /health is liveness (is the process up); /ready is readiness (can this
        // node serve reads) — a load balancer keys on /ready so the shutdown
        // drain works.
        .route("/health", get(health))
        .route("/ready", get(ready))
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
        // CSRF guard for state-changing requests, and `nosniff` on every
        // response. Both sit *after* `.merge(streaming)` so they wrap /legacy and
        // the /files DELETE too, and *inside* log_requests so a blocked cross-site
        // attempt still lands in the audit log.
        .layer(middleware::from_fn(csrf_guard))
        .layer(middleware::from_fn(security_headers))
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

    // Flip /ready to 503 first, so a load balancer pulls this node out of
    // rotation before we stop accepting — otherwise new requests race straight
    // into connection-refused during the drain. (/health stays 200: liveness
    // must not flap during a graceful drain, or k8s would restart the pod
    // mid-shutdown.) Only cloud (multi-node, LB-fronted) deployments need the
    // pause; disk is single-node, so Ctrl-C stays instant.
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
    // what keeps a restart from being a lease-TTL write outage on the successor,
    // so it runs first in the worker's exit tail and the best-effort counter
    // flush behind it carries its own smaller budget.
    if tokio::time::timeout(worker::WORKER_STOP_GRACE, worker_handle)
        .await
        .is_err()
    {
        warn!(
            "worker did not stop within {}s; exiting without lease release",
            worker::WORKER_STOP_GRACE.as_secs()
        );
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
/// request (the full access log). Either way, the operational probes (`/health`,
/// `/ready`, `/metrics`) are logged only at `debug`: load balancers and
/// Prometheus poll them constantly, so an info-level access log would drown in
/// them.
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
    let probe_endpoint = matches!(req.uri().path(), "/health" | "/ready" | "/metrics");

    if !is_read && state.mutations_fenced() {
        return simple_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "application/json",
            "no-cache",
            r#"{"error":"bucket topology mismatch; writes are fenced"}"#,
        );
    }

    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());
    // Failed-login throttle: only requests presenting credentials participate —
    // anonymous traffic can't guess a password and is never held up. Keyed by
    // the same address the audit log records (the forwarded header behind
    // `--trusted-proxy`, else the peer), falling back to the peer when the
    // forwarded value isn't an address.
    let login_ip = if req.headers().contains_key(header::AUTHORIZATION) {
        client_ip(req.headers(), peer, state.trusted_proxy)
            .parse()
            .ok()
            .or(peer)
    } else {
        None
    };
    let login_refused_secs = login_ip.and_then(|ip| state.login_throttle.blocked_secs(ip));

    // Decide up front whether this request could be logged, so the hot read path
    // does no timing or field work.
    let consider = if probe_endpoint {
        // Only ever at debug — checked here so frequent probes cost nothing
        // at the default info level.
        tracing::enabled!(target: "pypiron::access", tracing::Level::DEBUG)
    } else if state.access_log {
        true // firehose: every request
    } else {
        !is_read // default audit: mutations only
    };
    if !consider {
        return run_throttling_logins(&state, req, next, login_ip, login_refused_secs).await;
    }

    let clf = state.access_log && matches!(state.access_log_format, AccessLogFormat::Clf);

    // Captured before `next.run` consumes the request.
    let target = req.uri().to_string();
    let project = project_tag(req.headers());
    let ua = header_str(req.headers(), header::USER_AGENT);
    let host = client_ip(req.headers(), peer, state.trusted_proxy);
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
    let response = run_throttling_logins(&state, req, next, login_ip, login_refused_secs).await;
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
        let proto = format!("{version:?}");
        let line = format_clf(&AccessLogLine {
            host: &host,
            authuser: authuser.as_deref(),
            time: &time,
            method: &method,
            target: &target,
            proto: &proto,
            status: status.as_u16(),
            bytes,
            referer: referer.as_deref(),
            ua: ua.as_deref(),
        });
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
    // The operational probes only reach here when debug is enabled, so keep them
    // at debug; otherwise 5xx at warn so failures surface under a warn filter.
    if probe_endpoint {
        access_event!(debug);
    } else if status.is_server_error() {
        access_event!(warn);
    } else {
        access_event!(info);
    }
    response
}

/// Dispatch `req`, feeding the failed-login throttle. A currently-blocked
/// address is refused before its credential is evaluated, so even a correct
/// guess during the cooldown confirms nothing; a 401 coming back — a credential
/// that authenticated as nobody, never a role denial (403) — records one
/// failure. Both dispatch paths of `log_requests` funnel through here, so
/// throttled refusals still reach the access log and metrics.
async fn run_throttling_logins(
    state: &AppState,
    req: Request,
    next: Next,
    login_ip: Option<std::net::IpAddr>,
    login_refused_secs: Option<u64>,
) -> Response<Body> {
    if let Some(secs) = login_refused_secs {
        return too_many_logins(secs);
    }
    let response = next.run(req).await;
    if response.status() == StatusCode::UNAUTHORIZED {
        if let Some(ip) = login_ip {
            state.login_throttle.record_failure(ip);
        }
    }
    response
}

/// The refusal for a login-throttled address. `Retry-After` carries when trying
/// again can work, so a misconfigured CI job's operator sees a cooldown, not an
/// outage.
fn too_many_logins(retry_secs: u64) -> Response<Body> {
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(header::RETRY_AFTER, retry_secs)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from("Too many failed logins; wait out the cooldown"))
        .unwrap_or_else(not_found)
}

/// A header's value as an owned `String`, if present and valid UTF-8.
fn header_str(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// One forwarded-header value as a canonical bare IP, or `None` if it isn't one.
/// The value has to be a real address because it lands unquoted in the
/// space-delimited CLF `host` field, where a client-supplied `1.2.3.4 - x [y]`
/// would forge whole log fields; a trailing port (`1.2.3.4:5678`, `[::1]:80`)
/// that some proxies append is dropped.
fn forwarded_ip(value: &str) -> Option<String> {
    let value = value.trim();
    if let Ok(ip) = value.parse::<std::net::IpAddr>() {
        return Some(ip.to_string());
    }
    value
        .parse::<std::net::SocketAddr>()
        .ok()
        .map(|addr| addr.ip().to_string())
}

/// The client's address for logging. Only behind a reverse proxy
/// (`--trusted-proxy`) do the proxy-set `X-Forwarded-For` (leftmost) or
/// `X-Real-IP` win; otherwise those headers are ignored — they are
/// client-settable, so an ungated direct caller could forge its audit-logged
/// address — and the direct peer is used, else `-`. A header that doesn't hold
/// an IP counts as absent, so the next fallback takes over.
fn client_ip(headers: &HeaderMap, peer: Option<std::net::IpAddr>, trusted_proxy: bool) -> String {
    if trusted_proxy {
        if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            if let Some(ip) = forwarded_ip(xff.split(',').next().unwrap_or("")) {
                return ip;
            }
        }
        if let Some(real) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
            if let Some(ip) = forwarded_ip(real) {
                return ip;
            }
        }
    }
    peer.map(|ip| ip.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// The fields of one Combined Log Format line, borrowed from the request/response
/// parts the caller has already computed. Grouped so [`format_clf`] reads by name.
#[derive(Clone, Copy)]
struct AccessLogLine<'a> {
    host: &'a str,
    authuser: Option<&'a str>,
    time: &'a str,
    method: &'a Method,
    target: &'a str,
    proto: &'a str,
    status: u16,
    bytes: Option<u64>,
    referer: Option<&'a str>,
    ua: Option<&'a str>,
}

/// Render one Combined Log Format line. Pure (no clock) so it unit-tests; the
/// caller supplies the formatted timestamp. Missing fields render as `-`.
fn format_clf(line: &AccessLogLine<'_>) -> String {
    let &AccessLogLine {
        host,
        authuser,
        time,
        method,
        target,
        proto,
        status,
        bytes,
        referer,
        ua,
    } = line;
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

/// Reject state-changing requests a browser initiated cross-site. pypiron's HTTP
/// Basic credentials are ambient authority: a browser that cached them for this
/// origin (say, after an admin opened a read-gated page) re-attaches them to a
/// cross-site form POST, exactly like a cookie — the classic CSRF-with-Basic
/// vector. The command-line clients (pip/uv/twine, `pypiron sync`) send no fetch
/// metadata, so on an unsafe method the rule is: trust the browser's own
/// `Sec-Fetch-Site` (allow only `same-origin`/`none`); if the client is too old
/// to send it, fall back to matching `Origin`'s authority against our `Host`; a
/// request with neither header is a non-browser client and passes. Header-only —
/// it never touches the body, so /legacy's auth-before-body order and upload
/// streaming are preserved. Safe methods (GET/HEAD/OPTIONS) are ignored.
async fn csrf_guard(req: Request, next: Next) -> Response<Body> {
    let method = req.method();
    let unsafe_method =
        *method != Method::GET && *method != Method::HEAD && *method != Method::OPTIONS;
    if unsafe_method {
        let headers = req.headers();
        if let Some(site) = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
            // Browser fetch metadata: authoritative and unspoofable by page JS.
            if !site.eq_ignore_ascii_case("same-origin") && !site.eq_ignore_ascii_case("none") {
                return csrf_blocked();
            }
        } else if let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
            // Pre-fetch-metadata browser: compare Origin's authority to our Host.
            if !origin_matches_host(origin, headers) {
                return csrf_blocked();
            }
        }
        // Neither header: a non-browser client (pip/uv/twine, pypiron CLI). Allow.
    }
    next.run(req).await
}

/// A cross-site mutation gets a bare 403 — a browser page can't read the body and
/// a CLI client never reaches here.
fn csrf_blocked() -> Response<Body> {
    (StatusCode::FORBIDDEN, "cross-site request blocked").into_response()
}

/// True when `Origin`'s authority (host[:port]) equals our own `Host` header.
/// Keys off `Host`, never `X-Forwarded-Host`, so a proxy rewrite can't force a
/// match; a `null` or malformed Origin never matches.
fn origin_matches_host(origin: &str, headers: &HeaderMap) -> bool {
    let Some((_scheme, authority)) = origin.split_once("://") else {
        return false;
    };
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|host| host.eq_ignore_ascii_case(authority))
}

/// Defense-in-depth: `X-Content-Type-Options: nosniff` on every response so a
/// browser can't MIME-sniff a body into an executable type. Every pypiron
/// response already carries an explicit, correct Content-Type, so this only
/// forecloses future regressions.
async fn security_headers(req: Request, next: Next) -> Response<Body> {
    let mut resp = next.run(req).await;
    resp.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    resp
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
    // Attribute traffic to the client's project tag — except on auth failures
    // and login-throttled refusals, where the tag never validated against
    // anything, and except on /health and /metrics (groups 3 and 4), which
    // bypass read-auth and return 200: a spoofed project header there would
    // otherwise poison the tag set. Mirrors the note_request guard above.
    if let Some(tag) = project {
        if group != 3 && group != 4 && !matches!(status, 401 | 403 | 429) {
            state.metrics.record_project(&tag, group);
        }
    }
    resp
}

/// The site icon, carved from the logo. Static and immutable per build, so it's
/// served straight from the embedded bytes with a day-long cache and no auth —
/// browsers fetch it unprompted, before any credential is in play.
async fn favicon() -> Response<Body> {
    simple_response(
        StatusCode::OK,
        "image/x-icon",
        "public, max-age=86400",
        html::FAVICON_ICO,
    )
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

    // An errored HEAD is not an answer, and the branch below acts on "absent"
    // by overwriting the live global index with an empty one. A probe that
    // fails while the write path still works — a throttle, a 503 past
    // object_store's retries, a reset — would publish a zero-package index over
    // a real corpus, and peers would then reload that empty set as authority.
    // Refuse to boot instead; a cold start answers NotFound, not an error.
    let html_exists = storage.head_exists(&html_key).await?;
    let json_exists = storage.head_exists(&json_key).await?;

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

/// The multi-bucket coherence recheck: re-read the origin claim after serving and
/// confirm it still matches the `baseline` taken before the read. `None` means
/// the name is unchanged (proceed); `Some(response)` is the error the caller must
/// return (wrapping in `Some` where its own return type is `Option`) — either the
/// claim moved mid-serve or the reread failed. Callers gate on
/// `state.buckets.is_multi()`; single-bucket never rechecks. Load-bearing for
/// read-your-write coherence across buckets.
pub(crate) async fn recheck_settled(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    baseline: &Option<origin::OriginObservation>,
    action: &str,
) -> Option<Response<Body>> {
    match require_settled_package_read(state, storage, pkg).await {
        Ok(after) if after == *baseline => None,
        Ok(_) => Some(read_error(anyhow!(
            "package '{pkg}' changed while {action}"
        ))),
        Err(error) => Some(read_error(error)),
    }
}

/// The pin/fence state the read-through index path threads together: the
/// region-local read pin, the write home, and whether they are the same bucket
/// (`same_pin`, so a single-bucket 404 needn't read through to itself). Grouped
/// so the fenced serve functions don't thread the trio positionally.
pub(crate) struct Pins<'a> {
    pub(crate) read: &'a Pinned,
    pub(crate) write: &'a Pinned,
    pub(crate) same_pin: bool,
}

impl<'a> Pins<'a> {
    pub(crate) fn new(read: &'a Pinned, write: &'a Pinned) -> Self {
        Pins {
            read,
            write,
            same_pin: read.index == write.index,
        }
    }
}

/// A `200 application/json` response with no-store caching, or a 404 if the body
/// can't be built. Shared by the `/stats` and `/audit.json` endpoints.
/// Build a response from a status, content type, cache-control, and body, with
/// the (practically infallible) builder error mapped to a 404. The shape behind
/// the fixed-payload responders — health, metrics, favicon, the JSON APIs.
pub(crate) fn simple_response(
    status: StatusCode,
    content_type: &'static str,
    cache_control: &'static str,
    body: impl Into<Body>,
) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, cache_control)
        .body(body.into())
        .unwrap_or_else(not_found)
}

/// Whether the request's `If-None-Match` is a 304 revalidation hit: `*`, or the
/// header lists any of `etags`. The conditional-GET check shared by the index
/// and advisory responders.
pub(crate) fn if_none_match(headers: &HeaderMap, etags: &[&str]) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "*" || etags.iter().any(|e| v.contains(*e)))
        .unwrap_or(false)
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

/// Map a write-path failure to a `500` with a `"<label>: <err>"` body — the shape
/// the admin/mutation handlers repeat when a storage or encode step fails.
pub(crate) fn internal(label: &'static str, e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{label}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_ip_honors_forwarded_headers_only_behind_trusted_proxy() {
        use std::net::{IpAddr, Ipv4Addr};
        let peer = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

        let mut fwd = HeaderMap::new();
        fwd.insert(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.9, 10.0.0.1"),
        );
        // Ungated (default): client-set forwarding headers are ignored, so the
        // direct peer is logged and a caller can't forge its audit address.
        assert_eq!(client_ip(&fwd, peer, false), "127.0.0.1");
        // Behind a trusted proxy: the leftmost X-Forwarded-For entry wins.
        assert_eq!(client_ip(&fwd, peer, true), "203.0.113.9");

        // X-Real-IP is the fallback when X-Forwarded-For is absent, again only
        // when the proxy is trusted.
        let mut real = HeaderMap::new();
        real.insert("x-real-ip", HeaderValue::from_static("198.51.100.7"));
        assert_eq!(client_ip(&real, peer, false), "127.0.0.1");
        assert_eq!(client_ip(&real, peer, true), "198.51.100.7");

        // No headers and no peer → "-".
        assert_eq!(client_ip(&HeaderMap::new(), None, true), "-");
    }

    #[test]
    fn client_ip_rejects_forwarded_values_that_are_not_addresses() {
        use std::net::{IpAddr, Ipv4Addr};
        let peer = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

        // The logged host lands unquoted in a CLF line, so junk that would forge
        // extra fields is treated as absent and the peer is logged instead.
        let mut junk = HeaderMap::new();
        junk.insert(
            "x-forwarded-for",
            HeaderValue::from_static("1.2.3.4 - evil [junk] \"GET / HTTP/1.1\" 200"),
        );
        assert_eq!(client_ip(&junk, peer, true), "127.0.0.1");

        // A proxy that appends a port is still a real address; the port is dropped.
        let mut ported = HeaderMap::new();
        ported.insert("x-forwarded-for", HeaderValue::from_static("1.2.3.4:5678"));
        assert_eq!(client_ip(&ported, peer, true), "1.2.3.4");
        ported.insert("x-forwarded-for", HeaderValue::from_static("[::1]:80"));
        assert_eq!(client_ip(&ported, peer, true), "::1");

        // Junk X-Forwarded-For falls through to a valid X-Real-IP.
        junk.insert("x-real-ip", HeaderValue::from_static("198.51.100.7"));
        assert_eq!(client_ip(&junk, peer, true), "198.51.100.7");

        // Junk in both, and no peer → "-", never the attacker's string.
        junk.insert("x-real-ip", HeaderValue::from_static("not an ip"));
        assert_eq!(client_ip(&junk, None, true), "-");
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

    #[test]
    fn intent_grace_rejects_unsafe_or_unrepresentable_values() {
        assert!(validate_intent_grace_secs(3).is_ok());
        assert!(validate_intent_grace_secs(900).is_ok());
        assert!(validate_intent_grace_secs(0).is_err());
        assert!(validate_intent_grace_secs(2).is_err());
        assert!(validate_intent_grace_secs(i64::MAX as u64 + 1).is_err());
    }

    #[test]
    fn clf_line_full_and_missing() {
        // All fields present.
        assert_eq!(
            format_clf(&AccessLogLine {
                host: "10.0.0.5",
                authuser: Some("ci"),
                time: "10/Oct/2000:13:55:36 +0000",
                method: &Method::GET,
                target: "/simple/flask/",
                proto: "HTTP/1.1",
                status: 200,
                bytes: Some(1532),
                referer: Some("http://ref"),
                ua: Some("uv/0.4.0"),
            }),
            "10.0.0.5 - ci [10/Oct/2000:13:55:36 +0000] \"GET /simple/flask/ HTTP/1.1\" 200 1532 \"http://ref\" \"uv/0.4.0\""
        );
        // Missing host/user/bytes/referer/ua all collapse to `-`.
        assert_eq!(
            format_clf(&AccessLogLine {
                host: "",
                authuser: None,
                time: "10/Oct/2000:13:55:36 +0000",
                method: &Method::POST,
                target: "/legacy/",
                proto: "HTTP/1.1",
                status: 503,
                bytes: None,
                referer: None,
                ua: None,
            }),
            "- - - [10/Oct/2000:13:55:36 +0000] \"POST /legacy/ HTTP/1.1\" 503 - \"-\" \"-\""
        );
    }

    #[test]
    fn clf_authuser_drops_control_chars() {
        // A base64 username can decode to arbitrary UTF-8, including CR/LF/ESC.
        // Those must not survive into the line: no forged second record, no ANSI.
        let line = format_clf(&AccessLogLine {
            host: "10.0.0.5",
            authuser: Some("evil\r\n10.0.0.6 - - [x] \"GET /forged\" 200 0 \"\" \"\"\x1b[31m"),
            time: "10/Oct/2000:13:55:36 +0000",
            method: &Method::GET,
            target: "/simple/flask/",
            proto: "HTTP/1.1",
            status: 200,
            bytes: Some(1532),
            referer: None,
            ua: None,
        });
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
