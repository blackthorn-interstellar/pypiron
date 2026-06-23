use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Multipart, Path, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, Response, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use base64::engine::general_purpose::STANDARD as b64;
use base64::Engine;
use clap::{Args as ClapArgs, CommandFactory, FromArgMatches, Parser, Subcommand};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::{info, warn};

mod cache;
mod config;
mod coremeta;
#[cfg(test)]
mod corpus_check;
mod counters;
mod lease;
mod markdown;
mod metrics;
mod names;
mod origin;
mod provenance;
mod proxy;
mod range;
mod render;
mod sidecar;
mod simple;
mod status;
mod storage;
mod sync;
mod upload;
mod verify;
mod web;
mod wheel;
mod worker;

use names::{
    checked_pkg_name, infer_package_from_filename, infer_version_from_filename, is_normalized,
    normalize_pkg_name,
};
use sidecar::{
    metadata_key, provenance_key, sidecar_key, Sidecar, Yanked, METADATA_SUFFIX, PROVENANCE_SUFFIX,
};
use storage::{Storage, StorageArgs};

const PACKAGES_PREFIX: &str = "packages/";
const SIMPLE_PREFIX: &str = "simple/";
const DIRTY_PREFIX: &str = "_dirty/";

// Bare `pypiron` (no args) prints help (arg_required_else_help). Every verb is a
// subcommand — serving is `pypiron serve`. Only genuinely cross-cutting flags
// (`--log-format`) live at the top level; everything serve-specific is under
// `serve`, so the top-level help stays a short front door instead of dumping
// every server flag.
/// The git commit baked in at build time (see `build.rs`); `unknown` when built
/// without git (e.g. from an sdist).
const GIT_HASH: &str = env!("PYPIRON_GIT_HASH");
/// Crate version plus the commit it was built from, e.g. `0.0.0 (abc1234)`.
/// One string for `--version`, the startup banner, and the web footer.
const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("PYPIRON_GIT_HASH"),
    ")"
);

/// PypIron — a fast single-binary PyPI server: index, upload, mirror, on-demand proxy.
#[derive(Parser, Debug)]
#[command(author, version = VERSION, about, long_about = None, arg_required_else_help = true)]
struct Cli {
    /// Subcommands: `serve`, `sync`, `verify-index`, `rebuild-index`.
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to a pypiron.toml (defaults to ./pypiron.toml when present). Read by
    /// both `serve` and `sync`; CLI/env values take precedence over the file.
    /// `global` so it may sit before or after the subcommand.
    #[arg(long, env = "PYPIRON_CONFIG", global = true)]
    config: Option<std::path::PathBuf>,

    /// Log output format: `text` (human-readable) or `json` (one object per
    /// line, for log pipelines). Applies to every subcommand; `global` so it
    /// may sit before or after the subcommand.
    #[arg(
        long,
        env = "PYPIRON_LOG_FORMAT",
        value_enum,
        default_value_t = LogFormat::Text,
        global = true
    )]
    log_format: LogFormat,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the PypIron server (the default day-to-day command)
    Serve(Box<ServeArgs>),
    /// Mirror packages from PyPI (or another source) into a PypIron instance
    Sync(Box<sync::SyncArgs>),
    /// Recompute every index from truth and diff against what storage serves
    /// (read-only); exits nonzero on any divergence
    VerifyIndex(Box<verify::VerifyArgs>),
    /// Rebuild every materialized view from truth, unconditionally. Run after
    /// restoring a backup or editing storage out-of-band.
    RebuildIndex(Box<RebuildIndexArgs>),
}

#[derive(ClapArgs, Debug)]
struct RebuildIndexArgs {
    #[command(flatten)]
    storage: StorageArgs,
}

/// One-shot deep audit against a storage backend, no server attached.
async fn run_rebuild_index(args: RebuildIndexArgs) -> Result<()> {
    let storage = args.storage.build().await?;
    let state = AppState::headless(storage);
    worker::audit(&state, true).await
}

/// How artifact bytes reach clients. The tension: redirects move the
/// megabytes to S3, but a fresh presigned URL per request defeats any client
/// cache keyed by the final URL (pip's HTTP cache re-downloads every wheel),
/// while streaming keeps every cache effective at the cost of this node
/// serving the bytes. Index pages always carry stable `/files/` URLs; this
/// only governs what happens when a client GETs one.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactDelivery {
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

/// Log output format.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    /// Human-readable lines (default).
    Text,
    /// One JSON object per line, for log pipelines.
    Json,
}

fn redirect_safe_client(headers: &HeaderMap) -> bool {
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    REDIRECT_SAFE_UA_PREFIXES.iter().any(|p| ua.starts_with(p))
}

/// `PypIron` - A fast, reliable, and scalable `PyPI` server
#[derive(ClapArgs, Debug, Clone)]
struct ServeArgs {
    #[command(flatten)]
    storage: StorageArgs,

    /// Uploader credential username — may publish (ordinary uploads). With no
    /// credential of any kind configured, the server is read-only.
    #[arg(long, env = "PYPIRON_UPLOADER_USER")]
    uploader_user: Option<String>,

    /// Uploader credential password (see --uploader-user).
    #[arg(long, env = "PYPIRON_UPLOADER_PASS")]
    uploader_pass: Option<String>,

    /// Admin credential username — may do everything an uploader can, plus the
    /// privileged operations: mirror uploads (backdating + `mirror` origin),
    /// deletion, and yank. Configuring a password is what enables those
    /// operations; the username defaults to `admin` when only the password is
    /// set.
    #[arg(long, env = "PYPIRON_ADMIN_USER")]
    admin_user: Option<String>,

    /// Admin credential password (see --admin-user).
    #[arg(long, env = "PYPIRON_ADMIN_PASS")]
    admin_pass: Option<String>,

    /// Reserve this namespace for private uploads: new private packages must
    /// match `<prefix>` or `<prefix>-*` (PEP 503-normalized)
    #[arg(long, env = "PYPIRON_PRIVATE_PREFIX")]
    private_prefix: Option<String>,

    /// How artifact bytes reach clients. `stream`: proxy through this node
    /// (URL-keyed HTTP caches like pip's stay effective). `redirect`: 302 to
    /// presigned S3 URLs so this node never touches wheel bytes. `auto`
    /// (default): per-client — redirect clients whose caches are immune to
    /// presigned-URL churn (uv), stream everyone else. Disk backend always
    /// streams. See dev/DESIGN.md for the tradeoffs.
    #[arg(
        long,
        env = "PYPIRON_ARTIFACT_DELIVERY",
        value_enum,
        default_value_t = ArtifactDelivery::Auto
    )]
    artifact_delivery: ArtifactDelivery,

    /// Worker interval in seconds. The nudge path makes same-process writes
    /// visible at rebuild speed regardless; this is the marker-poll cadence
    /// for peer nodes' writes. 1s costs ~$0.45/month in S3 LISTs.
    #[arg(long, env = "PYPIRON_WORKER_INTERVAL_SECS", default_value = "1")]
    worker_interval_secs: u64,

    /// Seconds an in-flight write may hold off its package's rebuild before
    /// the worker assumes the writer crashed and rebuilds anyway. Must exceed
    /// the slowest expected upload.
    #[arg(long, env = "PYPIRON_INTENT_GRACE_SECS", default_value = "900")]
    intent_grace_secs: u64,

    /// Run an audit sweep as soon as this node becomes leader (heals a
    /// restored backup or a crashed predecessor without waiting an interval).
    #[arg(long, env = "PYPIRON_AUDIT_ON_BOOT", default_value_t = true, action = clap::ArgAction::Set)]
    audit_on_boot: bool,

    /// Seconds between audit sweeps. Day-to-day freshness rides the event
    /// markers; the audit only catches out-of-band storage changes, so daily
    /// is plenty. Fingerprint shards make an unchanged corpus cost a flat
    /// listing and nothing else.
    #[arg(long, env = "PYPIRON_RECONCILE_INTERVAL_SECS", default_value = "86400")]
    reconcile_interval_secs: u64,

    /// Leader lease TTL in seconds (multi-node S3 only; sloppy by design)
    #[arg(long, env = "PYPIRON_LEASE_TTL_SECS", default_value = "30")]
    lease_ttl_secs: u64,

    /// Count per-package/version downloads per day into the S3-backed counter
    /// store (`_counters/`). A best-effort derived analytic — lossy by design,
    /// never truth. Adds a periodic small PUT per node (see
    /// docs/reference/configuration.md).
    #[arg(long, env = "PYPIRON_DOWNLOAD_STATS", default_value_t = true, action = clap::ArgAction::Set)]
    download_stats: bool,

    /// Counter resolution (intra-day bucket width): a whole number of minutes
    /// dividing a day, e.g. `1d`, `1h`, `30m`, `2h`. Coarser is cheaper; changing
    /// it is non-destructive (old days keep their granularity).
    #[arg(long, env = "PYPIRON_COUNTERS_RESOLUTION", default_value = "1d")]
    counters_resolution: String,

    /// Seconds between counter flushes (every node). Lower = fresher and less
    /// loss on crash, at more S3 PUTs. The dominant cost knob.
    #[arg(
        long,
        env = "PYPIRON_COUNTERS_FLUSH_INTERVAL_SECS",
        default_value = "300"
    )]
    counters_flush_interval_secs: u64,

    /// Seconds between leader compaction passes (freeze finished days, prune).
    #[arg(
        long,
        env = "PYPIRON_COUNTERS_ROLLUP_INTERVAL_SECS",
        default_value = "3600"
    )]
    counters_rollup_interval_secs: u64,

    /// Days of per-day counter history to keep before deletion.
    #[arg(long, env = "PYPIRON_COUNTERS_RETENTION_DAYS", default_value = "90")]
    counters_retention_days: i64,

    /// Wait for the uploaded file to appear in the index before returning
    /// 200 (publish-then-install CI pipelines)
    #[arg(long, env = "PYPIRON_SYNC_UPLOADS")]
    sync_uploads: bool,

    /// Bound on the synchronous-upload wait, in seconds
    #[arg(long, env = "PYPIRON_SYNC_UPLOAD_TIMEOUT_SECS", default_value = "10")]
    sync_upload_timeout_secs: u64,

    /// Address to bind the server to
    #[arg(long, env = "PYPIRON_BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind_addr: String,

    /// Directory for upload spool files (defaults to the system temp dir).
    /// Point this at real disk on distros where /tmp is a RAM-backed tmpfs —
    /// otherwise large uploads spool into memory and defeat streaming.
    #[arg(long, env = "PYPIRON_SPOOL_DIR")]
    spool_dir: Option<std::path::PathBuf>,

    /// Read credential username — when set, the simple indexes and artifact
    /// downloads require basic auth (this credential, the uploader, or the
    /// admin all work). When unset, reads are public. Usernames support
    /// `+tag` subaddressing (e.g. `reader+billing-api`) for per-project
    /// traffic attribution in /metrics and the request logs.
    #[arg(long, env = "PYPIRON_READ_USER")]
    read_user: Option<String>,

    /// Read credential password (see --read-user).
    #[arg(long, env = "PYPIRON_READ_PASS")]
    read_pass: Option<String>,

    /// Serve unknown (non-private) packages on demand from this upstream
    /// simple index (e.g. https://pypi.org): package pages are answered from
    /// upstream metadata and artifacts are downloaded, verified, and cached
    /// in storage as `mirror`-origin packages on first request. Names claimed
    /// `private` (or inside --private-prefix) never fall through. Off by
    /// default.
    #[arg(long, env = "PYPIRON_PROXY_UPSTREAM")]
    proxy_upstream: Option<String>,

    /// The slice of PyPI the proxy serves and caches. The same `--filter-*`
    /// surface as `sync`, set once and shared: a `[filter]` table in
    /// pypiron.toml governs both.
    #[command(flatten)]
    filter: sync::FilterArgs,
}

/// Shared TTL cache of the ranked download leaderboard: `(computed_at, board)`,
/// or `None` until first populated.
type DownloadBoard = Arc<std::sync::Mutex<Option<(std::time::Instant, Vec<(String, u64)>)>>>;

#[derive(Clone)]
struct AppState {
    storage: Arc<dyn Storage>,
    // auth — two roles: uploader (publish) and admin (everything, incl. mirror,
    // delete, yank). Admin is a strict superset of uploader.
    uploader_user: Option<String>,
    uploader_pass: Option<String>,
    admin_user: Option<String>,
    admin_pass: Option<String>,
    // read credential — when configured, index and artifact reads require it
    // (or any stronger credential).
    read_user: Option<String>,
    read_pass: Option<String>,
    private_prefix: Option<String>,
    artifact_delivery: ArtifactDelivery,
    // worker cfg
    worker_interval: Duration,
    reconcile_interval: Duration,
    /// How long an unpaired intent marker may sit before the worker treats
    /// its writer as crashed and rebuilds anyway. time::Duration because it
    /// is compared against storage timestamps.
    intent_grace: time::Duration,
    audit_on_boot: bool,
    lease_ttl: Duration,
    sync_uploads: bool,
    sync_upload_timeout: Duration,
    /// RAM-served indexes with precomputed ETags; see cache.rs.
    index_cache: Arc<cache::IndexCache>,
    /// Reused presigned GET URLs for immutable artifacts; see cache.rs.
    presign_cache: Arc<cache::PresignCache>,
    /// Where upload spools live (must be real disk, not tmpfs).
    spool_dir: std::path::PathBuf,
    /// In-memory global-index name set + the lock serializing its writes.
    global_names: Arc<tokio::sync::Mutex<Option<worker::GlobalNames>>>,
    /// In-memory per-package inventory: the working set behind the storage view
    /// `_state/inventory.json`. The leader maintains it on every rebuild and
    /// re-baselines it each sweep; followers read the persisted view.
    inventory: Arc<tokio::sync::Mutex<worker::InventoryMap>>,
    /// Wakes the worker immediately after a write drops a dirty marker.
    worker_nudge: Arc<tokio::sync::Notify>,
    /// Hand-rolled Prometheus counters served at /metrics.
    metrics: Arc<metrics::Metrics>,
    /// Distributed S3-backed event counters (per-package/version downloads per
    /// day). Self-contained engine; see counters.rs. Disabled => a no-op.
    counters: Arc<counters::Counters>,
    /// TTL cache of the global download leaderboard (ranked, top 500). Shared by
    /// the homepage marquee and `/downloads/` so a public homepage doesn't rescan
    /// the counter store on every hit; the numbers lag a flush interval anyway.
    download_board: DownloadBoard,
    /// On-demand upstream mirroring (None unless --proxy-upstream is set).
    proxy: Option<Arc<proxy::Proxy>>,
    /// Process start, for the homepage uptime readout.
    started: std::time::Instant,
}

/// Adapts the pypiron [`Storage`] trait to the counter engine's minimal
/// [`counters::ObjectStore`]. This is the *only* coupling between the otherwise
/// self-contained `counters` module and the rest of pypiron — lifting the engine
/// into its own crate means reproducing just these four methods.
struct CounterStore(Arc<dyn Storage>);

#[async_trait::async_trait]
impl counters::ObjectStore for CounterStore {
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

#[tokio::main]
async fn main() -> Result<()> {
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
        LogFormat::Text => tracing_subscriber::fmt().with_env_filter(env_filter).init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init(),
    }

    let config_path = cli.config.clone();
    match cli.command {
        Some(Commands::Sync(args)) => sync::run_sync(*args, config_path).await,
        Some(Commands::VerifyIndex(args)) => verify::run_verify(*args).await,
        Some(Commands::RebuildIndex(args)) => run_rebuild_index(*args).await,
        Some(Commands::Serve(args)) => {
            let serve_matches = matches
                .subcommand_matches("serve")
                .expect("serve subcommand matched");
            run_serve(*args, config_path, serve_matches, cli.log_format).await
        }
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
fn build_counters(cli: &ServeArgs, storage: Arc<dyn Storage>) -> Result<counters::Counters> {
    if !cli.download_stats {
        return Ok(counters::Counters::disabled());
    }
    let resolution_secs = parse_resolution_secs(&cli.counters_resolution)?;
    let cfg = counters::Config {
        resolution_secs,
        flush_interval: Duration::from_secs(cli.counters_flush_interval_secs.max(1)),
        rollup_interval: Duration::from_secs(cli.counters_rollup_interval_secs.max(1)),
        retention_days: cli.counters_retention_days.max(1),
        ..counters::Config::default()
    }
    .checked()
    .map_err(|e| anyhow::anyhow!("--counters-resolution: {e}"))?;
    Ok(counters::Counters::new(
        Box::new(CounterStore(storage)),
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

/// Did this arg come from the command line or an env var (as opposed to sitting
/// at its clap default)? This is how `[serve]` layers *under* CLI/env without
/// dropping clap's `[default: …]` hints: the file fills a knob only when the
/// user left it untouched. Panics only on a typo'd id — every id below is a real
/// `ServeArgs`/`StorageArgs` field.
fn arg_from_cli_or_env(m: &clap::ArgMatches, id: &str) -> bool {
    matches!(
        m.value_source(id),
        Some(clap::parser::ValueSource::CommandLine) | Some(clap::parser::ValueSource::EnvVariable)
    )
}

/// Parse a `[serve]` string into a clap value-enum, naming the table key on
/// error so a bad `storage = "s4"` reads clearly.
fn serve_value_enum<T: clap::ValueEnum>(key: &str, v: &str) -> Result<T> {
    <T as clap::ValueEnum>::from_str(v, true).map_err(|e| anyhow::anyhow!("[serve].{key}: {e}"))
}

/// Fold the `[serve]` table into the parsed CLI args. Defaulted/bool/enum knobs
/// take the file value only when the CLI/env didn't set them; `Option` knobs use
/// CLI/env-or-file. Secrets (credentials, the Azure access key) are never here —
/// they stay CLI/env only.
fn merge_serve_file(
    cli: &mut ServeArgs,
    f: &config::ServeConfig,
    m: &clap::ArgMatches,
) -> Result<()> {
    macro_rules! fill {
        ($field:expr, $id:literal, $val:expr) => {
            if !arg_from_cli_or_env(m, $id) {
                if let Some(v) = $val {
                    $field = v;
                }
            }
        };
    }

    // Server knobs (defaulted scalars / bools / enums).
    fill!(cli.bind_addr, "bind_addr", f.bind_addr.clone());
    if !arg_from_cli_or_env(m, "artifact_delivery") {
        if let Some(v) = &f.artifact_delivery {
            cli.artifact_delivery = serve_value_enum("artifact-delivery", v)?;
        }
    }
    fill!(cli.sync_uploads, "sync_uploads", f.sync_uploads);
    fill!(
        cli.sync_upload_timeout_secs,
        "sync_upload_timeout_secs",
        f.sync_upload_timeout_secs
    );
    fill!(
        cli.worker_interval_secs,
        "worker_interval_secs",
        f.worker_interval_secs
    );
    fill!(
        cli.intent_grace_secs,
        "intent_grace_secs",
        f.intent_grace_secs
    );
    fill!(cli.audit_on_boot, "audit_on_boot", f.audit_on_boot);
    fill!(
        cli.reconcile_interval_secs,
        "reconcile_interval_secs",
        f.reconcile_interval_secs
    );
    fill!(cli.lease_ttl_secs, "lease_ttl_secs", f.lease_ttl_secs);
    fill!(cli.download_stats, "download_stats", f.download_stats);
    fill!(
        cli.counters_resolution,
        "counters_resolution",
        f.counters_resolution.clone()
    );
    fill!(
        cli.counters_flush_interval_secs,
        "counters_flush_interval_secs",
        f.counters_flush_interval_secs
    );
    fill!(
        cli.counters_rollup_interval_secs,
        "counters_rollup_interval_secs",
        f.counters_rollup_interval_secs
    );
    fill!(
        cli.counters_retention_days,
        "counters_retention_days",
        f.counters_retention_days
    );

    // Storage backend selection (defaulted/bool knobs).
    if !arg_from_cli_or_env(m, "storage") {
        if let Some(v) = &f.storage {
            cli.storage.storage = serve_value_enum("storage", v)?;
        }
    }
    fill!(
        cli.storage.s3_force_path_style,
        "s3_force_path_style",
        f.s3_force_path_style
    );
    fill!(
        cli.storage.azure_use_emulator,
        "azure_use_emulator",
        f.azure_use_emulator
    );

    // Option knobs: CLI/env wins when present, else the file.
    cli.proxy_upstream = cli.proxy_upstream.take().or(f.proxy_upstream.clone());
    cli.spool_dir = cli.spool_dir.take().or(f.spool_dir.clone());
    cli.storage.data_dir = cli.storage.data_dir.take().or(f.data_dir.clone());
    cli.storage.s3_bucket = cli.storage.s3_bucket.take().or(f.s3_bucket.clone());
    cli.storage.aws_region = cli.storage.aws_region.take().or(f.aws_region.clone());
    cli.storage.s3_endpoint_url = cli
        .storage
        .s3_endpoint_url
        .take()
        .or(f.s3_endpoint_url.clone());
    cli.storage.gcs_bucket = cli.storage.gcs_bucket.take().or(f.gcs_bucket.clone());
    cli.storage.gcs_service_account_path = cli
        .storage
        .gcs_service_account_path
        .take()
        .or(f.gcs_service_account_path.clone());
    cli.storage.gcs_endpoint_url = cli
        .storage
        .gcs_endpoint_url
        .take()
        .or(f.gcs_endpoint_url.clone());
    cli.storage.azure_account = cli.storage.azure_account.take().or(f.azure_account.clone());
    cli.storage.azure_container = cli
        .storage
        .azure_container
        .take()
        .or(f.azure_container.clone());
    cli.storage.azure_endpoint_url = cli
        .storage
        .azure_endpoint_url
        .take()
        .or(f.azure_endpoint_url.clone());

    Ok(())
}

async fn run_serve(
    mut cli: ServeArgs,
    config_path: Option<std::path::PathBuf>,
    serve_matches: &clap::ArgMatches,
    log_format: LogFormat,
) -> Result<()> {
    // Layer pypiron.toml under CLI/env before anything reads the config: the
    // `[serve]` table fills in any knob the CLI/env left at its default, and the
    // top-level `private-prefix` + shared `[filter]` reach the server here. The
    // filter itself is resolved through sync's one shared path, so the proxy and
    // a sync run can never drift.
    let file = config::load(config_path.as_deref())?;
    merge_serve_file(&mut cli, &file.serve, serve_matches)?;
    cli.private_prefix = cli.private_prefix.take().or(file.private_prefix.clone());

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

    let storage_desc = cli.storage.describe();
    let storage = cli.storage.build().await?;
    let proxy = match cli.proxy_upstream.as_deref() {
        Some(upstream) => {
            let filter = cli.filter.resolve(Some(&file.filter))?;
            Some(Arc::new(proxy::Proxy::new(upstream, filter)?))
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

    let counters = Arc::new(build_counters(&cli, storage.clone())?);
    if counters.enabled() {
        info!(
            resolution = %cli.counters_resolution,
            flush_secs = cli.counters_flush_interval_secs,
            retention_days = cli.counters_retention_days,
            "download counters enabled (_counters/)"
        );
    }

    let state = Arc::new(AppState {
        storage,
        uploader_user: cli.uploader_user,
        uploader_pass: cli.uploader_pass,
        admin_user: cli.admin_user,
        admin_pass: cli.admin_pass,
        read_user: cli.read_user,
        read_pass: cli.read_pass,
        private_prefix,
        artifact_delivery: cli.artifact_delivery,
        worker_interval: Duration::from_secs(cli.worker_interval_secs),
        reconcile_interval: Duration::from_secs(cli.reconcile_interval_secs),
        intent_grace: time::Duration::seconds(cli.intent_grace_secs as i64),
        audit_on_boot: cli.audit_on_boot,
        lease_ttl: Duration::from_secs(cli.lease_ttl_secs),
        sync_uploads: cli.sync_uploads,
        sync_upload_timeout: Duration::from_secs(cli.sync_upload_timeout_secs),
        index_cache: Arc::new(cache::IndexCache::new(cache::INDEX_CACHE_TTL)),
        presign_cache: Arc::new(cache::PresignCache::new(cache::PRESIGN_CACHE_TTL)),
        spool_dir: cli.spool_dir.unwrap_or_else(std::env::temp_dir),
        global_names: Arc::new(tokio::sync::Mutex::new(None)),
        inventory: Arc::new(tokio::sync::Mutex::new(worker::InventoryMap::default())),
        worker_nudge: Arc::new(tokio::sync::Notify::new()),
        metrics: Arc::new(metrics::Metrics::new()),
        counters,
        download_board: Arc::new(std::sync::Mutex::new(None)),
        proxy,
        started: std::time::Instant::now(),
    });

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

    // Initialize empty index files if they don't exist
    initialize_indexes(&state).await?;

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
        // Legacy PyPI upload API (used by uv/twine)
        .route("/legacy", post(legacy_upload))
        .route("/legacy/", post(legacy_upload))
        .route("/simple", get(simple_root))
        .route("/simple/", get(simple_root))
        .route("/simple/index.json", get(simple_root_json))
        .route("/simple/:package", get(simple_pkg))
        .route("/simple/:package/", get(simple_pkg))
        .route("/simple/:package/index.json", get(simple_pkg_json))
        .route(
            "/files/:package/:filename",
            get(files_get).delete(files_delete),
        )
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
        // Operational endpoints: deliberately outside read auth — load
        // balancers and Prometheus scrapers don't carry package credentials.
        .route("/health", get(health))
        .route("/metrics", get(serve_metrics))
        // Catch-all for debugging unmatched routes
        .fallback(fallback_handler)
        .with_state(state.clone())
        // Axum's default 2 MB body limit would reject any real wheel.
        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024))
        .layer(middleware::from_fn(log_requests))
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
        axum::serve(listener, app)
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

/// Middleware to log all incoming requests
/// Per-request logging is `debug`, not `info`: at tens of thousands of rps
/// the access log otherwise becomes the workload (a 30 s benchmark filled a
/// 924 MB tmpfs with INFO lines and wedged the box). `RUST_LOG=debug` turns
/// it back on for troubleshooting.
async fn log_requests(req: Request, next: Next) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let project = project_tag(req.headers());
    tracing::debug!(
        project = project.as_deref(),
        "Incoming request: {} {}",
        method,
        uri
    );
    let response = next.run(req).await;
    tracing::debug!(
        project = project.as_deref(),
        "Response status: {} {} {}",
        response.status(),
        method,
        uri
    );
    response
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
    let project = project_tag(req.headers());
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

/// The root landing page: a self-contained HTML front door with copy-paste
/// client config. Public (no secrets) — like `/health`, it carries no auth.
/// The live activity panel (traffic counters, project-tag names) is folded in
/// only for an authorized reader, so a public deployment never leaks stats: it
/// surfaces the same data as `/metrics`, but legibly, and only to operators who
/// can already read. When reads are public (no read credential), everyone sees
/// it — consistent with that deployment's open posture.
async fn root(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response<Body> {
    let ctx = page_context(&state, &headers);
    // Registry inventory (counts only) is public — shown under the header.
    let inventory = state.metrics.inventory();
    if !state.is_reader(&headers) {
        return html_ok(web::landing_html(&ctx, inventory.as_ref(), None));
    }
    let snap = state.metrics.snapshot();
    let (cache_hits, cache_misses) = state.index_cache.stats();
    let board = download_leaderboard(&state).await;
    let dash = web::DashboardData {
        snapshot: &snap,
        cache_hits,
        cache_misses,
        top_downloads: &board,
    };
    html_ok(web::landing_html(&ctx, inventory.as_ref(), Some(&dash)))
}

/// Optional `?q=` search term for the package browser.
#[derive(serde::Deserialize)]
struct BrowseQuery {
    q: Option<String>,
}

/// The human package browser (`/projects/`), which doubles as the search results
/// page: every hosted package (or those matching `?q=`), linked to its project
/// page. Read-only and gated by read auth like the activity panel, so a `?q=`
/// search can never enumerate private names on a credentialed deployment.
async fn projects_page(
    State(state): State<Arc<AppState>>,
    Query(browse): Query<BrowseQuery>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let names = match worker::global_package_names(&state).await {
        Ok(names) => names,
        Err(e) => return read_error(e),
    };
    html_ok(web::projects_html(
        &page_context(&state, &headers),
        &names,
        browse.q.as_deref().unwrap_or(""),
    ))
}

/// The download leaderboard (`/downloads/`): the most-downloaded packages over
/// the last 30 days, busiest first (top 500). Read-only and gated by read auth
/// like the dashboard, so it never enumerates private names on a credentialed
/// deployment. Served from the same TTL-cached board as the homepage marquee.
async fn downloads_page(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let board = download_leaderboard(&state).await;
    html_ok(web::downloads_html(&page_context(&state, &headers), &board))
}

/// The human project page (`/project/<pkg>/`): the latest version, tabbed into
/// description / release history / download files. Read-only and gated by read
/// auth like the dashboard. Rendered on demand — no materialized view.
async fn project_page(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    render_project(&state, &headers, &raw, None).await
}

/// The per-version page (`/project/<pkg>/<version>/`): the same page focused on
/// one release, with a version-pinned install snippet.
async fn project_version_page(
    State(state): State<Arc<AppState>>,
    Path((raw, version)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    render_project(&state, &headers, &raw, Some(&version)).await
}

/// Shared project-page renderer. `requested_version` is `None` for the latest
/// view; when present it's validated against the hosted versions (so an
/// arbitrary path segment is never reflected) and pins the install snippet.
async fn render_project(
    state: &AppState,
    headers: &HeaderMap,
    raw: &str,
    requested_version: Option<&str>,
) -> Response<Body> {
    if !state.is_reader(headers) {
        return unauthorized();
    }
    let Some(pkg) = checked_pkg_name(raw) else {
        return not_found("invalid package name");
    };
    // Canonical URL is the normalized package name; everything else 301s there.
    // The version segment is percent-encoded so a hostile, not-yet-validated
    // value (e.g. `..%2f..%2fsimple`, which axum has already decoded) can't cross
    // a path boundary in the `Location` header — it lands back here and 404s.
    if raw != pkg {
        let dest = match requested_version {
            Some(v) => format!(
                "/project/{pkg}/{}/",
                percent_encoding::utf8_percent_encode(v, PATH_SEGMENT)
            ),
            None => format!("/project/{pkg}/"),
        };
        return moved_permanently(&dest);
    }
    let files = match worker::list_artifacts(state, &pkg).await {
        Ok((files, _raw)) => files,
        Err(e) => return read_error(e),
    };
    if files.is_empty() {
        return not_found("no such project");
    }

    // Pick the version to display: the requested one (must be hosted), else the
    // latest by PEP 440 order.
    let mut versions: Vec<String> = files
        .iter()
        .filter_map(web::file_version)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    versions.sort_by(|a, b| names::version_cmp_desc(a, b));
    let (selected, pinned) = match requested_version {
        Some(v) => {
            if !versions.iter().any(|known| known == v) {
                return not_found("no such version");
            }
            (v.to_string(), true)
        }
        // The bare page headlines the newest *stable*, not-fully-yanked release
        // (pypi.org behavior), falling back to the newest overall, then to empty
        // for legacy artifacts with no derivable version (still rendered).
        None => (default_display_version(&files, &versions), false),
    };

    // Representative files of the selected version — the newest carrying each
    // companion. Metadata and provenance may ride on different artifacts.
    let meta_rep = representative(&files, &selected, |f| f.core_metadata);
    let meta = match meta_rep {
        Some(f) => load_core_metadata(state, &pkg, f).await,
        None => None,
    };
    let publisher = match representative(&files, &selected, |f| f.provenance) {
        Some(f) => load_provenance(state, &pkg, f).await,
        None => None,
    };

    let downloads = download_summary(state, &pkg).await;

    html_ok(web::project_html(
        &page_context(state, headers),
        &pkg,
        &files,
        &selected,
        pinned,
        meta.as_ref(),
        publisher.as_ref(),
        &downloads,
    ))
}

/// Last-30-day download counts for a package, filenames rolled up to versions
/// and sorted busiest first — the data behind the project page's Downloads card.
/// Empty (no traffic, or counters disabled) renders nothing.
async fn download_summary(state: &AppState, pkg: &str) -> Vec<(String, u64)> {
    let to = OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    let series = state
        .counters
        .query_package("downloads", pkg, from, to)
        .await;
    let mut by_ver: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for files in series.values() {
        for (filename, count) in files {
            let ver = infer_version_from_filename(filename).unwrap_or_else(|| "unknown".into());
            *by_ver.entry(ver).or_insert(0) += count;
        }
    }
    let mut out: Vec<(String, u64)> = by_ver.into_iter().collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// How long the cached download leaderboard stays warm. Downloads already lag a
/// flush interval (300 s default), so a minute of staleness is invisible — but
/// it spares a public, S3-backed homepage a counter-store rescan on every hit.
const DOWNLOAD_BOARD_TTL: Duration = Duration::from_secs(60);

/// Rank packages by total downloads from the per-day counter summaries, busiest
/// first. Each summary's top keys are `<pkg>/<filename>`; we roll them up to the
/// package. Approximate at the tail (a day keeps only its top keys), which is
/// fine for a leaderboard glance. Shared by the global `/stats` JSON and the
/// human leaderboard so both rank identically.
fn rank_packages(
    summaries: &std::collections::BTreeMap<String, counters::DaySummary>,
) -> Vec<(String, u64)> {
    let mut by_pkg: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for s in summaries.values() {
        for (k, v) in &s.top {
            let pkg = k.split('/').next().unwrap_or(k);
            *by_pkg.entry(pkg.to_string()).or_insert(0) += v;
        }
    }
    let mut ranked: Vec<(String, u64)> = by_pkg.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked
}

/// The most-downloaded packages over the last 30 days, ranked busiest first and
/// capped at 500. The uncached compute behind [`download_leaderboard`].
async fn compute_download_board(state: &AppState) -> Vec<(String, u64)> {
    let to = OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    let summaries = state.counters.query_summaries("downloads", from, to).await;
    let mut ranked = rank_packages(&summaries);
    ranked.truncate(500);
    ranked
}

/// The download leaderboard, served from a short TTL cache so a public homepage
/// (where every viewer sees the activity panel) doesn't rescan the counter store
/// on every request. Returns up to the top 500 packages; callers slice as needed.
async fn download_leaderboard(state: &AppState) -> Vec<(String, u64)> {
    {
        let guard = state
            .download_board
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((at, board)) = guard.as_ref() {
            if at.elapsed() < DOWNLOAD_BOARD_TTL {
                return board.clone();
            }
        }
    }
    let board = compute_download_board(state).await;
    let mut guard = state
        .download_board
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *guard = Some((std::time::Instant::now(), board.clone()));
    board
}

/// Characters refused raw in a redirect path segment — anything that could
/// cross a path boundary or restructure the URL. `versions` come in
/// already-percent-decoded by axum, so a `/` or control byte here is hostile.
const PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
    .add(b'/')
    .add(b'%')
    .add(b'?')
    .add(b'#')
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'|');

/// The version to headline on the bare project page: the newest stable release
/// (not a pre-release/dev) that isn't fully yanked, like pypi.org — falling back
/// to the newest overall, then to empty (legacy artifacts with no version).
fn default_display_version(files: &[render::FileMetadata], versions: &[String]) -> String {
    versions
        .iter()
        .find(|v| !is_prerelease(v) && !fully_yanked(files, v))
        .or_else(|| versions.first())
        .cloned()
        .unwrap_or_default()
}

/// Whether a version string parses as a PEP 440 pre-release or dev release.
fn is_prerelease(v: &str) -> bool {
    v.parse::<pep440_rs::Version>()
        .map(|ver| ver.any_prerelease())
        .unwrap_or(false)
}

/// Whether every file of `version` is yanked (so the release shouldn't headline).
fn fully_yanked(files: &[render::FileMetadata], version: &str) -> bool {
    let mut vers = files
        .iter()
        .filter(|f| web::file_version(f).as_deref() == Some(version))
        .peekable();
    vers.peek().is_some() && vers.all(|f| !matches!(f.yanked, Yanked::Flag(false)))
}

/// The newest-uploaded file of `version` for which `want` holds — used to pick
/// the artifact whose `.metadata` / `.provenance` companion represents a release.
fn representative<'a>(
    files: &'a [render::FileMetadata],
    version: &str,
    want: impl Fn(&render::FileMetadata) -> bool,
) -> Option<&'a render::FileMetadata> {
    files
        .iter()
        .filter(|f| want(f) && web::file_version(f).as_deref() == Some(version))
        .max_by(|a, b| a.upload_time.cmp(&b.upload_time))
}

/// Parse a representative file's `.metadata` companion. Best-effort: any miss
/// returns `None` and the page renders without a sidebar.
async fn load_core_metadata(
    state: &AppState,
    pkg: &str,
    rep: &render::FileMetadata,
) -> Option<coremeta::CoreMetadata> {
    let key = sidecar::metadata_key(&format!("{PACKAGES_PREFIX}{pkg}/{}", rep.filename));
    let bytes = state.storage.get_bytes(&key).await.ok()?;
    Some(coremeta::parse(&bytes))
}

/// Parse a representative file's relayed `.provenance` companion into its
/// publisher. Best-effort: a miss or malformed bundle returns `None` and the
/// page renders without a "Verified details" section.
async fn load_provenance(
    state: &AppState,
    pkg: &str,
    rep: &render::FileMetadata,
) -> Option<provenance::Publisher> {
    let key = sidecar::provenance_key(&format!("{PACKAGES_PREFIX}{pkg}/{}", rep.filename));
    let bytes = state.storage.get_bytes(&key).await.ok()?;
    provenance::parse_publisher(&bytes)
}

/// Build the request-derived context both pages share. The base URL honors a
/// reverse proxy's `X-Forwarded-Proto`/`-Host`, falling back to the `Host`
/// header; the host is restricted to a plausible charset (it lands in the page
/// as escaped text, but we keep it tidy too).
fn page_context(state: &AppState, headers: &HeaderMap) -> web::PageContext {
    web::PageContext {
        base_url: base_url_from_headers(headers),
        version: VERSION,
        proxy_enabled: state.proxy.is_some(),
        delivery: match state.artifact_delivery {
            ArtifactDelivery::Auto => "auto",
            ArtifactDelivery::Redirect => "redirect",
            ArtifactDelivery::Stream => "stream",
        },
        reads_authenticated: state.read_credential().is_some(),
        uptime_secs: state.started.elapsed().as_secs(),
    }
}

fn base_url_from_headers(headers: &HeaderMap) -> String {
    let first = |v: &HeaderValue| -> Option<String> {
        v.to_str()
            .ok()
            .and_then(|s| s.split(',').next())
            .map(|s| s.trim().to_string())
    };
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(first)
        .filter(|s| s == "http" || s == "https")
        .unwrap_or_else(|| "http".to_string());
    let host = headers
        .get("x-forwarded-host")
        .and_then(first)
        .or_else(|| headers.get(header::HOST).and_then(first))
        .filter(|h| is_plausible_host(h))
        .unwrap_or_else(|| "localhost:8080".to_string());
    format!("{proto}://{host}")
}

/// A host:port we're willing to echo into the page verbatim — letters, digits,
/// and the few punctuation marks a real authority uses. Anything else (spaces,
/// control bytes, quotes) falls back to the default.
fn is_plausible_host(h: &str) -> bool {
    !h.is_empty()
        && h.len() <= 255
        && h.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']'))
}

fn html_ok(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(body))
        .unwrap_or_else(not_found)
}

/// Liveness + storage reachability. A storage error is the only failure mode:
/// `Ok(false)` (probe object missing) still proves storage answers.
async fn health(State(state): State<Arc<AppState>>) -> Response<Body> {
    let probe = format!("{SIMPLE_PREFIX}index.json");
    let (status, body) = match state.storage.head_exists(&probe).await {
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
    let html_key = format!("{SIMPLE_PREFIX}index.html");
    let json_key = format!("{SIMPLE_PREFIX}index.json");

    // Check if global indexes exist
    let html_exists = state.storage.head_exists(&html_key).await.unwrap_or(false);
    let json_exists = state.storage.head_exists(&json_key).await.unwrap_or(false);

    if !html_exists || !json_exists {
        info!("Initializing empty global indexes");
        let empty_packages: Vec<String> = Vec::new();
        let html = render::pep503_global_html(&empty_packages);
        let json = render::pep691_global_json(&empty_packages);

        if !html_exists {
            state
                .storage
                .put_bytes(
                    &html_key,
                    html.into_bytes(),
                    Some("text/html; charset=utf-8"),
                )
                .await?;
        }

        if !json_exists {
            state
                .storage
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

    let version = fields
        .get("version")
        .cloned()
        .or_else(|| infer_version_from_filename(&filename))
        .unwrap_or_default();

    let key = format!("{PACKAGES_PREFIX}{pkg_norm}/{filename}");

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

    // Origin exclusivity: each package belongs to exactly one world. A
    // mismatch is a hard error, never a merge — the dependency-confusion
    // defense. Storage errors are outages (503), never "unclaimed".
    let desired_origin = if is_mirror {
        origin::MIRROR
    } else {
        origin::PRIVATE
    };
    let claimed_origin = origin::read_origin(state.storage.as_ref(), &pkg_norm)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error reading origin: {e}"),
            )
        })?;
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
    match claimed_origin.as_deref() {
        Some(owner) if owner == desired_origin => {}
        Some(owner) => {
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "Package '{pkg_norm}' is {owner}-owned; {desired_origin} uploads are rejected"
                ),
            ));
        }
        None => {
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
            let (_created, winner) =
                origin::claim_origin(state.storage.as_ref(), &pkg_norm, desired_origin)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Failed to claim origin: {e}"),
                        )
                    })?;
            if winner != desired_origin {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("Package '{pkg_norm}' is {winner}-owned; {desired_origin} uploads are rejected"),
                ));
            }
        }
    }

    // Intent marker before any truth write: if this request dies anywhere
    // below, the stale intent guarantees a rebuild without any sweep.
    // Best-effort — the commit marker after the writes is the primary signal.
    let intent_nonce = match worker::mark_intent(state.storage.as_ref(), &pkg_norm).await {
        Ok(nonce) => Some(nonce),
        Err(e) => {
            warn!(error=?e, "legacy: failed to write intent marker");
            None
        }
    };

    // Ordering invariant: artifact, then sidecars, then index job.
    // The conditional create IS the immutability rule (pypi.org's): a plain
    // HEAD-then-PUT is a TOCTOU hole that lets concurrent uploads swap bytes.
    let size = spooled.size;
    match state
        .storage
        .put_file_if_absent(&key, spooled.path.path(), Some("application/octet-stream"))
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

    // PEP 658: capture the wheel's METADATA as a static file next to it.
    if is_wheel {
        match wheel_metadata {
            Some(md) => {
                if let Err(e) = state
                    .storage
                    .put_bytes(&metadata_key(&key), md, Some("text/plain; charset=utf-8"))
                    .await
                {
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
        if let Some(prov) = fields.get("provenance") {
            if let Err(e) = state
                .storage
                .put_bytes(
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

    let sc = Sidecar {
        sha256,
        size,
        version,
        upload_time,
        requires_python: fields.get("requires_python").cloned(),
        yanked,
    };
    let sc_bytes = serde_json::to_vec(&sc).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode sidecar".to_string(),
        )
    })?;
    state
        .storage
        .put_bytes(&sidecar_key(&key), sc_bytes, Some("application/json"))
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store sidecar".to_string(),
            )
        })?;

    // Commit marker: truth changed, rebuild now. Pairs with the intent above
    // so the worker consumes both; if this write fails the intent still goes
    // stale and heals the package.
    if let Err(e) = commit_marker(&state, &pkg_norm, intent_nonce).await {
        warn!(error=?e, "legacy: failed to write commit marker");
    }

    // Read-your-writes by waiting: poll our own index until the file shows
    // up, so publish-then-install pipelines never see a missing version.
    if state.sync_uploads {
        wait_for_index_visibility(&state, &pkg_norm, &filename).await;
    }

    // Return a simple OK text body compatible with legacy clients.
    Ok((StatusCode::OK, "OK"))
}

/// Bounded wait for a freshly uploaded file to appear in the package index.
/// A timeout still returns success upstream — the artifact is durable and the
/// index will catch up; failing the upload would only provoke a client retry
/// into the 409 from immutability.
async fn wait_for_index_visibility(state: &AppState, pkg: &str, filename: &str) {
    let key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
    let deadline = std::time::Instant::now() + state.sync_upload_timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(bytes) = state.storage.get_bytes(&key).await {
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
    warn!(%pkg, %filename, "sync upload: index visibility wait timed out");
}

/// Current time as RFC 3339 at whole-second precision.
fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .unwrap_or_else(|_| OffsetDateTime::now_utc())
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Commit a truth change, pairing with `intent_nonce` when the intent marker
/// landed (so the worker consumes both), and wake the worker now instead of
/// letting the marker wait out the tick — upload→visible drops from
/// ~tick+rebuild to ~rebuild. Peer nodes still ride the marker/tick path;
/// the nudge is a same-process accelerant only.
pub(crate) async fn commit_marker(
    state: &AppState,
    pkg: &str,
    intent_nonce: Option<String>,
) -> Result<()> {
    match intent_nonce {
        Some(nonce) => worker::mark_commit(state.storage.as_ref(), pkg, &nonce).await?,
        None => worker::mark_dirty(state.storage.as_ref(), pkg).await?,
    }
    state.worker_nudge.notify_one();
    Ok(())
}

/// --- Simple index endpoints ----------------------------------------------
const CT_JSON: &str = render::SIMPLE_JSON_CONTENT_TYPE;
const CT_HTML: &str = render::SIMPLE_HTML_CONTENT_TYPE;
/// Indexes change on every rebuild: always revalidate, never stale.
const INDEX_CACHE_CONTROL: &str = "no-cache";
/// Filenames are immutable, so artifact bytes can be cached forever.
const ARTIFACT_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

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
    let (key, ct) = if json {
        (format!("{SIMPLE_PREFIX}index.json"), CT_JSON)
    } else {
        (format!("{SIMPLE_PREFIX}index.html"), CT_HTML)
    };
    serve_index(state, key, ct, INDEX_CACHE_CONTROL, headers).await
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
    let json = force_json || accepts_json(headers);
    if let Some(resp) = proxy_package_index(state, &pkg, json, headers).await {
        return resp;
    }
    let (key, ct) = if json {
        (format!("{SIMPLE_PREFIX}{pkg}/index.json"), CT_JSON)
    } else {
        (format!("{SIMPLE_PREFIX}{pkg}/index.html"), CT_HTML)
    };
    serve_index(state, key, ct, INDEX_CACHE_CONTROL, headers).await
}

/// Resolve the proxy for `pkg`, enforcing the eligibility gate (the
/// dependency-confusion defense) in one place. `None` = no proxy configured or
/// the name is ineligible (private / reserved prefix), so fall through to local
/// serving; `Some(Err)` = origin unreadable, an outage to surface rather than
/// answer "who owns this name" optimistically; `Some(Ok)` = serve upstream.
async fn eligible_proxy<'a>(
    state: &'a AppState,
    pkg: &str,
) -> Option<Result<&'a Arc<proxy::Proxy>, Response<Body>>> {
    let proxy = state.proxy.as_ref()?;
    match proxy::eligible(state, pkg).await {
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
    pkg: &str,
    json: bool,
    headers: &HeaderMap,
) -> Option<Response<Body>> {
    let proxy = state.proxy.as_ref()?;
    match proxy::eligible(state, pkg).await {
        Ok(true) => {}
        Ok(false) => return None,
        // Origin unreadable is an outage: never answer "what owns this name"
        // questions optimistically (the dependency-confusion direction).
        Err(e) => return Some(read_error(e)),
    }
    let rendered = proxy.package_index(state, pkg, json).await?;
    let revalidated = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "*" || v.contains(&*rendered.etag))
        .unwrap_or(false);
    let builder = Response::builder()
        .header(header::ETAG, &*rendered.etag)
        .header(header::CACHE_CONTROL, INDEX_CACHE_CONTROL);
    let resp = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        builder
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, if json { CT_JSON } else { CT_HTML })
            .header(header::CONTENT_LENGTH, rendered.body.len())
            .body(Body::from(rendered.body.clone()))
    };
    Some(resp.unwrap_or_else(not_found))
}

/// Serve a materialized index file with a content-hash ETag; conditional GETs
/// revalidate to 304. Bytes and ETag come from the in-memory cache — the hot
/// path costs zero storage calls and zero hashing (see cache.rs).
async fn serve_index(
    state: &AppState,
    key: String,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    let (identity, gzip) = match state.index_cache.get(state.storage.as_ref(), &key).await {
        Ok(Some(hit)) => hit,
        Ok(None) => return not_found("no such index"),
        Err(e) => return read_error(e),
    };

    // Content negotiation against the precompressed variant: zero per-request
    // CPU — big indexes were NIC-bound, and gzip is a ~5-7x cut in bytes.
    // Each representation carries its own strong ETag (hence Vary).
    let accepts_gzip = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("gzip"))
        .unwrap_or(false);
    let (variant, encoding) = match (&gzip, accepts_gzip) {
        (Some(gz), true) => (gz, Some("gzip")),
        _ => (&identity, None),
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
        let resp = serve_index(
            &state,
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
            if let Some(upstream) = proxy_metadata_passthrough(&state, &pkg, &filename).await {
                return upstream;
            }
        }
        return resp;
    }

    // PEP 740 provenance companion: same RAM-cache + passthrough story as
    // metadata, served as JSON. A mirror snapshot is point-in-time, so it is
    // cached as immutably as the artifact it describes.
    if filename.ends_with(PROVENANCE_SUFFIX) && headers.get(header::RANGE).is_none() {
        let resp = serve_index(
            &state,
            key,
            "application/json",
            ARTIFACT_CACHE_CONTROL,
            &headers,
        )
        .await;
        if resp.status() == StatusCode::NOT_FOUND {
            if let Some(upstream) = proxy_provenance_passthrough(&state, &pkg, &filename).await {
                return upstream;
            }
        }
        return resp;
    }

    // On-demand mirroring: make sure the artifact is in storage before the
    // presign/stream logic runs (a presigned redirect never observes a 404,
    // so the fetch can't be triggered by one).
    if let Some(resp) = proxy_ensure_artifact(&state, &pkg, &filename).await {
        return resp;
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
        // No existence check: presigning is local HMAC math, so the redirect
        // path costs zero network round trips. A signed URL to a missing key
        // gets S3's own 404 (the server's credentials carry s3:ListBucket —
        // required for index rebuilds — which is what makes S3 say 404
        // rather than 403). Existence is the index's job, not this path's.
        // Immutability also makes signed URLs reusable across clients: serve
        // a cached one while it has plenty of validity left (see cache.rs).
        if let Some(url) = state.presign_cache.fresh(&key) {
            if let Some(k) = &dl_key {
                state.counters.record("downloads", k);
                state.metrics.record_download();
            }
            return found_redirect(&url);
        }
        match state.storage.presign_get(&key, cache::PRESIGN_EXPIRY).await {
            Ok(Some(url)) => {
                let url: Arc<str> = url.into();
                state.presign_cache.put(&key, url.clone());
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
    match state.storage.serve_artifact(&key, range).await {
        Ok(mut resp) => {
            // Count only a full delivered body (200): a 206 range read is a
            // partial of one logical download, a 416 is none. (A whole-file range
            // served as 206 — rare, e.g. `curl -C-`/`wget -c` — is undercounted;
            // download stats are best-effort, so we don't parse Content-Range.)
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
        Err(e) => read_error(e),
    }
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

/// Proxy hook for artifact downloads: fetch-and-commit on a local miss.
/// `None` means fall through to normal serving (the file is now in storage,
/// was already there, or doesn't exist upstream either); `Some` is a hard
/// failure response (storage outage, upstream verification failure).
async fn proxy_ensure_artifact(
    state: &Arc<AppState>,
    pkg: &str,
    filename: &str,
) -> Option<Response<Body>> {
    let proxy = match eligible_proxy(state, pkg).await {
        Some(Ok(proxy)) => proxy,
        Some(Err(resp)) => return Some(resp),
        None => return None,
    };
    match proxy.ensure_artifact_cached(state, pkg, filename).await {
        Ok(()) => None,
        Err(e) => Some(read_error(e)),
    }
}

/// Serve a PEP 658 companion straight from upstream, no storage writes.
async fn proxy_metadata_passthrough(
    state: &Arc<AppState>,
    pkg: &str,
    filename: &str,
) -> Option<Response<Body>> {
    let proxy = match eligible_proxy(state, pkg).await {
        Some(Ok(proxy)) => proxy,
        Some(Err(resp)) => return Some(resp),
        None => return None,
    };
    let bytes = proxy.fetch_metadata(state, pkg, filename).await?;
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
    pkg: &str,
    filename: &str,
) -> Option<Response<Body>> {
    let proxy = match eligible_proxy(state, pkg).await {
        Some(Ok(proxy)) => proxy,
        Some(Err(resp)) => return Some(resp),
        None => return None,
    };
    let bytes = proxy.fetch_provenance(state, pkg, filename).await?;
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

fn moved_permanently(location: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header(header::LOCATION, location)
        .body(Body::empty())
        .unwrap_or_else(not_found)
}

fn unauthorized() -> Response<Body> {
    // The WWW-Authenticate header rides in via middleware.
    let mut resp = Response::new(Body::from("Unauthorized"));
    *resp.status_mut() = StatusCode::UNAUTHORIZED;
    resp
}

fn not_found<E: std::fmt::Debug>(err: E) -> Response<Body> {
    warn!(error=?err, "read miss");
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::NOT_FOUND;
    resp
}

/// 404 only when storage says the object does not exist; everything else is
/// an outage and must surface as 503 — telling pip "no such package" during
/// an S3 blip is the dependency-confusion direction.
fn read_error(err: anyhow::Error) -> Response<Body> {
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
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    match state.storage.head_exists(&key).await {
        Ok(true) => {}
        Ok(false) => return Err((StatusCode::NOT_FOUND, "No such file".into())),
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error: {e}"),
            ));
        }
    }

    // Intent before any mutation: a crash mid-delete heals via the stale
    // intent instead of leaving the view permanently ahead of truth.
    let intent_nonce = worker::mark_intent(state.storage.as_ref(), &pkg).await.ok();

    worker::rebuild_package_excluding(&state, &pkg, Some(&filename))
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("index rewrite failed: {e}"),
            )
        })?;

    state
        .storage
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
    // The `.origin` claim is durable on purpose: deleting every artifact must
    // not release the name for the *opposite* world to re-claim. Otherwise a
    // credentialed client could empty a mirror-owned public name and re-upload
    // it as a private package (the dependency-confusion direction). Re-purposing
    // a name from private to mirror is an operator action gated on storage
    // access — delete the `.origin` file directly.
    let _ = state
        .storage
        .delete_keys(&[
            sidecar_key(&key),
            sidecar::metadata_key(&key),
            sidecar::provenance_key(&key),
        ])
        .await;

    // Worker confirms from truth and prunes global membership if needed.
    if let Err(e) = commit_marker(&state, &pkg, intent_nonce).await {
        warn!(error=?e, "delete: failed to write commit marker");
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
    state: &AppState,
    headers: &HeaderMap,
    package: &str,
    filename: &str,
    yanked: Yanked,
) -> Result<StatusCode, (StatusCode, String)> {
    require_admin(state, headers)?;
    let Some(pkg) = checked_pkg_name(package).filter(|_| valid_artifact_filename(filename)) else {
        return Err((StatusCode::NOT_FOUND, "No such file".to_string()));
    };
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let sc_key = sidecar_key(&key);

    let bytes = state
        .storage
        .get_bytes(&sc_key)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "No such file".to_string()))?;
    let mut sc: Sidecar = serde_json::from_slice(&bytes).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bad sidecar: {e}"),
        )
    })?;
    sc.yanked = yanked;

    let intent_nonce = worker::mark_intent(state.storage.as_ref(), &pkg).await.ok();
    let out = serde_json::to_vec(&sc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")))?;
    state
        .storage
        .put_bytes(&sc_key, out, Some("application/json"))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;

    if let Err(e) = commit_marker(state, &pkg, intent_nonce).await {
        warn!(error=?e, "yank: failed to write commit marker");
    }
    Ok(StatusCode::OK)
}

/// Set a project's PEP 792 status (admin). The body is the status doc, e.g.
/// `{"status":"quarantined","reason":"..."}`. An `active` target carries no
/// marker, so it is treated as a clear. This is how mirror-over-HTTP `sync`
/// relays an upstream freeze; the marker is truth, so the index heals from it.
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

/// Write (or, for `active`, remove) the project-status marker, then rebuild the
/// index — status changes what the listing renders (a quarantine serves no
/// files). Marker is truth, so this is crash-safe via the intent/commit pair.
/// Callers MUST enforce admin auth first.
async fn write_project_status(
    state: &AppState,
    package: &str,
    doc: status::ProjectStatusDoc,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(pkg) = checked_pkg_name(package) else {
        return Err((StatusCode::NOT_FOUND, "no such package".to_string()));
    };

    let intent_nonce = worker::mark_intent(state.storage.as_ref(), &pkg).await.ok();
    let result = if doc.status.is_active() {
        status::clear_status(state.storage.as_ref(), &pkg).await
    } else {
        status::write_status(state.storage.as_ref(), &pkg, &doc).await
    };
    result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;

    if let Err(e) = commit_marker(state, &pkg, intent_nonce).await {
        warn!(error=?e, "status: failed to write commit marker");
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
    let bytes = match state.storage.get_bytes(sync::CURSORS_KEY).await {
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
    let key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
    let bytes = match state.storage.get_bytes(&key).await {
        Ok(b) => b,
        Err(e) if storage::is_not_found(&e) => br#"{"files":[]}"#.to_vec(),
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("read: {e}"))),
    };
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

impl AppState {
    /// State for one-shot storage operations (rebuild-index) — no credentials, no
    /// server, default knobs. Only the storage-facing fields matter.
    fn headless(storage: Arc<dyn Storage>) -> Self {
        AppState {
            storage,
            uploader_user: None,
            uploader_pass: None,
            admin_user: None,
            admin_pass: None,
            read_user: None,
            read_pass: None,
            private_prefix: None,
            artifact_delivery: ArtifactDelivery::Auto,
            worker_interval: Duration::from_secs(1),
            reconcile_interval: Duration::from_secs(86400),
            intent_grace: time::Duration::seconds(900),
            audit_on_boot: true,
            lease_ttl: Duration::from_secs(30),
            sync_uploads: false,
            sync_upload_timeout: Duration::from_secs(10),
            index_cache: Arc::new(cache::IndexCache::new(cache::INDEX_CACHE_TTL)),
            presign_cache: Arc::new(cache::PresignCache::new(cache::PRESIGN_CACHE_TTL)),
            spool_dir: std::env::temp_dir(),
            global_names: Arc::new(tokio::sync::Mutex::new(None)),
            inventory: Arc::new(tokio::sync::Mutex::new(worker::InventoryMap::default())),
            worker_nudge: Arc::new(tokio::sync::Notify::new()),
            metrics: Arc::new(metrics::Metrics::new()),
            counters: Arc::new(counters::Counters::disabled()),
            download_board: Arc::new(std::sync::Mutex::new(None)),
            proxy: None,
            started: std::time::Instant::now(),
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
    fn read_credential(&self) -> Option<(&str, &str)> {
        cred_pair(self.read_user.as_deref(), self.read_pass.as_deref())
    }

    /// No write credential configured: every write path is disabled and the
    /// server is read-only. Unauthenticated open writes were a footgun on the
    /// default 0.0.0.0 bind, not a dev convenience.
    fn uploads_disabled(&self) -> bool {
        self.uploader_credential().is_none() && self.admin_credential().is_none()
    }

    /// Does the request authenticate as admin?
    fn is_admin(&self, headers: &HeaderMap) -> bool {
        self.admin_credential()
            .is_some_and(|(u, p)| check_basic_auth(headers, u, p).is_ok())
    }

    /// May the request publish? Admin ⊇ uploader.
    fn is_uploader(&self, headers: &HeaderMap) -> bool {
        self.is_admin(headers)
            || self
                .uploader_credential()
                .is_some_and(|(u, p)| check_basic_auth(headers, u, p).is_ok())
    }

    /// May the request read indexes and artifacts? Public unless a read
    /// credential is configured; any stronger credential also reads
    /// (admin ⊇ uploader ⊇ reader).
    fn is_reader(&self, headers: &HeaderMap) -> bool {
        match self.read_credential() {
            None => true,
            Some((u, p)) => check_basic_auth(headers, u, p).is_ok() || self.is_uploader(headers),
        }
    }
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

/// Gate the privileged routes (delete, yank) behind the admin credential.
fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if state.is_admin(headers) {
        return Ok(());
    }
    Err(if state.admin_credential().is_none() {
        (
            StatusCode::FORBIDDEN,
            "This operation is disabled (no admin credential configured)".into(),
        )
    } else {
        (StatusCode::UNAUTHORIZED, "Admin credential required".into())
    })
}

/// Length-independent constant-time byte equality, so credential checks don't
/// leak the secret one prefix-byte at a time (CWE-208). The length may leak;
/// the bytes do not.
fn ct_eq(a: &str, b: &str) -> bool {
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

    fn basic_headers(user: &str, pass: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        let v = format!("Basic {}", b64.encode(format!("{user}:{pass}")));
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(&v).unwrap());
        h
    }

    #[test]
    fn rank_packages_rolls_up_files_and_ranks_busiest_first() {
        use std::collections::BTreeMap;
        let day = |entries: &[(&str, u64)]| counters::DaySummary {
            total: entries.iter().map(|(_, c)| c).sum(),
            top: entries.iter().map(|(k, c)| (k.to_string(), *c)).collect(),
        };
        let mut summaries: BTreeMap<String, counters::DaySummary> = BTreeMap::new();
        summaries.insert(
            "2026-06-20".into(),
            day(&[
                ("requests/requests-2.31.0-py3-none-any.whl", 6),
                ("requests/requests-2.30.0-py3-none-any.whl", 4),
                ("flask/flask-3.0.0-py3-none-any.whl", 7),
            ]),
        );
        summaries.insert(
            "2026-06-21".into(),
            day(&[
                ("requests/requests-2.31.0-py3-none-any.whl", 5),
                ("zeta/zeta-1.0.0-py3-none-any.whl", 7),
            ]),
        );
        // requests rolls up across files AND days (6+4+5=15) and ranks first;
        // flask & zeta tie at 7, broken by name ascending (flask before zeta).
        assert_eq!(
            rank_packages(&summaries),
            vec![
                ("requests".to_string(), 15),
                ("flask".to_string(), 7),
                ("zeta".to_string(), 7),
            ]
        );
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
}
