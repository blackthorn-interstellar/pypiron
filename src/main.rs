use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Multipart, Path, Request, State},
    http::{header, HeaderMap, HeaderValue, Response, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use base64::engine::general_purpose::STANDARD as b64;
use base64::Engine;
use clap::{Args as ClapArgs, Parser, Subcommand};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::{info, warn};

mod cache;
mod config;
#[cfg(test)]
mod corpus_check;
mod lease;
mod metrics;
mod names;
mod origin;
mod proxy;
mod render;
mod sidecar;
mod simple;
mod status;
mod storage;
mod sync;
mod upload;
mod verify;
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

/// Top-level CLI. If no subcommand is supplied, we run the server (serve).
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Subcommands: `serve` (default) or `sync`
    #[command(subcommand)]
    command: Option<Commands>,

    /// Flattened server args so `pypiron` with no subcommand still works.
    #[command(flatten)]
    serve: ServeArgs,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the PypIron server (same args as top-level default)
    Serve(Box<ServeArgs>),
    /// Mirror packages from PyPI (or another source) into this PypIron instance
    Sync(Box<sync::SyncArgs>),
    /// Recompute every index from truth and diff against what storage serves
    /// (read-only); exits nonzero on any divergence
    Verify(Box<verify::VerifyArgs>),
    /// Rebuild every materialized view from truth, unconditionally. Run after
    /// restoring a backup or editing storage out-of-band.
    Resync(Box<ResyncArgs>),
}

#[derive(ClapArgs, Debug)]
struct ResyncArgs {
    #[command(flatten)]
    storage: StorageArgs,
}

/// One-shot deep audit against a storage backend, no server attached.
async fn run_resync(args: ResyncArgs) -> Result<()> {
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
    /// deletion, and yank. Configuring it is what enables those operations.
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
    /// streams. See docs/DESIGN.md for the tradeoffs.
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

    /// Full-reconcile sweep interval in seconds (the self-heal backbone)
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

    /// Log output format: `text` (human-readable) or `json` (one object per
    /// line, for log pipelines).
    #[arg(long, env = "PYPIRON_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,

    /// Serve unknown (non-private) packages on demand from this upstream
    /// simple index (e.g. https://pypi.org): package pages are answered from
    /// upstream metadata and artifacts are downloaded, verified, and cached
    /// in storage as `mirror`-origin packages on first request. Names claimed
    /// `private` (or inside --private-prefix) never fall through. Off by
    /// default.
    #[arg(long, env = "PYPIRON_PROXY_UPSTREAM")]
    proxy_upstream: Option<String>,

    /// Filters gating what the proxy serves and caches (same semantics as
    /// the `sync` filters, under a `--proxy-` prefix).
    #[command(flatten)]
    proxy_filter: proxy::ProxyFilterArgs,
}

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
    /// Wakes the worker immediately after a write drops a dirty marker.
    worker_nudge: Arc<tokio::sync::Notify>,
    /// Hand-rolled Prometheus counters served at /metrics.
    metrics: Arc<metrics::Metrics>,
    /// On-demand upstream mirroring (None unless --proxy-upstream is set).
    proxy: Option<Arc<proxy::Proxy>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // logging — format comes from --log-format/PYPIRON_LOG_FORMAT (the env
    // var also reaches `sync` runs through the flattened serve args).
    let env_filter =
        std::env::var("RUST_LOG").unwrap_or_else(|_| "info,pypiron=info,object_store=warn".into());
    match cli.serve.log_format {
        LogFormat::Text => tracing_subscriber::fmt().with_env_filter(env_filter).init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init(),
    }

    match cli.command {
        Some(Commands::Sync(args)) => {
            return sync::run_sync(*args).await;
        }
        Some(Commands::Verify(args)) => {
            return verify::run_verify(*args).await;
        }
        Some(Commands::Resync(args)) => {
            return run_resync(*args).await;
        }
        Some(Commands::Serve(args)) => {
            return run_serve(*args).await;
        }
        None => {
            // Back-compat: `pypiron` with no subcommand runs the server
            return run_serve(cli.serve).await;
        }
    }
}

async fn run_serve(cli: ServeArgs) -> Result<()> {
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

    let storage = cli.storage.build().await?;
    let proxy = match cli.proxy_upstream.as_deref() {
        Some(upstream) => Some(Arc::new(proxy::Proxy::new(upstream, &cli.proxy_filter)?)),
        None => None,
    };

    let state = Arc::new(AppState {
        storage,
        uploader_user: cli.uploader_user,
        uploader_pass: cli.uploader_pass,
        admin_user: cli.admin_user,
        admin_pass: cli.admin_pass,
        read_user: cli.read_user,
        read_pass: cli.read_pass,
        private_prefix: cli.private_prefix.as_deref().map(normalize_pkg_name),
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
        worker_nudge: Arc::new(tokio::sync::Notify::new()),
        metrics: Arc::new(metrics::Metrics::new()),
        proxy,
    });

    if state.uploads_disabled() {
        warn!("no credentials configured: the server is read-only (set --uploader-user/--admin-user to enable uploads)");
    } else {
        if state.admin_credential().is_none() {
            info!("no admin credential: mirror uploads, deletion, and yank are disabled");
        }
        if state.admin_credential().is_some()
            && state.admin_credential() == state.uploader_credential()
        {
            warn!("uploader and admin credentials are identical: every uploader has admin powers");
        }
    }
    if state.read_credential().is_none() {
        info!("no read credential: indexes and artifacts are served without authentication");
    }
    if let Some(p) = &state.proxy {
        info!(upstream = %p.upstream(), "on-demand proxy enabled");
        if state.private_prefix.is_none() {
            warn!("proxy enabled without --private-prefix: new private uploads race public names for first claim; a reserved prefix closes that hole");
        }
    }

    // Initialize empty index files if they don't exist
    initialize_indexes(&state).await?;

    // router
    let app = Router::new()
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
    info!(
        version = env!("CARGO_PKG_VERSION"),
        "listening on http://{}", cli.bind_addr
    );
    let listener = tokio::net::TcpListener::bind(&cli.bind_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Tell the worker to stop and give it a moment to release the leader
    // lease — that hand-off is what keeps a restart from being a lease-TTL
    // write outage on the successor.
    let _ = shutdown_tx.send(true);
    if tokio::time::timeout(Duration::from_secs(5), worker_handle)
        .await
        .is_err()
    {
        warn!("worker did not stop within 5s; exiting without lease release");
    }
    Ok(())
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
    info!("shutdown signal received");
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
            return found_redirect(&url);
        }
        match state.storage.presign_get(&key, cache::PRESIGN_EXPIRY).await {
            Ok(Some(url)) => {
                let url: Arc<str> = url.into();
                state.presign_cache.put(&key, url.clone());
                return found_redirect(&url);
            }
            Ok(None) => {} // disk backend: fall through to streaming
            Err(e) => warn!(error=?e, %key, "presign failed; falling back to streaming"),
        }
    }

    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    match state.storage.serve_artifact(&key, range).await {
        Ok(mut resp) => {
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static(ARTIFACT_CACHE_CONTROL),
            );
            resp
        }
        Err(e) => read_error(e),
    }
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
    /// State for one-shot storage operations (resync) — no credentials, no
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
            worker_nudge: Arc::new(tokio::sync::Notify::new()),
            metrics: Arc::new(metrics::Metrics::new()),
            proxy: None,
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
