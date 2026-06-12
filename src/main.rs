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
mod lease;
mod names;
mod origin;
mod render;
mod sidecar;
mod storage;
mod sync;
mod upload;
mod wheel;
mod worker;

use names::{
    infer_package_from_filename, infer_version_from_filename, is_normalized, normalize_pkg_name,
};
use sidecar::{metadata_key, sidecar_key, Sidecar, Yanked, METADATA_SUFFIX};
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

    /// Uploader credential username — may publish (ordinary uploads). When no
    /// credential of any kind is set, all writes are unauthenticated.
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
    #[arg(long, env = "PYPIRON_RECONCILE_INTERVAL_SECS", default_value = "300")]
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
    private_prefix: Option<String>,
    artifact_delivery: ArtifactDelivery,
    // worker cfg
    worker_interval: Duration,
    reconcile_interval: Duration,
    lease_ttl: Duration,
    sync_uploads: bool,
    sync_upload_timeout: Duration,
    /// RAM-served indexes with precomputed ETags; see cache.rs.
    index_cache: Arc<cache::IndexCache>,
    /// Reused presigned GET URLs for immutable artifacts; see cache.rs.
    presign_cache: Arc<cache::PresignCache>,
    /// Where upload spools live (must be real disk, not tmpfs).
    spool_dir: std::path::PathBuf,
    /// Serializes incremental global-index read-modify-writes in-process.
    global_index_lock: Arc<tokio::sync::Mutex<()>>,
    /// Wakes the worker immediately after a write drops a dirty marker.
    worker_nudge: Arc<tokio::sync::Notify>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // logging
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "info,pypiron=info,aws_config=warn,aws_smithy_http_tower=warn".into()
        }))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Sync(args)) => {
            return sync::run_sync(*args).await;
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
    let storage = cli.storage.build().await?;

    let state = Arc::new(AppState {
        storage,
        uploader_user: cli.uploader_user,
        uploader_pass: cli.uploader_pass,
        admin_user: cli.admin_user,
        admin_pass: cli.admin_pass,
        private_prefix: cli.private_prefix.as_deref().map(normalize_pkg_name),
        artifact_delivery: cli.artifact_delivery,
        worker_interval: Duration::from_secs(cli.worker_interval_secs),
        reconcile_interval: Duration::from_secs(cli.reconcile_interval_secs),
        lease_ttl: Duration::from_secs(cli.lease_ttl_secs),
        sync_uploads: cli.sync_uploads,
        sync_upload_timeout: Duration::from_secs(cli.sync_upload_timeout_secs),
        index_cache: Arc::new(cache::IndexCache::new(cache::INDEX_CACHE_TTL)),
        presign_cache: Arc::new(cache::PresignCache::new(cache::PRESIGN_CACHE_TTL)),
        spool_dir: cli.spool_dir.unwrap_or_else(std::env::temp_dir),
        global_index_lock: Arc::new(tokio::sync::Mutex::new(())),
        worker_nudge: Arc::new(tokio::sync::Notify::new()),
    });

    if state.auth_open() {
        warn!("no credentials configured: uploads, mirror, deletes, and yanks are UNAUTHENTICATED");
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
        // Catch-all for debugging unmatched routes
        .fallback(fallback_handler)
        .with_state(state.clone())
        // Axum's default 2 MB body limit would reject any real wheel.
        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024))
        .layer(middleware::from_fn(log_requests));

    // spawn worker (with a shutdown handle so it can release the leader
    // lease on graceful exit)
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let worker_handle = tokio::spawn(worker::run_worker_until(state.clone(), shutdown_rx));

    // serve
    info!("listening on http://{}", cli.bind_addr);
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
    tracing::debug!("Incoming request: {} {}", method, uri);
    let response = next.run(req).await;
    tracing::debug!("Response status: {} {} {}", response.status(), method, uri);
    response
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
        return Err((StatusCode::UNAUTHORIZED, "Unauthorized".into()));
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
                if let Ok(text) = field.text().await {
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
    if filename.contains('/') || filename.contains('\\') || !sidecar::is_artifact(&filename) {
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
            return Err(
                if state.admin_credential().is_none() && !state.auth_open() {
                    (
                        StatusCode::FORBIDDEN,
                        "Mirror uploads are disabled (no admin credential configured)".into(),
                    )
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        "Mirror uploads require the admin credential".into(),
                    )
                },
            );
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
            let winner = origin::claim_origin(state.storage.as_ref(), &pkg_norm, desired_origin)
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

    // Drop a dirty marker for the worker; best-effort — the artifact is
    // durable either way and the periodic reconcile heals lost markers.
    if let Err(e) = mark_dirty(&state, &pkg_norm).await {
        warn!(error=?e, "legacy: failed to write dirty marker");
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

/// Mark a package as needing an index rebuild (empty object at `_dirty/<pkg>`).
async fn mark_dirty(state: &AppState, pkg: &str) -> Result<()> {
    worker::mark_dirty(state.storage.as_ref(), pkg).await?;
    // Wake the worker now instead of letting the marker wait out the tick —
    // upload→visible drops from ~tick+rebuild to ~rebuild. Peer nodes still
    // ride the marker/tick path; this is a same-process accelerant only.
    state.worker_nudge.notify_one();
    Ok(())
}

/// --- Simple index endpoints ----------------------------------------------
const CT_JSON: &str = "application/vnd.pypi.simple.v1+json";
const CT_HTML: &str = "text/html; charset=utf-8";
/// Indexes change on every rebuild: always revalidate, never stale.
const INDEX_CACHE_CONTROL: &str = "no-cache";
/// Filenames are immutable, so artifact bytes can be cached forever.
const ARTIFACT_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

async fn simple_root(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response<Body> {
    if accepts_json(&headers) {
        serve_index(
            &state,
            format!("{SIMPLE_PREFIX}index.json"),
            CT_JSON,
            INDEX_CACHE_CONTROL,
            &headers,
        )
        .await
    } else {
        serve_index(
            &state,
            format!("{SIMPLE_PREFIX}index.html"),
            CT_HTML,
            INDEX_CACHE_CONTROL,
            &headers,
        )
        .await
    }
}

async fn simple_root_json(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_index(
        &state,
        format!("{SIMPLE_PREFIX}index.json"),
        CT_JSON,
        INDEX_CACHE_CONTROL,
        &headers,
    )
    .await
}

async fn simple_pkg(
    State(state): State<Arc<AppState>>,
    Path(pkg): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    let pkg = normalize_pkg_name(&pkg);
    if !is_normalized(&pkg) {
        return not_found("invalid package name");
    }
    if accepts_json(&headers) {
        serve_index(
            &state,
            format!("{SIMPLE_PREFIX}{pkg}/index.json"),
            CT_JSON,
            INDEX_CACHE_CONTROL,
            &headers,
        )
        .await
    } else {
        serve_index(
            &state,
            format!("{SIMPLE_PREFIX}{pkg}/index.html"),
            CT_HTML,
            INDEX_CACHE_CONTROL,
            &headers,
        )
        .await
    }
}

async fn simple_pkg_json(
    State(state): State<Arc<AppState>>,
    Path(pkg): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    let pkg = normalize_pkg_name(&pkg);
    if !is_normalized(&pkg) {
        return not_found("invalid package name");
    }
    serve_index(
        &state,
        format!("{SIMPLE_PREFIX}{pkg}/index.json"),
        CT_JSON,
        INDEX_CACHE_CONTROL,
        &headers,
    )
    .await
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
    let pkg = normalize_pkg_name(&package);
    let servable = match filename.strip_suffix(METADATA_SUFFIX) {
        Some(base) => sidecar::is_artifact(base),
        None => sidecar::is_artifact(&filename),
    };
    if !is_normalized(&pkg) || !servable || filename.contains('/') || filename.contains('\\') {
        return not_found("not an artifact");
    }
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");

    // PEP 658 metadata is immutable, tiny, and hammered by resolvers (uv
    // fetches one per candidate wheel) — serve it from the same RAM cache as
    // the indexes instead of one storage GET per request. Range requests
    // fall through to storage; nobody range-reads a METADATA file.
    if filename.ends_with(METADATA_SUFFIX) && headers.get(header::RANGE).is_none() {
        return serve_index(
            &state,
            key,
            "text/plain; charset=utf-8",
            ARTIFACT_CACHE_CONTROL,
            &headers,
        )
        .await;
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
    if redirect && !filename.ends_with(METADATA_SUFFIX) {
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

fn found_redirect(url: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, url)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::empty())
        .unwrap_or_else(not_found)
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
    let pkg = normalize_pkg_name(&package);
    // Artifacts only: .origin, sidecars, and metadata companions are managed
    // by the server, not deletable handles.
    if !is_normalized(&pkg) || !sidecar::is_artifact(&filename) || filename.contains('/') {
        return Err((StatusCode::NOT_FOUND, "No such file".into()));
    }
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
        .delete_keys(&[sidecar_key(&key), sidecar::metadata_key(&key)])
        .await;

    // Worker confirms from truth and prunes global membership if needed.
    if let Err(e) = mark_dirty(&state, &pkg).await {
        warn!(error=?e, "delete: failed to write dirty marker");
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
    let pkg = normalize_pkg_name(package);
    if !is_normalized(&pkg) || !sidecar::is_artifact(filename) || filename.contains('/') {
        return Err((StatusCode::NOT_FOUND, "No such file".to_string()));
    }
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

    let out = serde_json::to_vec(&sc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")))?;
    state
        .storage
        .put_bytes(&sc_key, out, Some("application/json"))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;

    if let Err(e) = mark_dirty(state, &pkg).await {
        warn!(error=?e, "yank: failed to write dirty marker");
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
    /// The configured uploader credential, if any (both halves required).
    fn uploader_credential(&self) -> Option<(&str, &str)> {
        match (&self.uploader_user, &self.uploader_pass) {
            (Some(u), Some(p)) => Some((u, p)),
            _ => None,
        }
    }

    /// The configured admin credential, if any. Its presence is what enables
    /// the privileged operations (mirror, delete, yank).
    fn admin_credential(&self) -> Option<(&str, &str)> {
        match (&self.admin_user, &self.admin_pass) {
            (Some(u), Some(p)) => Some((u, p)),
            _ => None,
        }
    }

    /// No credentials of any kind configured: writes are fully open (dev mode).
    fn auth_open(&self) -> bool {
        self.uploader_credential().is_none() && self.admin_credential().is_none()
    }

    /// Does the request authenticate as admin? (Open mode is admin.)
    fn is_admin(&self, headers: &HeaderMap) -> bool {
        if self.auth_open() {
            return true;
        }
        match self.admin_credential() {
            Some((u, p)) => check_basic_auth(headers, u, p).is_ok(),
            None => false,
        }
    }

    /// May the request publish? Admin ⊇ uploader; open mode allows all.
    fn is_uploader(&self, headers: &HeaderMap) -> bool {
        if self.is_admin(headers) {
            return true;
        }
        match self.uploader_credential() {
            Some((u, p)) => check_basic_auth(headers, u, p).is_ok(),
            None => false,
        }
    }
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

fn check_basic_auth(headers: &HeaderMap, user: &str, pass: &str) -> Result<()> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| anyhow!("missing authorization"))?
        .to_str()
        .map_err(|_| anyhow!("bad header"))?;

    let prefix = "Basic ";
    if !auth.starts_with(prefix) {
        return Err(anyhow!("not basic auth"));
    }

    let decoded = b64
        .decode(auth.trim_start_matches(prefix))
        .map_err(|_| anyhow!("bad base64"))?;
    let s = String::from_utf8(decoded).map_err(|_| anyhow!("utf8"))?;
    let mut parts = s.splitn(2, ':');
    let u = parts.next().unwrap_or("");
    let p = parts.next().unwrap_or("");

    if u == user && p == pass {
        Ok(())
    } else {
        Err(anyhow!("bad credentials"))
    }
}
