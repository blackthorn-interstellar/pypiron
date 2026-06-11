use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
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
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use sha2::{Digest, Sha256};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::{info, warn};

mod names;
mod render;
mod sidecar;
mod storage;
mod sync;
mod worker;

use names::{infer_package_from_filename, infer_version_from_filename, normalize_pkg_name};
use sidecar::{sidecar_key, Sidecar, Yanked};
use storage::{DiskStorage, S3Storage, Storage};

const PACKAGES_PREFIX: &str = "packages/";
const SIMPLE_PREFIX: &str = "simple/";
const DIRTY_PREFIX: &str = "_dirty/";

/// Storage backend selection.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum StorageBackend {
    Disk,
    S3,
}

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
    Serve(ServeArgs),
    /// Mirror packages from PyPI (or another source) into this PypIron instance
    Sync(sync::SyncArgs),
}

/// `PypIron` - A fast, reliable, and scalable `PyPI` server
#[derive(ClapArgs, Debug, Clone)]
struct ServeArgs {
    /// Storage backend to use: "disk" or "s3"
    #[arg(long, env = "PYPIRON_STORAGE", value_enum, default_value_t = StorageBackend::Disk)]
    storage: StorageBackend,

    /// Root data directory for disk storage (defaults to $HOME/.pypiron/packages)
    #[arg(long, env = "PYPIRON_DATA_DIR")]
    data_dir: Option<String>,

    /// S3 bucket name for package storage (required if --storage s3)
    #[arg(long, env = "PYPIRON_S3_BUCKET")]
    s3_bucket: Option<String>,

    /// AWS region (e.g., us-east-1)
    #[arg(long, env = "AWS_REGION")]
    aws_region: Option<String>,

    /// S3 endpoint URL (for S3-compatible services)
    #[arg(long, env = "PYPIRON_S3_ENDPOINT_URL")]
    s3_endpoint_url: Option<String>,

    /// Force S3 path-style addressing
    #[arg(long, env = "PYPIRON_S3_FORCE_PATH_STYLE")]
    s3_force_path_style: bool,

    /// Basic auth username for uploads (optional, allows unauthenticated uploads if not provided)
    #[arg(long, env = "PYPIRON_BASIC_AUTH_USER")]
    basic_auth_user: Option<String>,

    /// Basic auth password for uploads (optional, allows unauthenticated uploads if not provided)
    #[arg(long, env = "PYPIRON_BASIC_AUTH_PASS")]
    basic_auth_pass: Option<String>,

    /// Worker interval in seconds
    #[arg(long, env = "PYPIRON_WORKER_INTERVAL_SECS", default_value = "5")]
    worker_interval_secs: u64,

    /// Full-reconcile sweep interval in seconds (the self-heal backbone)
    #[arg(long, env = "PYPIRON_RECONCILE_INTERVAL_SECS", default_value = "300")]
    reconcile_interval_secs: u64,

    /// Upload confirmation timeout in seconds
    #[arg(
        long,
        env = "PYPIRON_UPLOAD_CONFIRM_TIMEOUT_SECS",
        default_value = "300"
    )]
    upload_confirm_timeout_secs: u64,

    /// Public base URL for generating absolute URLs (optional)
    #[arg(long, env = "PYPIRON_PUBLIC_BASE_URL")]
    public_base_url: Option<String>,

    /// Address to bind the server to
    #[arg(long, env = "PYPIRON_BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind_addr: String,
}

#[derive(Clone)]
struct AppState {
    storage: Arc<dyn Storage>,
    // auth
    upload_user: Option<String>,
    upload_pass: Option<String>,
    // worker cfg
    worker_interval: Duration,
    reconcile_interval: Duration,
    #[allow(dead_code)]
    upload_confirm_timeout: Duration,
    // behavior
    #[allow(dead_code)]
    public_base_url: Option<String>,
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
            return sync::run_sync(args).await;
        }
        Some(Commands::Serve(args)) => {
            return run_serve(args).await;
        }
        None => {
            // Back-compat: `pypiron` with no subcommand runs the server
            return run_serve(cli.serve).await;
        }
    }
}

async fn run_serve(cli: ServeArgs) -> Result<()> {
    // Build storage backend
    let storage: Arc<dyn Storage> = match cli.storage {
        StorageBackend::Disk => {
            let data_dir = cli.data_dir.unwrap_or_else(|| {
                std::env::var("HOME")
                    .map(|home| format!("{}/.pypiron/packages", home))
                    .unwrap_or_else(|_| "./.pypiron/packages".to_string())
            });
            Arc::new(DiskStorage::new(&data_dir))
        }
        StorageBackend::S3 => {
            let bucket = cli
                .s3_bucket
                .clone()
                .ok_or_else(|| anyhow!("--s3-bucket is required when using --storage s3"))?;

            // AWS config
            let mut cfg_loader = aws_config::defaults(BehaviorVersion::latest());
            if let Some(ref r) = cli.aws_region {
                cfg_loader = cfg_loader.region(Region::new(r.clone()));
            }
            let base_cfg = cfg_loader.load().await;

            let mut s3_cfg_builder = aws_sdk_s3::config::Builder::from(&base_cfg);
            if let Some(url) = cli.s3_endpoint_url {
                s3_cfg_builder = s3_cfg_builder.endpoint_url(url);
            }
            if cli.s3_force_path_style {
                s3_cfg_builder = s3_cfg_builder.force_path_style(true);
            }
            let s3 = aws_sdk_s3::Client::from_conf(s3_cfg_builder.build());
            Arc::new(S3Storage::new(s3, bucket))
        }
    };

    let state = Arc::new(AppState {
        storage,
        upload_user: cli.basic_auth_user,
        upload_pass: cli.basic_auth_pass,
        worker_interval: Duration::from_secs(cli.worker_interval_secs),
        reconcile_interval: Duration::from_secs(cli.reconcile_interval_secs),
        upload_confirm_timeout: Duration::from_secs(cli.upload_confirm_timeout_secs),
        public_base_url: cli.public_base_url,
    });

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

    // spawn worker
    tokio::spawn(worker::run_worker(state.clone()));

    // serve
    info!("listening on http://{}", cli.bind_addr);
    let listener = tokio::net::TcpListener::bind(&cli.bind_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}

/// Middleware to log all incoming requests
async fn log_requests(req: Request, next: Next) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();
    info!("Incoming request: {} {}", method, uri);
    let response = next.run(req).await;
    info!("Response status: {}", response.status());
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
    require_auth(&state, &headers)?;

    let mut filename_opt: Option<String> = None;
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut fields: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    while let Some(field) = multipart.next_field().await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Invalid multipart form data".into(),
        )
    })? {
        let field_name = field.name().unwrap_or("").to_string();
        let part_filename = field.file_name().map(|s| s.to_string());

        match field_name.as_str() {
            "content" | "file" => {
                let bytes = field.bytes().await.map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Could not read uploaded file".into(),
                    )
                })?;
                file_bytes = Some(bytes.to_vec());
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
    let data = file_bytes.ok_or((StatusCode::BAD_REQUEST, "Missing file content".to_string()))?;

    // No path separators, dotfiles, or names colliding with sidecar suffixes.
    if filename.contains('/') || filename.contains('\\') || !sidecar::is_artifact(&filename) {
        return Err((StatusCode::BAD_REQUEST, "Invalid filename".into()));
    }

    let pkg_norm = match fields.get("name") {
        Some(name) => normalize_pkg_name(name),
        None => infer_package_from_filename(&filename),
    };
    if pkg_norm.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Could not determine package name".into(),
        ));
    }

    // Verify the client-supplied digest, and capture the hash for the sidecar.
    let sha256 = {
        let mut hasher = Sha256::new();
        hasher.update(&data);
        format!("{:x}", hasher.finalize())
    };
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

    // Filenames are immutable once written (pypi.org rule): supply-chain
    // safety and perfect cacheability in one check.
    if state.storage.head_exists(&key).await.unwrap_or(false) {
        return Err((
            StatusCode::CONFLICT,
            format!("File already exists: {filename}"),
        ));
    }

    // Ordering invariant: artifact, then sidecar, then index job.
    state
        .storage
        .put_bytes(&key, data.clone(), Some("application/octet-stream"))
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store file".to_string(),
            )
        })?;

    let sc = Sidecar {
        sha256,
        size: data.len() as u64,
        version,
        upload_time: now_rfc3339(),
        requires_python: fields.get("requires_python").cloned(),
        yanked: Yanked::Flag(false),
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

    // Return a simple OK text body compatible with legacy clients.
    Ok((StatusCode::OK, "OK"))
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
    state
        .storage
        .put_bytes(&format!("{DIRTY_PREFIX}{pkg}"), Vec::new(), None)
        .await
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
            &headers,
        )
        .await
    } else {
        serve_index(
            &state,
            format!("{SIMPLE_PREFIX}index.html"),
            CT_HTML,
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
    if accepts_json(&headers) {
        serve_index(
            &state,
            format!("{SIMPLE_PREFIX}{pkg}/index.json"),
            CT_JSON,
            &headers,
        )
        .await
    } else {
        serve_index(
            &state,
            format!("{SIMPLE_PREFIX}{pkg}/index.html"),
            CT_HTML,
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
    serve_index(
        &state,
        format!("{SIMPLE_PREFIX}{pkg}/index.json"),
        CT_JSON,
        &headers,
    )
    .await
}

/// Serve a materialized index file with a content-hash ETag; conditional GETs
/// revalidate to 304.
async fn serve_index(
    state: &AppState,
    key: String,
    content_type: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    let bytes = match state.storage.get_bytes(&key).await {
        Ok(o) => o,
        Err(e) => return not_found(e),
    };

    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let etag = format!("\"{:x}\"", hasher.finalize());

    let revalidated = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "*" || v.contains(etag.as_str()))
        .unwrap_or(false);

    let builder = Response::builder()
        .header(header::ETAG, etag.clone())
        .header(header::CACHE_CONTROL, INDEX_CACHE_CONTROL);

    let result = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        builder
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(Body::from(bytes))
    };
    result.unwrap_or_else(not_found)
}

/// --- Artifact download endpoint ------------------------------------------
async fn files_get(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    let pkg = normalize_pkg_name(&package);
    if !sidecar::is_artifact(&filename) {
        return not_found("not an artifact");
    }
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    match state.storage.serve_artifact(&key, range).await {
        Ok(mut resp) => {
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static(ARTIFACT_CACHE_CONTROL),
            );
            resp
        }
        Err(e) => not_found(e),
    }
}

fn not_found<E: std::fmt::Debug>(err: E) -> Response<Body> {
    warn!(error=?err, "read miss");
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::NOT_FOUND;
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
    require_auth(&state, &headers)?;
    let pkg = normalize_pkg_name(&package);
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    if !state.storage.head_exists(&key).await.unwrap_or(false) {
        return Err((StatusCode::NOT_FOUND, "No such file".into()));
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
    require_auth(state, headers)?;
    let pkg = normalize_pkg_name(package);
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

/// Gate writes behind basic auth when credentials are configured.
fn require_auth(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if let (Some(user), Some(pass)) = (&state.upload_user, &state.upload_pass) {
        if check_basic_auth(headers, user, pass).is_err() {
            return Err((StatusCode::UNAUTHORIZED, "Unauthorized".into()));
        }
    }
    Ok(())
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
