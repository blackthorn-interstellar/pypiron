use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
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
use serde::{Deserialize, Serialize};
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
use storage::{DiskStorage, ObjectData, S3Storage, Storage};

const PACKAGES_PREFIX: &str = "packages/";
const SIMPLE_PREFIX: &str = "simple/";
const QUEUE_PENDING_PREFIX: &str = "_internal/queue/pending/";
const QUEUE_PROCESSING_PREFIX: &str = "_internal/queue/processing/";

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

    /// Number of jobs to process per worker batch
    #[arg(long, env = "PYPIRON_JOB_BATCH_SIZE", default_value = "20")]
    job_batch_size: usize,

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
    job_batch_size: usize,
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
        job_batch_size: cli.job_batch_size,
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
        .route("/files/:package/:filename", get(files_get))
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
    // auth - only check if credentials are configured
    if let (Some(user), Some(pass)) = (&state.upload_user, &state.upload_pass) {
        if check_basic_auth(&headers, user, pass).is_err() {
            return Err((StatusCode::UNAUTHORIZED, "Unauthorized".into()));
        }
    }

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

    // Enqueue a job for background indexing; best-effort
    if let Err(e) = enqueue_job(&state, &pkg_norm, &filename, &key).await {
        warn!(error=?e, "legacy: failed to enqueue indexing job");
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

#[derive(Serialize, Deserialize)]
struct Job {
    package: String,
    filename: String,
    s3_key: String,
    uploaded_at: String,
}

async fn enqueue_job(state: &AppState, package: &str, filename: &str, s3_key: &str) -> Result<()> {
    let ts = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "now".into());
    let stamp = OffsetDateTime::now_utc().unix_timestamp();
    let job = Job {
        package: package.to_string(),
        filename: filename.to_string(),
        s3_key: s3_key.to_string(),
        uploaded_at: ts,
    };
    let name = format!("{stamp}-{package}-{filename}.json");
    let body = serde_json::to_vec(&job)?;
    let key = format!("{QUEUE_PENDING_PREFIX}{name}");
    state
        .storage
        .put_bytes(&key, body, Some("application/json"))
        .await?;
    Ok(())
}

/// --- Simple index endpoints ----------------------------------------------
async fn simple_root(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response<Body> {
    if accepts_json(&headers) {
        stream_object(
            &state,
            format!("{SIMPLE_PREFIX}index.json"),
            Some("application/vnd.pypi.simple.v1+json"),
        )
        .await
        .unwrap_or_else(internal_or_404)
    } else {
        stream_object(
            &state,
            format!("{SIMPLE_PREFIX}index.html"),
            Some("text/html"),
        )
        .await
        .unwrap_or_else(internal_or_404)
    }
}

async fn simple_root_json(State(state): State<Arc<AppState>>) -> Response<Body> {
    stream_object(
        &state,
        format!("{SIMPLE_PREFIX}index.json"),
        Some("application/vnd.pypi.simple.v1+json"),
    )
    .await
    .unwrap_or_else(internal_or_404)
}

async fn simple_pkg(
    State(state): State<Arc<AppState>>,
    Path(pkg): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    let pkg = normalize_pkg_name(&pkg);
    if accepts_json(&headers) {
        stream_object(
            &state,
            format!("{SIMPLE_PREFIX}{pkg}/index.json"),
            Some("application/vnd.pypi.simple.v1+json"),
        )
        .await
        .unwrap_or_else(internal_or_404)
    } else {
        stream_object(
            &state,
            format!("{SIMPLE_PREFIX}{pkg}/index.html"),
            Some("text/html"),
        )
        .await
        .unwrap_or_else(internal_or_404)
    }
}

async fn simple_pkg_json(
    State(state): State<Arc<AppState>>,
    Path(pkg): Path<String>,
) -> Response<Body> {
    let pkg = normalize_pkg_name(&pkg);
    stream_object(
        &state,
        format!("{SIMPLE_PREFIX}{pkg}/index.json"),
        Some("application/vnd.pypi.simple.v1+json"),
    )
    .await
    .unwrap_or_else(internal_or_404)
}

/// --- Artifact download endpoint ------------------------------------------
async fn files_get(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
) -> Response<Body> {
    let pkg = normalize_pkg_name(&package);
    stream_object(&state, format!("{PACKAGES_PREFIX}{pkg}/{filename}"), None)
        .await
        .unwrap_or_else(internal_or_404)
}

async fn stream_object(
    state: &AppState,
    key: String,
    override_ct: Option<&'static str>,
) -> Result<Response<Body>> {
    let out: ObjectData = state
        .storage
        .get_bytes(&key)
        .await
        .with_context(|| key.clone())?;

    let ct = override_ct
        .map(std::string::ToString::to_string)
        .or(out.content_type.clone())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let content_length = out.content_length;
    let mut resp = Response::new(Body::from(out.bytes));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&ct).unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    if let Some(sz) = content_length {
        if let Ok(v) = HeaderValue::from_str(&sz.to_string()) {
            resp.headers_mut().insert(header::CONTENT_LENGTH, v);
        }
    }
    Ok(resp)
}

fn internal_or_404<E: std::fmt::Debug>(err: E) -> Response<Body> {
    // If it's a NotFound we'd prefer a 404; MVP: return 404 for all errors.
    warn!(error=?err, "stream error");
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
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
