use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use axum::{
    body::{to_bytes, Body},
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use base64::engine::general_purpose::STANDARD as b64;
use base64::Engine;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::time::sleep;
use tracing::{error, info, warn};

mod render;
mod storage;
mod worker;

use storage::{DiskStorage, ObjectData, S3Storage, Storage};

const PACKAGES_PREFIX: &str = "packages/";
const SIMPLE_PREFIX: &str = "simple/";
const QUEUE_PENDING_PREFIX: &str = "_internal/queue/pending/";
const QUEUE_PROCESSING_PREFIX: &str = "_internal/queue/processing/";

/// Storage backend selection.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum StorageBackend {
    /// Use the local filesystem (default).
    Disk,
    /// Use AWS S3 or an S3-compatible service.
    S3,
}

/// `PypIron` - A fast, reliable, and scalable `PyPI` server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Storage backend to use: "disk" or "s3"
    #[arg(long, env = "PYPIRON_STORAGE", value_enum, default_value_t = StorageBackend::Disk)]
    storage: StorageBackend,

    /// Root data directory for disk storage
    #[arg(long, env = "PYPIRON_DATA_DIR", default_value = "./pypiron-data")]
    data_dir: String,

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
    #[arg(long, env = "PYPIRON_WORKER_INTERVAL_SECS", default_value = "300")]
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
    upload_confirm_timeout: Duration,
    // behavior
    #[allow(dead_code)]
    public_base_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UploadQuery {
    filename: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // logging
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "info,pypiron=info,aws_config=warn,aws_smithy_http_tower=warn".into()
        }))
        .init();

    // parse CLI args (with env var fallbacks)
    let cli = Cli::parse();

    // Build storage backend
    let storage: Arc<dyn Storage> = match cli.storage {
        StorageBackend::Disk => Arc::new(DiskStorage::new(&cli.data_dir)),
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
        .route("/", post(upload_redirect))
        // Local upload endpoint (used by DiskStorage presign URL)
        .route("/_upload/:token", post(local_upload).put(local_upload))
        .route("/simple", get(simple_root))
        .route("/simple/", get(simple_root))
        .route("/simple/index.json", get(simple_root_json))
        .route("/simple/:package", get(simple_pkg))
        .route("/simple/:package/", get(simple_pkg))
        .route("/simple/:package/index.json", get(simple_pkg_json))
        .route("/files/:package/:filename", get(files_get))
        .with_state(state.clone());

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
/// POST `/` → Basic-auth; do not read body; require filename hint in header or query;
/// generate upload URL (presigned S3 or local) and respond 307 with Location.
/// In the background, confirm upload exists and enqueue a job json into pending/.
async fn upload_redirect(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<UploadQuery>,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    // auth - only check if credentials are configured
    if let (Some(user), Some(pass)) = (&state.upload_user, &state.upload_pass) {
        if check_basic_auth(&headers, user, pass).is_err() {
            return Err((StatusCode::UNAUTHORIZED, "Unauthorized"));
        }
    }

    let filename = filename_from_hint(&headers, q.filename.as_deref()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Missing filename hint (X-Filename header or ?filename= query)",
        )
    })?;

    let package = infer_package_from_filename(&filename);
    let pkg_norm = normalize_pkg_name(&package);
    let key = format!("{PACKAGES_PREFIX}{pkg_norm}/{filename}");

    // presign/prepare upload URL
    let upload_url = state
        .storage
        .presign_put(&key, Duration::from_secs(15 * 60))
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "presign failed"))?;

    // fire-and-forget: poll for object existence, then enqueue job file
    let state_bg = state.clone();
    let filename_bg = filename.clone();
    tokio::spawn(async move {
        if wait_until_exists(&state_bg, &key, state_bg.upload_confirm_timeout).await {
            if let Err(e) = enqueue_job(&state_bg, &pkg_norm, &filename_bg, &key).await {
                error!(error=?e, "failed to enqueue job");
            }
        } else {
            warn!(key=%key, "upload not observed within timeout; no job enqueued");
        }
    });

    // 307 with Location
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    resp.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&upload_url).unwrap_or(HeaderValue::from_static("")),
    );
    Ok(resp)
}

/// Local/Disk upload sink used by DiskStorage::presign_put.
/// The :token encodes the final object key (URL-safe base64).
async fn local_upload(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    body: Body,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    let key = storage::DiskStorage::key_from_token(&token)
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad upload token"))?;
    let data = to_bytes(body, 100 * 1024 * 1024) // 100MB limit
        .await
        .map_err(|_| (StatusCode::BAD_REQUEST, "failed to read body"))?;
    state
        .storage
        .put_bytes(&key, data.to_vec(), Some("application/octet-stream"))
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "write failed"))?;
    Ok(StatusCode::NO_CONTENT)
}

fn filename_from_hint(headers: &HeaderMap, query_filename: Option<&str>) -> Result<String> {
    if let Some(v) = headers.get("x-filename") {
        return Ok(v.to_str()?.to_owned());
    }
    if let Some(f) = query_filename {
        return Ok(f.to_owned());
    }
    Err(anyhow!("no filename hint"))
}

async fn wait_until_exists(state: &AppState, key: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        match state.storage.head_exists(key).await {
            Ok(true) => return true,
            Ok(false) => {}
            Err(e) => {
                warn!(error=?e, key=%key, "exists check failed");
            }
        }
        if start.elapsed() >= timeout {
            return false;
        }
        sleep(Duration::from_secs(3)).await;
    }
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

/// Extract distribution name (PEP 427 wheel or sdist) → lower & PEP 503 normalize.
fn infer_package_from_filename(filename: &str) -> String {
    // common cases:
    //  - requests-2.28.1-py3-none-any.whl                  → "requests"
    //  - my_pkg_name-1.2.3.tar.gz / .zip / .tar.bz2       → "my_pkg_name"
    let stem = filename.split('/').next_back().unwrap_or(filename);

    // Wheel: distribution-version-...
    let dist = if let Some(idx) = stem.find('-') {
        &stem[..idx]
    } else if let Some(idx) = stem.rfind(".tar.") {
        // sdist with two extensions (.tar.gz, .tar.bz2, .tar.xz)
        stem.split_at(idx)
            .0
            .rsplit_once('-')
            .map_or(stem, |(d, _)| d)
    } else if let Some((d, _v)) = stem.rsplit_once('-') {
        d
    } else {
        stem
    };
    normalize_pkg_name(dist)
}

/// PEP 503 normalization: lowercase; replace runs of [-_.] with single '-'.
fn normalize_pkg_name(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut last_dash = false;
    for ch in lower.chars() {
        let is_sep = ch == '-' || ch == '_' || ch == '.';
        if is_sep {
            if !last_dash {
                out.push('-');
                last_dash = true;
            }
        } else {
            out.push(ch);
            last_dash = false;
        }
    }
    out.trim_matches('-').to_string()
}
