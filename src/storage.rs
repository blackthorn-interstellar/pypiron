use anyhow::{anyhow, Context as _, Result};
use async_trait::async_trait;
use axum::body::Body;
use clap::{Args as ClapArgs, ValueEnum};
use http::{header, Response, StatusCode};
use std::io::SeekFrom;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

// Cloud object-store deps: S3, GCS, and Azure Blob behind one API. Disk is a
// separate, dependency-free backend; everything remote shares one impl.
use futures::StreamExt as _;
use object_store::aws::{AmazonS3Builder, S3CopyIfNotExists};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::path::Path as OsPath;
use object_store::signer::Signer;
use object_store::{
    Attribute, Attributes, Error as OsError, GetOptions, GetRange, ObjectStore, ObjectStoreExt,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, UpdateVersion, WriteMultipart,
};

use crate::range::{parse_range, RangeSpec};

/// Storage backend selection.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum StorageBackend {
    Disk,
    S3,
    Gcs,
    Azure,
}

/// Storage configuration shared by `serve` and `sync` — one binary, one
/// storage layer, no second implementation.
#[derive(ClapArgs, Debug, Clone)]
pub struct StorageArgs {
    /// Storage backend to use: "disk", "s3", "gcs", or "azure"
    #[arg(long, env = "PYPIRON_STORAGE", value_enum, default_value_t = StorageBackend::Disk)]
    pub storage: StorageBackend,

    /// Root data directory for disk storage (defaults to $HOME/.pypiron/packages)
    #[arg(long, env = "PYPIRON_DATA_DIR")]
    pub data_dir: Option<String>,

    /// S3 bucket name for package storage (required if --storage s3)
    #[arg(long, env = "PYPIRON_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// AWS region (e.g., us-east-1)
    #[arg(long, env = "AWS_REGION")]
    pub aws_region: Option<String>,

    /// S3 endpoint URL (for S3-compatible services)
    #[arg(long, env = "PYPIRON_S3_ENDPOINT_URL")]
    pub s3_endpoint_url: Option<String>,

    /// Force S3 path-style addressing
    #[arg(long, env = "PYPIRON_S3_FORCE_PATH_STYLE")]
    pub s3_force_path_style: bool,

    // --- Google Cloud Storage (--storage gcs) ---
    /// GCS bucket name for package storage (required if --storage gcs)
    #[arg(long, env = "PYPIRON_GCS_BUCKET")]
    pub gcs_bucket: Option<String>,

    /// Path to a GCS service-account JSON key. Without it, Application Default
    /// Credentials are used — but presigned URLs are then unavailable.
    #[arg(long, env = "PYPIRON_GCS_SERVICE_ACCOUNT_PATH")]
    pub gcs_service_account_path: Option<String>,

    /// GCS endpoint URL (for a local emulator such as fake-gcs-server)
    #[arg(long, env = "PYPIRON_GCS_ENDPOINT_URL")]
    pub gcs_endpoint_url: Option<String>,

    // --- Azure Blob Storage (--storage azure) ---
    /// Azure storage account name (required if --storage azure)
    #[arg(long, env = "PYPIRON_AZURE_ACCOUNT")]
    pub azure_account: Option<String>,

    /// Azure blob container for package storage (required if --storage azure)
    #[arg(long, env = "PYPIRON_AZURE_CONTAINER")]
    pub azure_container: Option<String>,

    /// Azure storage account access key. Enables presigned (SAS) URLs.
    #[arg(long, env = "PYPIRON_AZURE_ACCESS_KEY")]
    pub azure_access_key: Option<String>,

    /// Azure endpoint URL (for a local emulator such as Azurite)
    #[arg(long, env = "PYPIRON_AZURE_ENDPOINT_URL")]
    pub azure_endpoint_url: Option<String>,

    /// Use the Azurite storage emulator (well-known dev account and key)
    #[arg(long, env = "PYPIRON_AZURE_USE_EMULATOR")]
    pub azure_use_emulator: bool,
}

impl StorageArgs {
    pub async fn build(&self) -> Result<Arc<dyn Storage>> {
        let storage = self.build_backend().await?;
        // Crash-consistency hook for the chaos tests: abort the process just
        // before the Nth mutating storage operation. Inert without the env
        // var; see tests/test_crash_consistency.py.
        if let Some(n) = std::env::var("PYPIRON_FAULT_ABORT_AFTER_WRITES")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
        {
            return Ok(Arc::new(FaultInjectStorage::new(storage, n)));
        }
        Ok(storage)
    }

    /// The disk data directory actually used, applying the default.
    fn resolved_data_dir(&self) -> String {
        self.data_dir.clone().unwrap_or_else(|| {
            std::env::var("HOME")
                .map(|home| format!("{home}/.pypiron/packages"))
                .unwrap_or_else(|_| "./.pypiron/packages".to_string())
        })
    }

    /// Short, human-friendly description for the startup banner.
    pub fn describe(&self) -> String {
        match self.storage {
            StorageBackend::Disk => format!("disk · {}", self.resolved_data_dir()),
            StorageBackend::S3 => format!("s3 · {}", self.s3_bucket.as_deref().unwrap_or("?")),
            StorageBackend::Gcs => format!("gcs · {}", self.gcs_bucket.as_deref().unwrap_or("?")),
            StorageBackend::Azure => {
                format!("azure · {}", self.azure_container.as_deref().unwrap_or("?"))
            }
        }
    }

    async fn build_backend(&self) -> Result<Arc<dyn Storage>> {
        match self.storage {
            StorageBackend::Disk => {
                let data_dir = self.resolved_data_dir();
                Ok(Arc::new(DiskStorage::new(&data_dir)))
            }
            StorageBackend::S3 => {
                let bucket = self
                    .s3_bucket
                    .clone()
                    .ok_or_else(|| anyhow!("--s3-bucket is required when using --storage s3"))?;
                // Credentials come from the standard AWS chain (env vars, web
                // identity, instance metadata). The default S3ConditionalPut is
                // ETag-match, so the single-PUT create-if-absent path works on
                // S3 and S3-compatible stores out of the box; large artifacts
                // are published with a multipart copy-if-not-exists.
                let mut b = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
                if let Some(ref r) = self.aws_region {
                    b = b.with_region(r.clone());
                }
                if let Some(ref url) = self.s3_endpoint_url {
                    b = b.with_endpoint(url.clone());
                    if url.starts_with("http://") {
                        b = b.with_allow_http(true);
                    }
                }
                if self.s3_force_path_style {
                    b = b.with_virtual_hosted_style_request(false);
                } else if self.s3_endpoint_url.is_none() {
                    // Real AWS prefers virtual-hosted-style addressing; custom
                    // endpoints (MinIO et al.) keep the path-style default.
                    b = b.with_virtual_hosted_style_request(true);
                }
                let s3 = Arc::new(b.build().context("configure S3 backend")?);
                let store: Arc<dyn ObjectStore> = s3.clone();
                let signer: Arc<dyn Signer> = s3;
                Ok(Arc::new(ObjectStorage::new(store, Some(signer), "s3")))
            }
            StorageBackend::Gcs => {
                let bucket = self
                    .gcs_bucket
                    .clone()
                    .ok_or_else(|| anyhow!("--gcs-bucket is required when using --storage gcs"))?;
                let mut b = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
                // Presigning needs a service-account private key; ADC tokens
                // cannot sign URLs, so presign is disabled under ADC.
                let mut can_sign = false;
                if let Some(ref p) = self.gcs_service_account_path {
                    b = b.with_service_account_path(p.clone());
                    can_sign = true;
                }
                if let Some(ref url) = self.gcs_endpoint_url {
                    // Emulator (fake-gcs-server): point at it and skip signing.
                    b = b
                        .with_config(GoogleConfigKey::BaseUrl, url.clone())
                        .with_config(GoogleConfigKey::SkipSignature, "true");
                    can_sign = false;
                }
                let gcs = Arc::new(b.build().context("configure GCS backend")?);
                let store: Arc<dyn ObjectStore> = gcs.clone();
                let signer = can_sign.then_some(gcs as Arc<dyn Signer>);
                Ok(Arc::new(ObjectStorage::new(store, signer, "gcs")))
            }
            StorageBackend::Azure => {
                let container = self.azure_container.clone().ok_or_else(|| {
                    anyhow!("--azure-container is required when using --storage azure")
                })?;
                let mut b = MicrosoftAzureBuilder::from_env().with_container_name(container);
                // SAS presigning needs the account access key.
                let mut can_sign = false;
                if let Some(ref a) = self.azure_account {
                    b = b.with_account(a.clone());
                }
                if let Some(ref k) = self.azure_access_key {
                    b = b.with_access_key(k.clone());
                    can_sign = true;
                }
                if self.azure_use_emulator {
                    b = b.with_use_emulator(true);
                    can_sign = true;
                }
                if let Some(ref url) = self.azure_endpoint_url {
                    b = b.with_endpoint(url.clone());
                    if url.starts_with("http://") {
                        b = b.with_allow_http(true);
                    }
                }
                let az = Arc::new(b.build().context("configure Azure backend")?);
                let store: Arc<dyn ObjectStore> = az.clone();
                let signer = can_sign.then_some(az as Arc<dyn Signer>);
                Ok(Arc::new(ObjectStorage::new(store, signer, "azure")))
            }
        }
    }
}

/// Sentinel error for "object does not exist" — callers translate this to
/// 404; every other storage error is an outage and must surface as one.
#[derive(Debug, thiserror::Error)]
#[error("not found: {0}")]
pub struct NotFound(pub String);

/// True if `err` is (or wraps) a missing-object error.
pub fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<NotFound>().is_some()
}

/// A file from a directory listing, with the metadata index rendering needs.
pub struct FileEntry {
    pub key: String,
    pub size: u64,
    /// RFC 3339 last-modified timestamp (serves as PEP 700 upload-time).
    pub last_modified: Option<String>,
}

/// One object from a flat (recursive) listing — see [`Storage::list_all`].
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    /// Opaque change detector, compared for equality only: the S3 ETag, or
    /// mtime+size on disk. Two listings agree on (key, size, etag) iff the
    /// object hasn't been rewritten between them.
    pub etag: String,
}

/// First characters a key can have under the prefixes the audit enumerates:
/// normalized package names start with [a-z0-9] (names.rs), and the global
/// index files are `index.html`/`index.json`. Fanning a flat listing out over
/// these sub-prefixes makes enumeration parallel — S3 pagination within one
/// prefix is inherently serial.
pub const SHARD_CHARS: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

#[async_trait]
pub trait Storage: Send + Sync {
    /// Check if an object exists.
    async fn head_exists(&self, key: &str) -> Result<bool>;

    /// Serve an artifact as an HTTP response, honoring a `Range` header.
    /// Each backend uses its native range machinery (seek for disk, S3's own
    /// validation for S3). Errors mean "not found" to the caller.
    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>>;

    /// A presigned GET URL, where the backend supports one (S3). `None` means
    /// "serve it yourself" (disk).
    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>>;

    /// Write bytes to `key`. `content_type` is best-effort (ignored on Disk).
    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()>;

    /// Atomically create `key` only if it does not exist. Returns false when
    /// the object was already there (or we lost the race). This is what
    /// enforces filename immutability and origin exclusivity — a HEAD check
    /// alone is a TOCTOU hole.
    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool>;

    /// `put_if_absent`, but the body comes from a local file — artifacts of
    /// any size are stored without ever being held in memory.
    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool>;

    /// Read full object bytes (indexes, sidecars — small files only).
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>>;

    /// List immediate file entries under the directory `dir_prefix` (non-recursive),
    /// returning full keys (dir_prefix + filename) with size and last-modified.
    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>>;

    /// Flat, recursive listing of every object whose key starts with
    /// `prefix`, sorted by key. This is the cheap way to see an entire
    /// corpus: one paged LIST per 1,000 keys on S3 (vs. one LIST per
    /// directory), one filesystem walk on disk. `prefix` is a *key* prefix,
    /// not a directory — `packages/a` matches every package starting with
    /// 'a', which is how callers parallelize (see [`SHARD_CHARS`]).
    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// Delete multiple keys (best-effort).
    async fn delete_keys(&self, keys: &[String]) -> Result<()>;

    /// Whether this backend supports conditional writes for leader leases.
    /// Disk is explicitly single-node: no lease, always leader.
    fn supports_leases(&self) -> bool {
        false
    }

    /// Read object bytes plus ETag; `None` if the object is missing.
    async fn get_with_etag(&self, _key: &str) -> Result<Option<(Vec<u8>, String)>> {
        Err(anyhow!("leases are not supported by this backend"))
    }

    /// Create-if-absent (`If-None-Match: *`). `Some(etag)` on success,
    /// `None` if the object already exists or we lost the race.
    async fn put_if_none_match(&self, _key: &str, _bytes: Vec<u8>) -> Result<Option<String>> {
        Err(anyhow!("leases are not supported by this backend"))
    }

    /// Replace-if-unchanged (`If-Match`). `Some(new_etag)` on success,
    /// `None` if the ETag no longer matches.
    async fn put_if_match(
        &self,
        _key: &str,
        _etag: &str,
        _bytes: Vec<u8>,
    ) -> Result<Option<String>> {
        Err(anyhow!("leases are not supported by this backend"))
    }
}

/// EXDEV ("invalid cross-device link"), hardcoded so we don't pull in libc.
const EXDEV: i32 = 18;

/// ------------------------------ DiskStorage -------------------------------
pub struct DiskStorage {
    root: PathBuf,
}

impl DiskStorage {
    pub fn new<P: Into<PathBuf>>(root: P) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, key: &str) -> Result<PathBuf> {
        if key.is_empty() {
            return Err(anyhow!("empty key"));
        }
        let rel = Path::new(key);
        let mut clean = PathBuf::new();
        for c in rel.components() {
            match c {
                Component::Normal(seg) => clean.push(seg),
                Component::CurDir => continue,
                _ => return Err(anyhow!("invalid key component in {}", key)),
            }
        }
        Ok(self.root.join(clean))
    }

    async fn ensure_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    /// A unique temp path next to `path` (same filesystem, so rename/link is atomic).
    fn tmp_sibling(&self, path: &Path) -> Result<PathBuf> {
        // A per-process atomic counter — not the clock alone — guarantees a
        // distinct staging path per call. On a coarse-clock host two concurrent
        // writes to the same key can read identical nanos, share one tmp inode,
        // and clobber each other's bytes (corrupting e.g. the .origin marker).
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("bad path"))?;
        let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        Ok(path.with_file_name(format!(".tmp-{nanos}-{}-{seq}-{name}", std::process::id())))
    }

    /// hard_link `tmp`→`dest` as an atomic create-if-absent (EEXIST → already
    /// there), then remove `tmp` regardless. `Ok(false)` means the destination
    /// already existed.
    async fn link_atomic(&self, tmp: &Path, dest: &Path) -> Result<bool> {
        let created = match fs::hard_link(tmp, dest).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        };
        let _ = fs::remove_file(tmp).await;
        created
    }
}

#[async_trait]
impl Storage for DiskStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        let p = self.resolve(key)?;
        Ok(fs::metadata(p).await.is_ok())
    }

    async fn presign_get(
        &self,
        _key: &str,
        _expires: std::time::Duration,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        let path = self.resolve(key)?;
        let md = match fs::metadata(&path).await {
            Ok(md) => md,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(NotFound(key.to_string()).into());
            }
            Err(e) => return Err(anyhow::Error::from(e).context(format!("stat {key}"))),
        };
        if !md.is_file() {
            return Err(NotFound(key.to_string()).into());
        }
        let size = md.len();

        let resp = match parse_range(range, size) {
            RangeSpec::Full => {
                let file = fs::File::open(&path).await?;
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, size)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(Body::from_stream(ReaderStream::new(file)))?
            }
            RangeSpec::Partial(start, end) => {
                let mut file = fs::File::open(&path).await?;
                file.seek(SeekFrom::Start(start)).await?;
                let len = end - start + 1;
                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_LENGTH, len)
                    .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"))
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(Body::from_stream(ReaderStream::new(file.take(len))))?
            }
            RangeSpec::Unsatisfiable => Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{size}"))
                .body(Body::empty())?,
        };
        Ok(resp)
    }

    async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<()> {
        // Write-to-tmp + rename: a crash or full disk never leaves a torn
        // file at the final key. S3 PUTs are already atomic.
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        let tmp = self.tmp_sibling(&p)?;
        fs::write(&tmp, bytes).await?;
        if let Err(e) = fs::rename(&tmp, &p).await {
            let _ = fs::remove_file(&tmp).await;
            return Err(e.into());
        }
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bool> {
        // hard_link fails with EEXIST if the destination exists — an atomic
        // create-if-absent with full content, unlike create_new + write.
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        let tmp = self.tmp_sibling(&p)?;
        fs::write(&tmp, bytes).await?;
        self.link_atomic(&tmp, &p).await
    }

    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        _content_type: Option<&str>,
    ) -> Result<bool> {
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        // Same atomic create-if-absent as put_if_absent. Try linking the
        // source directly (free when the spool shares a filesystem with the
        // data dir); EXDEV falls back to a copy into a tmp sibling first.
        match fs::hard_link(path, &p).await {
            Ok(()) => return Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(e) if e.raw_os_error() == Some(EXDEV) => {}
            Err(e) => return Err(anyhow::Error::from(e)),
        }
        let tmp = self.tmp_sibling(&p)?;
        fs::copy(path, &tmp).await?;
        self.link_atomic(&tmp, &p).await
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let p = self.resolve(key)?;
        match fs::read(&p).await {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(NotFound(key.to_string()).into())
            }
            Err(e) => Err(anyhow::Error::from(e).context(format!("read {key}"))),
        }
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        // A missing directory is an empty listing; any other error must
        // propagate — a silent empty here would make the reconciler delete
        // live indexes off a phantom "no packages" observation.
        let dir = self.resolve(dir_prefix)?;
        let mut rd = match fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(anyhow::Error::from(e).context(format!("list {dir_prefix}"))),
        };
        let mut files = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let md = entry.metadata().await?;
            if md.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    let last_modified = md
                        .modified()
                        .ok()
                        .map(OffsetDateTime::from)
                        .and_then(|t| t.format(&Rfc3339).ok());
                    files.push(FileEntry {
                        key: format!("{}{}", dir_prefix, name),
                        size: md.len(),
                        last_modified,
                    });
                }
            }
        }
        files.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(files)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        for k in keys {
            if let Ok(p) = self.resolve(k) {
                let _ = fs::remove_file(p).await;
            }
        }
        Ok(())
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        // Key prefix, not directory: walk the deepest enclosing directory and
        // filter first-level names against the remainder, so a sharded call
        // ("packages/a") never walks the other shards' trees. The walk is
        // std::fs on a blocking thread — a million-file tree is syscall
        // bound, and tokio::fs would add a channel hop per dirent.
        let (dir_part, name_filter) = match prefix.rfind('/') {
            Some(i) => (&prefix[..=i], &prefix[i + 1..]),
            None => ("", prefix),
        };
        let root = self.resolve(if dir_part.is_empty() { "." } else { dir_part })?;
        let dir_prefix = dir_part.to_string();
        let name_filter = name_filter.to_string();
        tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            let top = match std::fs::read_dir(&root) {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
                Err(e) => return Err(anyhow::Error::from(e).context("list_all root")),
            };
            for entry in top {
                let entry = entry?;
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if !name.starts_with(&name_filter) {
                    continue;
                }
                walk_disk(&entry.path(), &format!("{dir_prefix}{name}"), &mut out)?;
            }
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        })
        .await?
    }
}

/// Recurse one filesystem subtree, appending every regular file as an
/// ObjectMeta keyed by `key_base` plus its relative path.
fn walk_disk(path: &Path, key_base: &str, out: &mut Vec<ObjectMeta>) -> Result<()> {
    let md = std::fs::symlink_metadata(path)?;
    if md.is_file() {
        out.push(ObjectMeta {
            key: key_base.to_string(),
            size: md.len(),
            etag: disk_etag(&md),
        });
        return Ok(());
    }
    if !md.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        walk_disk(&entry.path(), &format!("{key_base}/{name}"), out)?;
    }
    Ok(())
}

/// mtime (nanos) + size: changes whenever the file is rewritten. Disk writes
/// go through tmp+rename, so a content change always produces a new inode
/// with a new mtime.
fn disk_etag(md: &std::fs::Metadata) -> String {
    let mtime = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{mtime}-{}", md.len())
}

/// ------------------------------ ObjectStorage -----------------------------
/// One backend for every cloud object store — S3, GCS, and Azure Blob — over
/// the `object_store` crate. Disk stays a separate, dependency-free backend;
/// everything remote shares this single implementation, so there is no
/// per-cloud code to drift.
pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    /// Present only when the backend can mint presigned GET URLs (S3 always;
    /// GCS with a service-account key; Azure with an account key or emulator).
    /// `None` means "serve it yourself" — never a hard failure.
    signer: Option<Arc<dyn Signer>>,
    /// Backend name, for error context.
    backend: &'static str,
}

/// At or below this size an upload is a single conditional PUT; above it the
/// body streams to a unique staging key as parallel multipart parts (bounded
/// RSS) and is then published atomically with `copy_if_not_exists`. The 16 MB
/// part size keeps a ~900 MB wheel to a handful of in-flight parts.
const MULTIPART_THRESHOLD: u64 = 64 * 1024 * 1024;
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 6;
const READ_CHUNK: usize = 8 * 1024 * 1024;

/// Staging keys live here; large uploads land under this prefix and are then
/// published (copy-if-not-exists) to their final key. Always cleaned up.
const STAGING_PREFIX: &str = "_staging/";

/// Packs object_store's (e_tag, version) pair into one opaque token. Stores use
/// differing combinations to express a conditional update (S3/Azure: ETag; GCS:
/// generation), so we round-trip both. Compared only for equality.
const VERSION_SEP: char = '\u{1f}';

impl ObjectStorage {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        backend: &'static str,
    ) -> Self {
        Self {
            store,
            signer,
            backend,
        }
    }

    /// GET the whole object as a 200 response.
    async fn full_response(&self, path: &OsPath, key: &str) -> Result<Response<Body>> {
        let res = match self.store.get(path).await {
            Ok(r) => r,
            Err(OsError::NotFound { .. }) => return Err(NotFound(key.to_string()).into()),
            Err(e) => {
                return Err(anyhow::Error::from(e).context(format!("{}: get {key}", self.backend)))
            }
        };
        let size = res.meta.size;
        let ct = content_type_of(&res.attributes);
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, size)
            .header(header::CONTENT_TYPE, ct)
            .header(header::ACCEPT_RANGES, "bytes")
            .body(Body::from_stream(res.into_stream()))?)
    }

    /// Stream a spooled file into a multipart upload at `staging`, bounding
    /// resident memory to a few parts in flight. Aborts on any error so no
    /// orphaned parts linger billable.
    async fn stream_multipart(
        &self,
        staging: &OsPath,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<()> {
        let opts = PutMultipartOptions::from(ct_attrs(content_type));
        let upload = self
            .store
            .put_multipart_opts(staging, opts)
            .await
            .with_context(|| format!("{}: begin multipart {staging}", self.backend))?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, MULTIPART_PART_SIZE);
        let mut file = fs::File::open(path)
            .await
            .with_context(|| format!("open upload spool {}", path.display()))?;
        let mut buf = vec![0u8; READ_CHUNK];
        loop {
            let n = match file.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    let _ = writer.abort().await;
                    return Err(anyhow::Error::from(e).context("read upload spool"));
                }
            };
            if let Err(e) = writer.wait_for_capacity(MULTIPART_CONCURRENCY).await {
                let _ = writer.abort().await;
                return Err(anyhow::Error::from(e).context("multipart part upload"));
            }
            writer.write(&buf[..n]);
        }
        writer
            .finish()
            .await
            .with_context(|| format!("{}: finish multipart {staging}", self.backend))?;
        Ok(())
    }
}

#[async_trait]
impl Storage for ObjectStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        match self.store.head(&oskey(key)).await {
            Ok(_) => Ok(true),
            Err(OsError::NotFound { .. }) => Ok(false),
            Err(e) => Err(anyhow::Error::from(e).context(format!("{}: head {key}", self.backend))),
        }
    }

    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>> {
        let Some(signer) = &self.signer else {
            return Ok(None);
        };
        let url = signer
            .signed_url(reqwest::Method::GET, &oskey(key), expires)
            .await
            .with_context(|| format!("{}: presign {key}", self.backend))?;
        Ok(Some(url.to_string()))
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        let path = oskey(key);
        let Some(raw_range) = range else {
            return self.full_response(&path, key).await;
        };
        // A range needs the size to build Content-Range and to reject an
        // unsatisfiable range with 416 — one HEAD, only on ranged requests.
        let size = match self.store.head(&path).await {
            Ok(m) => m.size,
            Err(OsError::NotFound { .. }) => return Err(NotFound(key.to_string()).into()),
            Err(e) => {
                return Err(anyhow::Error::from(e).context(format!("{}: head {key}", self.backend)))
            }
        };
        match parse_range(Some(raw_range), size) {
            RangeSpec::Full => self.full_response(&path, key).await,
            RangeSpec::Unsatisfiable => Ok(Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{size}"))
                .body(Body::empty())?),
            RangeSpec::Partial(start, end) => {
                let opts = GetOptions {
                    range: Some(GetRange::Bounded(start..end + 1)),
                    ..Default::default()
                };
                let res = match self.store.get_opts(&path, opts).await {
                    Ok(r) => r,
                    Err(OsError::NotFound { .. }) => return Err(NotFound(key.to_string()).into()),
                    Err(e) => {
                        return Err(
                            anyhow::Error::from(e).context(format!("{}: get {key}", self.backend))
                        )
                    }
                };
                let ct = content_type_of(&res.attributes);
                let len = end - start + 1;
                Ok(Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_LENGTH, len)
                    .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"))
                    .header(header::CONTENT_TYPE, ct)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(Body::from_stream(res.into_stream()))?)
            }
        }
    }

    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        let opts = PutOptions {
            mode: PutMode::Overwrite,
            attributes: ct_attrs(content_type),
            ..Default::default()
        };
        self.store
            .put_opts(&oskey(key), PutPayload::from(bytes), opts)
            .await
            .with_context(|| format!("{}: put {key}", self.backend))?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let opts = PutOptions {
            mode: PutMode::Create,
            attributes: ct_attrs(content_type),
            ..Default::default()
        };
        match self
            .store
            .put_opts(&oskey(key), PutPayload::from(bytes), opts)
            .await
        {
            Ok(_) => Ok(true),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => Ok(false),
            Err(e) => {
                Err(anyhow::Error::from(e)
                    .context(format!("{}: put_if_absent {key}", self.backend)))
            }
        }
    }

    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let size = fs::metadata(path)
            .await
            .with_context(|| format!("stat upload spool {}", path.display()))?
            .len();
        if size <= MULTIPART_THRESHOLD {
            // Small enough to create with one conditional PUT.
            let bytes = fs::read(path)
                .await
                .with_context(|| format!("read upload spool {}", path.display()))?;
            return self.put_if_absent(key, bytes, content_type).await;
        }
        // Too big for a single PUT: stream to a unique staging key (bounded
        // RSS), then publish atomically. copy_if_not_exists is the race-free
        // create-if-absent for large objects — native on GCS/Azure, a
        // multipart copy on S3.
        let staging = oskey(&staging_key(key));
        self.stream_multipart(&staging, path, content_type).await?;
        let outcome = match self.store.copy_if_not_exists(&staging, &oskey(key)).await {
            Ok(()) => Ok(true),
            Err(OsError::AlreadyExists { .. }) => Ok(false),
            Err(e) => {
                Err(anyhow::Error::from(e).context(format!("{}: publish {key}", self.backend)))
            }
        };
        let _ = self.store.delete(&staging).await;
        outcome
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        match self.store.get(&oskey(key)).await {
            Ok(res) => Ok(res
                .bytes()
                .await
                .with_context(|| format!("{}: read {key}", self.backend))?
                .to_vec()),
            Err(OsError::NotFound { .. }) => Err(NotFound(key.to_string()).into()),
            Err(e) => Err(anyhow::Error::from(e).context(format!("{}: get {key}", self.backend))),
        }
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        // list_with_delimiter is the directory listing: immediate files in
        // `objects`, sub-directories in `common_prefixes` (which we drop). A
        // missing prefix is an empty listing, not an error.
        let res = self
            .store
            .list_with_delimiter(Some(&oskey(dir_prefix)))
            .await
            .with_context(|| format!("{}: list {dir_prefix}", self.backend))?;
        let mut entries: Vec<FileEntry> = res
            .objects
            .into_iter()
            .map(|m| FileEntry {
                key: m.location.as_ref().to_string(),
                size: m.size,
                last_modified: OffsetDateTime::from_unix_timestamp(m.last_modified.timestamp())
                    .ok()
                    .and_then(|t| t.format(&Rfc3339).ok()),
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        // object_store's list() treats the prefix as a directory (it appends a
        // '/'), but our contract is a raw byte prefix: SHARD_CHARS passes
        // "packages/a" to match "packages/alpha". So list the enclosing
        // directory and filter by the exact byte prefix. A trailing-slash
        // prefix ("packages/foo/") lists only that directory; a sharded prefix
        // ("packages/a") lists "packages/" — the audit fans those out across
        // shards in parallel.
        let dir = match prefix.rfind('/') {
            Some(i) => &prefix[..=i],
            None => "",
        };
        let list_prefix = (!dir.is_empty()).then(|| oskey(dir));
        let mut stream = self.store.list(list_prefix.as_ref());
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let m = item.with_context(|| format!("{}: list_all {prefix}", self.backend))?;
            let key = m.location.as_ref();
            if key.starts_with(prefix) {
                out.push(ObjectMeta {
                    key: key.to_string(),
                    size: m.size,
                    etag: pack_version(&m.e_tag, &m.version),
                });
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        for k in keys {
            let _ = self.store.delete(&oskey(k)).await;
        }
        Ok(())
    }

    fn supports_leases(&self) -> bool {
        true
    }

    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match self.store.get(&oskey(key)).await {
            Ok(res) => {
                let etag = pack_version(&res.meta.e_tag, &res.meta.version);
                let bytes = res
                    .bytes()
                    .await
                    .with_context(|| format!("{}: read {key}", self.backend))?
                    .to_vec();
                Ok(Some((bytes, etag)))
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("{}: get {key}", self.backend))),
        }
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        match self
            .store
            .put_opts(
                &oskey(key),
                PutPayload::from(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(res) => Ok(Some(pack_version(&res.e_tag, &res.version))),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e)
                .context(format!("{}: put_if_none_match {key}", self.backend))),
        }
    }

    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let opts = PutOptions::from(PutMode::Update(unpack_version(etag)));
        match self
            .store
            .put_opts(&oskey(key), PutPayload::from(bytes), opts)
            .await
        {
            Ok(res) => Ok(Some(pack_version(&res.e_tag, &res.version))),
            // A failed precondition, a concurrent conditional write, or a
            // since-deleted object: we lost, cleanly.
            Err(
                OsError::Precondition { .. }
                | OsError::AlreadyExists { .. }
                | OsError::NotFound { .. },
            ) => Ok(None),
            Err(e) => {
                Err(anyhow::Error::from(e).context(format!("{}: put_if_match {key}", self.backend)))
            }
        }
    }
}

/// A key as an object_store path. Our keys carry no leading/trailing or doubled
/// slashes, so this round-trips exactly.
fn oskey(key: &str) -> OsPath {
    OsPath::from(key)
}

/// Best-effort content type as object_store attributes (ignored by stores that
/// don't support it).
fn ct_attrs(content_type: Option<&str>) -> Attributes {
    let mut a = Attributes::new();
    if let Some(ct) = content_type {
        a.insert(Attribute::ContentType, ct.to_string().into());
    }
    a
}

fn content_type_of(attrs: &Attributes) -> String {
    attrs
        .get(&Attribute::ContentType)
        .map(|v| v.as_ref().to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

/// A unique staging key for a large upload, namespaced by its final filename.
fn staging_key(key: &str) -> String {
    let fname = key.rsplit('/').next().unwrap_or(key);
    let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
    format!("{STAGING_PREFIX}{nanos}-{}-{fname}", std::process::id())
}

fn pack_version(e_tag: &Option<String>, version: &Option<String>) -> String {
    format!(
        "{}{VERSION_SEP}{}",
        e_tag.as_deref().unwrap_or(""),
        version.as_deref().unwrap_or("")
    )
}

fn unpack_version(packed: &str) -> UpdateVersion {
    let (e, v) = packed.split_once(VERSION_SEP).unwrap_or((packed, ""));
    UpdateVersion {
        e_tag: (!e.is_empty()).then(|| e.to_string()),
        version: (!v.is_empty()).then(|| v.to_string()),
    }
}

/// ---------------------------- FaultInjectStorage ---------------------------
/// Crash-point injection for the chaos tests: delegates everything, but
/// aborts the whole process immediately *before* the Nth mutating operation.
/// Sweeping N over a scenario's write count exercises a crash in every gap of
/// the write protocol; recovery + `pypiron verify` then prove convergence.
pub struct FaultInjectStorage {
    inner: Arc<dyn Storage>,
    remaining: std::sync::atomic::AtomicI64,
}

impl FaultInjectStorage {
    pub fn new(inner: Arc<dyn Storage>, abort_after: i64) -> Self {
        Self {
            inner,
            remaining: std::sync::atomic::AtomicI64::new(abort_after),
        }
    }

    fn count_mutation(&self, op: &str, key: &str) {
        let left = self
            .remaining
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if left <= 0 {
            eprintln!("fault injection: aborting before {op} {key}");
            std::process::abort();
        }
    }
}

#[async_trait]
impl Storage for FaultInjectStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        self.inner.head_exists(key).await
    }
    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        self.inner.serve_artifact(key, range).await
    }
    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>> {
        self.inner.presign_get(key, expires).await
    }
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get_bytes(key).await
    }
    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        self.inner.list_dir_entries(dir_prefix).await
    }
    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.inner.list_all(prefix).await
    }
    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }
    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.get_with_etag(key).await
    }

    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        self.count_mutation("put_bytes", key);
        self.inner.put_bytes(key, bytes, content_type).await
    }
    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        self.count_mutation("put_if_absent", key);
        self.inner.put_if_absent(key, bytes, content_type).await
    }
    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool> {
        self.count_mutation("put_file_if_absent", key);
        self.inner.put_file_if_absent(key, path, content_type).await
    }
    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        self.count_mutation("delete_keys", keys.first().map_or("", String::as_str));
        self.inner.delete_keys(keys).await
    }
    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.count_mutation("put_if_none_match", key);
        self.inner.put_if_none_match(key, bytes).await
    }
    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.count_mutation("put_if_match", key);
        self.inner.put_if_match(key, etag, bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disk_list_all_walks_filters_and_detects_change() {
        let dir = std::env::temp_dir().join(format!("pypiron-listall-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let s = DiskStorage::new(&dir);
        s.put_bytes("packages/alpha/a-1.0.tar.gz", b"x".to_vec(), None)
            .await
            .unwrap();
        s.put_bytes(
            "packages/alpha/a-1.0.tar.gz.meta.json",
            b"{}".to_vec(),
            None,
        )
        .await
        .unwrap();
        s.put_bytes("packages/beta/b-1.0.tar.gz", b"y".to_vec(), None)
            .await
            .unwrap();

        let all = s.list_all("packages/").await.unwrap();
        assert_eq!(
            all.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
            [
                "packages/alpha/a-1.0.tar.gz",
                "packages/alpha/a-1.0.tar.gz.meta.json",
                "packages/beta/b-1.0.tar.gz",
            ]
        );

        // Sharded key prefix: only the matching first-level subtree.
        let shard = s.list_all("packages/a").await.unwrap();
        assert_eq!(shard.len(), 2);
        assert!(shard.iter().all(|o| o.key.starts_with("packages/alpha/")));
        assert!(s.list_all("packages/z").await.unwrap().is_empty());
        assert!(s.list_all("nope/").await.unwrap().is_empty());

        // Rewriting an object must change its etag.
        let before = all[0].etag.clone();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        s.put_bytes("packages/alpha/a-1.0.tar.gz", b"xx".to_vec(), None)
            .await
            .unwrap();
        let after = &s.list_all("packages/alpha/a-1.0.tar.gz").await.unwrap()[0];
        assert_ne!(before, after.etag);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_token_round_trips_etag_and_generation() {
        // Stores express a conditional update with different fields: S3/Azure
        // use the ETag, GCS the generation. The opaque token must carry both
        // back into an UpdateVersion unchanged.
        for (etag, version) in [
            (Some("\"abc123\"".to_string()), None), // S3 / Azure: ETag only
            (Some("\"xyz\"".to_string()), Some("17".to_string())), // GCS: ETag + generation
            (None, Some("42".to_string())),         // generation only
            (None, None),                           // neither
        ] {
            let token = pack_version(&etag, &version);
            let back = unpack_version(&token);
            assert_eq!(back.e_tag, etag);
            assert_eq!(back.version, version);
        }
        // Distinct inputs produce distinct tokens (fingerprint equality).
        assert_ne!(
            pack_version(&Some("a".into()), &None),
            pack_version(&None, &Some("a".into())),
        );
    }
}

/// Minimal in-memory Storage for unit tests across modules.
#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct InMemStorage {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        gets: AtomicUsize,
        fail_next_get: AtomicBool,
    }

    impl InMemStorage {
        pub fn insert(&self, key: &str, bytes: Vec<u8>) {
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
        }
        pub fn get_count(&self) -> usize {
            self.gets.load(Ordering::SeqCst)
        }
        pub fn fail_next_get(&self) {
            self.fail_next_get.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl Storage for InMemStorage {
        async fn head_exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        async fn serve_artifact(
            &self,
            _key: &str,
            _range: Option<&str>,
        ) -> Result<axum::response::Response<axum::body::Body>> {
            anyhow::bail!("serve_artifact not supported by InMemStorage")
        }
        async fn presign_get(
            &self,
            _key: &str,
            _expires: std::time::Duration,
        ) -> Result<Option<String>> {
            Ok(None)
        }
        async fn put_bytes(
            &self,
            key: &str,
            bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<()> {
            self.insert(key, bytes);
            Ok(())
        }
        async fn put_if_absent(
            &self,
            key: &str,
            bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bool> {
            let mut map = self.objects.lock().unwrap();
            if map.contains_key(key) {
                return Ok(false);
            }
            map.insert(key.to_string(), bytes);
            Ok(true)
        }
        async fn put_file_if_absent(
            &self,
            key: &str,
            path: &std::path::Path,
            content_type: Option<&str>,
        ) -> Result<bool> {
            let bytes = std::fs::read(path)?;
            self.put_if_absent(key, bytes, content_type).await
        }
        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            if self.fail_next_get.swap(false, Ordering::SeqCst) {
                anyhow::bail!("injected storage failure");
            }
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| NotFound(key.to_string()).into())
        }
        async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
            let map = self.objects.lock().unwrap();
            let mut out: Vec<FileEntry> = map
                .iter()
                .filter(|(k, _)| k.starts_with(dir_prefix) && !k[dir_prefix.len()..].contains('/'))
                .map(|(k, v)| FileEntry {
                    key: k.clone(),
                    size: v.len() as u64,
                    last_modified: Some("2026-01-01T00:00:00Z".to_string()),
                })
                .collect();
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        }
        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            let mut map = self.objects.lock().unwrap();
            for k in keys {
                map.remove(k);
            }
            Ok(())
        }
        async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
            let map = self.objects.lock().unwrap();
            let mut out: Vec<ObjectMeta> = map
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| ObjectMeta {
                    key: k.clone(),
                    size: v.len() as u64,
                    etag: test_etag(v),
                })
                .collect();
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        }
        fn supports_leases(&self) -> bool {
            true
        }
        async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|b| (b.clone(), test_etag(b))))
        }
        async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
            let mut map = self.objects.lock().unwrap();
            if map.contains_key(key) {
                return Ok(None);
            }
            let etag = test_etag(&bytes);
            map.insert(key.to_string(), bytes);
            Ok(Some(etag))
        }
        async fn put_if_match(
            &self,
            key: &str,
            etag: &str,
            bytes: Vec<u8>,
        ) -> Result<Option<String>> {
            let mut map = self.objects.lock().unwrap();
            match map.get(key) {
                Some(current) if test_etag(current) == etag => {
                    let new_etag = test_etag(&bytes);
                    map.insert(key.to_string(), bytes);
                    Ok(Some(new_etag))
                }
                _ => Ok(None),
            }
        }
    }

    fn test_etag(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }
}
