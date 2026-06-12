use anyhow::{anyhow, Context as _, Result};
use async_trait::async_trait;
use axum::body::Body;
use clap::{Args as ClapArgs, ValueEnum};
use http::{header, Response, StatusCode};
use std::io::SeekFrom;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

// S3 deps
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::{primitives::ByteStream, Client as S3Client};

/// Storage backend selection.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum StorageBackend {
    Disk,
    S3,
}

/// Storage configuration shared by `serve` and `sync` — one binary, one
/// storage layer, no second implementation.
#[derive(ClapArgs, Debug, Clone)]
pub struct StorageArgs {
    /// Storage backend to use: "disk" or "s3"
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

    async fn build_backend(&self) -> Result<Arc<dyn Storage>> {
        match self.storage {
            StorageBackend::Disk => {
                let data_dir = self.data_dir.clone().unwrap_or_else(|| {
                    std::env::var("HOME")
                        .map(|home| format!("{home}/.pypiron/packages"))
                        .unwrap_or_else(|_| "./.pypiron/packages".to_string())
                });
                Ok(Arc::new(DiskStorage::new(&data_dir)))
            }
            StorageBackend::S3 => {
                let bucket = self
                    .s3_bucket
                    .clone()
                    .ok_or_else(|| anyhow!("--s3-bucket is required when using --storage s3"))?;

                let mut cfg_loader = aws_config::defaults(BehaviorVersion::latest());
                if let Some(ref r) = self.aws_region {
                    cfg_loader = cfg_loader.region(Region::new(r.clone()));
                }
                let base_cfg = cfg_loader.load().await;

                let mut s3_cfg_builder = aws_sdk_s3::config::Builder::from(&base_cfg);
                if let Some(ref url) = self.s3_endpoint_url {
                    s3_cfg_builder = s3_cfg_builder.endpoint_url(url);
                }
                if self.s3_force_path_style {
                    s3_cfg_builder = s3_cfg_builder.force_path_style(true);
                }
                let s3 = aws_sdk_s3::Client::from_conf(s3_cfg_builder.build());
                Ok(Arc::new(S3Storage::new(s3, bucket)))
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

/// A single HTTP byte range resolved against a known size.
#[derive(Debug, PartialEq)]
pub enum RangeSpec {
    Full,
    Partial(u64, u64),
    Unsatisfiable,
}

/// Parse a single-range `Range` header. Multi-range and malformed headers
/// fall back to the full body (RFC 9110 lets a server ignore Range).
pub fn parse_range(header: Option<&str>, size: u64) -> RangeSpec {
    let Some(spec) = header.and_then(|h| h.strip_prefix("bytes=")) else {
        return RangeSpec::Full;
    };
    let spec = spec.trim();
    if spec.contains(',') {
        return RangeSpec::Full;
    }
    if let Some(suffix) = spec.strip_prefix('-') {
        // suffix range: the last N bytes
        let Ok(n) = suffix.parse::<u64>() else {
            return RangeSpec::Full;
        };
        if n == 0 || size == 0 {
            return RangeSpec::Unsatisfiable;
        }
        let n = n.min(size);
        return RangeSpec::Partial(size - n, size - 1);
    }
    let Some((start_s, end_s)) = spec.split_once('-') else {
        return RangeSpec::Full;
    };
    let Ok(start) = start_s.parse::<u64>() else {
        return RangeSpec::Full;
    };
    if start >= size {
        return RangeSpec::Unsatisfiable;
    }
    let end = if end_s.is_empty() {
        size - 1
    } else {
        match end_s.parse::<u64>() {
            Ok(e) if e >= start => e.min(size - 1),
            _ => return RangeSpec::Full,
        }
    };
    RangeSpec::Partial(start, end)
}

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

/// EXDEV ("invalid cross-device link") without pulling in the libc crate.
fn libc_exdev() -> i32 {
    18
}

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
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("bad path"))?;
        let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
        Ok(path.with_file_name(format!(".tmp-{nanos}-{}-{name}", std::process::id())))
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
        let created = match fs::hard_link(&tmp, &p).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        };
        let _ = fs::remove_file(&tmp).await;
        created
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
            Err(e) if e.raw_os_error() == Some(libc_exdev()) => {}
            Err(e) => return Err(anyhow::Error::from(e)),
        }
        let tmp = self.tmp_sibling(&p)?;
        fs::copy(path, &tmp).await?;
        let created = match fs::hard_link(&tmp, &p).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        };
        let _ = fs::remove_file(&tmp).await;
        created
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

/// ------------------------------ S3Storage --------------------------------
pub struct S3Storage {
    s3: S3Client,
    bucket: String,
}

/// Above this, uploads go as parallel multipart parts instead of one
/// sequential PUT — a 900 MB wheel went from ~12 s of single-stream S3 to
/// ~2-3 s with parts in flight.
const MULTIPART_THRESHOLD: u64 = 64 * 1024 * 1024;
const MULTIPART_PART_SIZE: u64 = 16 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 6;

impl S3Storage {
    pub fn new(s3: S3Client, bucket: String) -> Self {
        Self { s3, bucket }
    }

    /// Parallel multipart upload from a local file with a conditional
    /// complete (`If-None-Match: *`) — immutability holds exactly as it does
    /// for the single-PUT path, just decided at complete time. Any failure
    /// aborts the multipart upload so no invisible parts linger billable.
    async fn multipart_from_file(
        &self,
        key: &str,
        path: &std::path::Path,
        size: u64,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let mut create = self
            .s3
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key);
        if let Some(ct) = content_type {
            create = create.content_type(ct);
        }
        let upload_id = create
            .send()
            .await
            .context("create multipart upload")?
            .upload_id
            .ok_or_else(|| anyhow!("S3 returned no upload id"))?;

        let result = self.upload_parts(key, path, size, &upload_id).await;
        let completed = match result {
            Ok(parts) => {
                let mpu = aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build();
                match self
                    .s3
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(mpu)
                    .if_none_match("*")
                    .send()
                    .await
                {
                    Ok(_) => return Ok(true),
                    Err(e) if lost_conditional_write(&e) => Ok(false),
                    Err(e) => Err(anyhow::Error::from(e).context("complete multipart upload")),
                }
            }
            Err(e) => Err(e),
        };
        // Lost the immutability race or failed mid-flight: clean up the
        // invisible parts either way.
        let _ = self
            .s3
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        completed
    }

    /// Multipart from an in-memory body (sync's mirror path verifies the
    /// digest before writing, so the bytes are already resident). Parts are
    /// Bytes slices — zero-copy views into the one buffer.
    async fn multipart_from_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let mut create = self
            .s3
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key);
        if let Some(ct) = content_type {
            create = create.content_type(ct);
        }
        let upload_id = create
            .send()
            .await
            .context("create multipart upload")?
            .upload_id
            .ok_or_else(|| anyhow!("S3 returned no upload id"))?;

        let body = bytes::Bytes::from(bytes);
        let part_ranges: Vec<(i32, std::ops::Range<usize>)> = (0..)
            .map(|i| {
                let start = i * MULTIPART_PART_SIZE as usize;
                let end = (start + MULTIPART_PART_SIZE as usize).min(body.len());
                (i as i32 + 1, start..end)
            })
            .take_while(|(_, r)| !r.is_empty())
            .collect();

        let mut parts = Vec::with_capacity(part_ranges.len());
        let mut failed: Option<anyhow::Error> = None;
        for chunk in part_ranges.chunks(MULTIPART_CONCURRENCY) {
            let uploads = chunk.iter().map(|(part_number, range)| {
                let part = body.slice(range.clone());
                let upload_id = upload_id.as_str();
                async move {
                    let out = self
                        .s3
                        .upload_part()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(*part_number)
                        .body(ByteStream::from(part))
                        .send()
                        .await
                        .with_context(|| format!("upload part {part_number}"))?;
                    Ok::<_, anyhow::Error>(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(*part_number)
                            .set_e_tag(out.e_tag)
                            .build(),
                    )
                }
            });
            for part in futures::future::join_all(uploads).await {
                match part {
                    Ok(p) => parts.push(p),
                    Err(e) => failed = Some(e),
                }
            }
            if failed.is_some() {
                break;
            }
        }

        let completed = match failed {
            None => {
                let mpu = aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build();
                match self
                    .s3
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(mpu)
                    .if_none_match("*")
                    .send()
                    .await
                {
                    Ok(_) => return Ok(true),
                    Err(e) if lost_conditional_write(&e) => Ok(false),
                    Err(e) => Err(anyhow::Error::from(e).context("complete multipart upload")),
                }
            }
            Some(e) => Err(e),
        };
        let _ = self
            .s3
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        completed
    }

    async fn upload_parts(
        &self,
        key: &str,
        path: &std::path::Path,
        size: u64,
        upload_id: &str,
    ) -> Result<Vec<aws_sdk_s3::types::CompletedPart>> {
        let n_parts = size.div_ceil(MULTIPART_PART_SIZE);
        let mut parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::with_capacity(n_parts as usize);
        let part_numbers: Vec<i32> = (1..=n_parts as i32).collect();
        for chunk in part_numbers.chunks(MULTIPART_CONCURRENCY) {
            let uploads = chunk.iter().map(|&part_number| {
                let offset = (part_number as u64 - 1) * MULTIPART_PART_SIZE;
                let length = MULTIPART_PART_SIZE.min(size - offset);
                async move {
                    let body = ByteStream::read_from()
                        .path(path)
                        .offset(offset)
                        .length(aws_sdk_s3::primitives::Length::Exact(length))
                        .build()
                        .await
                        .context("open spool range")?;
                    let out = self
                        .s3
                        .upload_part()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(part_number)
                        .body(body)
                        .send()
                        .await
                        .with_context(|| format!("upload part {part_number}"))?;
                    Ok::<_, anyhow::Error>(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(part_number)
                            .set_e_tag(out.e_tag)
                            .build(),
                    )
                }
            });
            for part in futures::future::join_all(uploads).await {
                parts.push(part?);
            }
        }
        Ok(parts)
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        match self
            .s3
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            // HEAD has no body, so missing objects surface as generic 404s.
            Err(e) if is_s3_not_found(&e) => Ok(false),
            Err(e) => Err(anyhow::Error::from(e).context(format!("head {key}"))),
        }
    }

    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>> {
        let cfg = aws_sdk_s3::presigning::PresigningConfig::expires_in(expires)?;
        let req = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(cfg)
            .await?;
        Ok(Some(req.uri().to_string()))
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        let mut req = self.s3.get_object().bucket(&self.bucket).key(key);
        if let Some(r) = range {
            req = req.range(r);
        }
        let out = match req.send().await {
            Ok(out) => out,
            Err(e) => {
                // S3 validates ranges itself; surface its verdict.
                if e.code() == Some("InvalidRange") {
                    return Ok(Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .body(Body::empty())?);
                }
                if is_s3_not_found(&e) {
                    return Err(NotFound(key.to_string()).into());
                }
                return Err(e.into());
            }
        };

        let status = if out.content_range().is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        let mut builder = Response::builder()
            .status(status)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(
                header::CONTENT_TYPE,
                out.content_type()
                    .unwrap_or("application/octet-stream")
                    .to_string(),
            );
        if let Some(len) = out.content_length() {
            builder = builder.header(header::CONTENT_LENGTH, len);
        }
        if let Some(cr) = out.content_range() {
            builder = builder.header(header::CONTENT_RANGE, cr.to_string());
        }
        let body = Body::from_stream(ReaderStream::new(out.body.into_async_read()));
        Ok(builder.body(body)?)
    }

    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        let mut req = self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes));
        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }
        req.send().await?;
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let out = match self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => out,
            Err(e) if is_s3_not_found(&e) => return Err(NotFound(key.to_string()).into()),
            Err(e) => return Err(anyhow::Error::from(e).context(format!("get {key}"))),
        };
        Ok(out.body.collect().await?.to_vec())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        if bytes.len() as u64 > MULTIPART_THRESHOLD {
            // Same parallel-multipart path as spooled uploads: a single
            // sequential PUT capped sync's torch-class mirroring at ~1 Gbps.
            return self.multipart_from_bytes(key, bytes, content_type).await;
        }
        let mut req = self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(bytes));
        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }
        match req.send().await {
            Ok(_) => Ok(true),
            Err(e) if lost_conditional_write(&e) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let size = tokio::fs::metadata(path)
            .await
            .with_context(|| format!("stat upload spool {}", path.display()))?
            .len();
        if size > MULTIPART_THRESHOLD {
            return self
                .multipart_from_file(key, path, size, content_type)
                .await;
        }
        // The SDK streams the file body; nothing is buffered beyond its
        // internal chunks. Same conditional create as put_if_absent.
        let body = ByteStream::from_path(path)
            .await
            .with_context(|| format!("open upload spool {}", path.display()))?;
        let mut req = self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(body);
        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }
        match req.send().await {
            Ok(_) => Ok(true),
            Err(e) if lost_conditional_write(&e) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        let mut token: Option<String> = None;
        let mut entries = Vec::new();
        loop {
            let mut req = self
                .s3
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(dir_prefix);
            if let Some(t) = token.take() {
                req = req.continuation_token(t);
            }
            let out = req.send().await?;
            for o in out.contents() {
                let Some(k) = o.key() else { continue };
                let Some(rest) = k.strip_prefix(dir_prefix) else {
                    continue;
                };
                if rest.is_empty() || rest.contains('/') {
                    continue;
                }
                let last_modified = o
                    .last_modified()
                    .and_then(|dt| OffsetDateTime::from_unix_timestamp(dt.secs()).ok())
                    .and_then(|t| t.format(&Rfc3339).ok());
                entries.push(FileEntry {
                    key: k.to_string(),
                    size: o.size().unwrap_or(0).max(0) as u64,
                    last_modified,
                });
            }
            if out.is_truncated().unwrap_or(false) {
                token = out.next_continuation_token.map(|s| s.to_string());
            } else {
                break;
            }
        }
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        for k in keys {
            let _ = self
                .s3
                .delete_object()
                .bucket(&self.bucket)
                .key(k)
                .send()
                .await;
        }
        Ok(())
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        // No delimiter: every page carries 1,000 real keys, so a whole
        // corpus is keys/1000 requests instead of one request per directory.
        let mut token: Option<String> = None;
        let mut out = Vec::new();
        loop {
            let mut req = self
                .s3
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(t) = token.take() {
                req = req.continuation_token(t);
            }
            let page = req.send().await.context("list_all page")?;
            for o in page.contents() {
                let Some(k) = o.key() else { continue };
                out.push(ObjectMeta {
                    key: k.to_string(),
                    size: o.size().unwrap_or(0).max(0) as u64,
                    etag: o.e_tag().unwrap_or_default().to_string(),
                });
            }
            match page.next_continuation_token() {
                Some(t) => token = Some(t.to_string()),
                None => break,
            }
        }
        // ListObjectsV2 returns keys in UTF-8 binary order already.
        Ok(out)
    }

    fn supports_leases(&self) -> bool {
        true
    }

    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let out = match self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => out,
            Err(e) if e.code() == Some("NoSuchKey") => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let etag = out.e_tag().unwrap_or_default().to_string();
        let bytes = out.body.collect().await?.to_vec();
        Ok(Some((bytes, etag)))
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        match self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(bytes))
            .send()
            .await
        {
            Ok(out) => Ok(Some(out.e_tag().unwrap_or_default().to_string())),
            Err(e) if lost_conditional_write(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        match self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_match(etag)
            .body(ByteStream::from(bytes))
            .send()
            .await
        {
            Ok(out) => Ok(Some(out.e_tag().unwrap_or_default().to_string())),
            Err(e) if lost_conditional_write(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
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

/// Missing object: the SDK models GET misses as `NoSuchKey` and HEAD misses
/// as `NotFound`.
fn is_s3_not_found<E: ProvideErrorMetadata>(e: &E) -> bool {
    matches!(e.code(), Some("NoSuchKey") | Some("NotFound"))
}

/// A failed precondition or a concurrent conditional write: we lost, cleanly.
fn lost_conditional_write<E: ProvideErrorMetadata>(e: &E) -> bool {
    matches!(
        e.code(),
        Some("PreconditionFailed") | Some("ConditionalRequestConflict") | Some("NoSuchKey")
    )
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
    fn range_parsing() {
        use RangeSpec::*;
        assert_eq!(parse_range(None, 100), Full);
        assert_eq!(parse_range(Some("bytes=0-49"), 100), Partial(0, 49));
        assert_eq!(parse_range(Some("bytes=50-"), 100), Partial(50, 99));
        assert_eq!(parse_range(Some("bytes=-10"), 100), Partial(90, 99));
        // end clamps to size
        assert_eq!(parse_range(Some("bytes=0-1000"), 100), Partial(0, 99));
        // suffix larger than the file means the whole file
        assert_eq!(parse_range(Some("bytes=-1000"), 100), Partial(0, 99));
        // out of bounds start
        assert_eq!(parse_range(Some("bytes=100-"), 100), Unsatisfiable);
        assert_eq!(parse_range(Some("bytes=-0"), 100), Unsatisfiable);
        // ignorable: multi-range, malformed, non-byte units
        assert_eq!(parse_range(Some("bytes=0-1,5-9"), 100), Full);
        assert_eq!(parse_range(Some("bytes=junk"), 100), Full);
        assert_eq!(parse_range(Some("items=0-5"), 100), Full);
        assert_eq!(parse_range(Some("bytes=9-5"), 100), Full);
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
