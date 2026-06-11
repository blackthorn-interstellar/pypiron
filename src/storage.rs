use anyhow::{anyhow, Context, Result};
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

/// A file from a directory listing, with the metadata index rendering needs.
pub struct FileEntry {
    pub key: String,
    pub size: u64,
    /// RFC 3339 last-modified timestamp (serves as PEP 700 upload-time).
    pub last_modified: Option<String>,
}

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

    /// Read full object bytes (indexes, sidecars — small files only).
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>>;

    /// List immediate file entries under the directory `dir_prefix` (non-recursive),
    /// returning full keys (dir_prefix + filename) with size and last-modified.
    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>>;

    /// List immediate child directory names under `dir_prefix` (without trailing slash).
    async fn list_dirs(&self, dir_prefix: &str) -> Result<Vec<String>>;

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
        let md = fs::metadata(&path)
            .await
            .with_context(|| format!("stat {key}"))?;
        if !md.is_file() {
            return Err(anyhow!("not a file: {key}"));
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
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        fs::write(p, bytes).await?;
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let p = self.resolve(key)?;
        Ok(fs::read(&p)
            .await
            .with_context(|| format!("read {}", key))?)
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        let dir = self.resolve(dir_prefix)?;
        let mut files = Vec::new();
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
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
        }
        files.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(files)
    }

    async fn list_dirs(&self, dir_prefix: &str) -> Result<Vec<String>> {
        let dir = self.resolve(dir_prefix)?;
        let mut dirs = Vec::new();
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let md = entry.metadata().await?;
                if md.is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        dirs.push(name.to_string());
                    }
                }
            }
        }
        dirs.sort();
        Ok(dirs)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        for k in keys {
            if let Ok(p) = self.resolve(k) {
                let _ = fs::remove_file(p).await;
            }
        }
        Ok(())
    }
}

/// ------------------------------ S3Storage --------------------------------
pub struct S3Storage {
    s3: S3Client,
    bucket: String,
}

impl S3Storage {
    pub fn new(s3: S3Client, bucket: String) -> Self {
        Self { s3, bucket }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        Ok(self
            .s3
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .is_ok())
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
        let out = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(out.body.collect().await?.to_vec())
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

    async fn list_dirs(&self, dir_prefix: &str) -> Result<Vec<String>> {
        let out = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(dir_prefix)
            .delimiter("/")
            .send()
            .await?;
        let mut dirs = Vec::new();
        for cp in out.common_prefixes() {
            if let Some(p) = cp.prefix() {
                if let Some(name) = p.strip_prefix(dir_prefix).and_then(|s| s.strip_suffix('/')) {
                    dirs.push(name.to_string());
                }
            }
        }
        dirs.sort();
        Ok(dirs)
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
