use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use std::path::{Component, Path, PathBuf};

use tokio::fs;

// S3 deps
use aws_sdk_s3::{primitives::ByteStream, Client as S3Client};

/// Object payload returned by storage backends.
pub struct ObjectData {
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
}

#[async_trait]
pub trait Storage: Send + Sync {
    /// Check if an object exists.
    async fn head_exists(&self, key: &str) -> Result<bool>;

    /// Write bytes to `key`. `content_type` is best-effort (ignored on Disk).
    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()>;

    /// Read object bytes + metadata.
    async fn get_bytes(&self, key: &str) -> Result<ObjectData>;

    /// List up to `limit` file keys under `prefix` (non-recursive where possible).
    async fn list_prefix_files_limited(&self, prefix: &str, limit: usize) -> Result<Vec<String>>;

    /// List immediate file names under the directory `dir_prefix` (non-recursive),
    /// returning full keys (dir_prefix + filename).
    async fn list_dir_files(&self, dir_prefix: &str) -> Result<Vec<String>>;

    /// List immediate child directory names under `dir_prefix` (without trailing slash).
    async fn list_dirs(&self, dir_prefix: &str) -> Result<Vec<String>>;

    /// Copy object from `src` to `dst` then delete `src`.
    async fn copy_then_delete(&self, src: &str, dst: &str) -> Result<()>;

    /// Delete multiple keys (best-effort).
    async fn delete_keys(&self, keys: &[String]) -> Result<()>;
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

    async fn get_bytes(&self, key: &str) -> Result<ObjectData> {
        let p = self.resolve(key)?;
        let bytes = fs::read(&p)
            .await
            .with_context(|| format!("read {}", key))?;
        Ok(ObjectData {
            content_length: Some(bytes.len() as u64),
            content_type: None, // no filesystem metadata — callers often override CT anyway
            bytes,
        })
    }

    async fn list_prefix_files_limited(&self, prefix: &str, limit: usize) -> Result<Vec<String>> {
        let dir = self.resolve(prefix)?;
        let mut out = Vec::new();
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                if out.len() >= limit {
                    break;
                }
                let md = entry.metadata().await?;
                if md.is_file() {
                    if let Some(name) = entry.file_name().to_str() {
                        out.push(format!("{}{}", prefix, name));
                    }
                }
            }
        }
        out.sort();
        if out.len() > limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    async fn list_dir_files(&self, dir_prefix: &str) -> Result<Vec<String>> {
        let dir = self.resolve(dir_prefix)?;
        let mut files = Vec::new();
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let md = entry.metadata().await?;
                if md.is_file() {
                    if let Some(name) = entry.file_name().to_str() {
                        files.push(format!("{}{}", dir_prefix, name));
                    }
                }
            }
        }
        files.sort();
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

    async fn copy_then_delete(&self, src: &str, dst: &str) -> Result<()> {
        let s = self.resolve(src)?;
        let d = self.resolve(dst)?;
        self.ensure_parent(&d).await?;
        fs::copy(&s, &d).await?;
        let _ = fs::remove_file(&s).await;
        Ok(())
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

    async fn list_all_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let mut token: Option<String> = None;
        let mut keys = Vec::new();
        loop {
            let mut req = self
                .s3
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(t) = token.take() {
                req = req.continuation_token(t);
            }
            let out = req.send().await?;
            for o in out.contents() {
                if let Some(k) = o.key() {
                    keys.push(k.to_string());
                }
            }
            if out.is_truncated().unwrap_or(false) {
                token = out.next_continuation_token.map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(keys)
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

    async fn get_bytes(&self, key: &str) -> Result<ObjectData> {
        let out = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        let ct = out.content_type().map(|s| s.to_string());
        let len = out.content_length().map(|v| v as u64);
        let bytes = out.body.collect().await?.to_vec();
        Ok(ObjectData {
            bytes,
            content_type: ct,
            content_length: len,
        })
    }

    async fn list_prefix_files_limited(&self, prefix: &str, limit: usize) -> Result<Vec<String>> {
        let out = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .max_keys(limit as i32)
            .send()
            .await?;
        let mut keys = Vec::new();
        for o in out.contents() {
            if let Some(k) = o.key() {
                // S3 objects with keys ending in "/" are not expected here; include files only.
                if !k.ends_with('/') {
                    keys.push(k.to_string());
                }
            }
        }
        Ok(keys)
    }

    async fn list_dir_files(&self, dir_prefix: &str) -> Result<Vec<String>> {
        let keys = self.list_all_objects(dir_prefix).await?;
        let mut out = Vec::new();
        for k in keys {
            if let Some(rest) = k.strip_prefix(dir_prefix) {
                if !rest.is_empty() && !rest.contains('/') {
                    out.push(k);
                }
            }
        }
        out.sort();
        Ok(out)
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

    async fn copy_then_delete(&self, src: &str, dst: &str) -> Result<()> {
        let src_uri = format!("{}/{}", self.bucket, src);
        self.s3
            .copy_object()
            .bucket(&self.bucket)
            .key(dst)
            .copy_source(src_uri)
            .send()
            .await?;
        let _ = self
            .s3
            .delete_object()
            .bucket(&self.bucket)
            .key(src)
            .send()
            .await;
        Ok(())
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
}
