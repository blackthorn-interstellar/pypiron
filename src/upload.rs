//! Upload spooling: stream a multipart body to a temp file, hashing as it
//! goes. Before this, uploads buffered the entire artifact in RAM — a
//! torch-class (900 MB) wheel OOM-killed a 2 GiB box. Memory is now bounded
//! by the multipart chunk size regardless of artifact size.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

static SPOOL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Temp-file path that cleans up after itself; survives every early-return
/// path of the upload handler without leaking spool files.
pub struct TempPath(PathBuf);

impl TempPath {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub struct UploadSpool {
    file: File,
    path: TempPath,
    hasher: Sha256,
    size: u64,
}

/// A fully spooled upload: temp file on disk (removed on drop), its SHA-256,
/// and its size.
pub struct FinishedSpool {
    pub path: TempPath,
    pub sha256: String,
    pub size: u64,
}

impl UploadSpool {
    pub async fn new(dir: &Path) -> Result<Self> {
        let name = format!(
            "pypiron-upload-{}-{}.spool",
            std::process::id(),
            SPOOL_COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        let path = dir.join(name);
        let file = File::create(&path).await?;
        Ok(Self {
            file,
            path: TempPath(path),
            hasher: Sha256::new(),
            size: 0,
        })
    }

    pub async fn write_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        self.hasher.update(chunk);
        self.file.write_all(chunk).await?;
        self.size += chunk.len() as u64;
        Ok(())
    }

    /// Bytes written so far — lets a streaming caller enforce a size cap
    /// mid-download instead of after the whole body has landed.
    pub fn size(&self) -> u64 {
        self.size
    }

    pub async fn finish(mut self) -> Result<FinishedSpool> {
        self.file.flush().await?;
        self.file.sync_data().await?;
        Ok(FinishedSpool {
            path: self.path,
            sha256: format!("{:x}", self.hasher.finalize()),
            size: self.size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn chunked_spool_matches_whole_file_hash() {
        let payload: Vec<u8> = (0..100_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut expected = Sha256::new();
        expected.update(&payload);
        let expected = format!("{:x}", expected.finalize());

        let mut spool = UploadSpool::new(&std::env::temp_dir()).await.unwrap();
        // Uneven chunk sizes: hash and size must not depend on chunking.
        for chunk in payload.chunks(7919) {
            spool.write_chunk(chunk).await.unwrap();
        }
        let done = spool.finish().await.unwrap();

        assert_eq!(done.sha256, expected);
        assert_eq!(done.size, payload.len() as u64);
        assert_eq!(std::fs::read(done.path.path()).unwrap(), payload);
    }

    #[tokio::test]
    async fn temp_file_removed_on_drop() {
        let mut spool = UploadSpool::new(&std::env::temp_dir()).await.unwrap();
        spool.write_chunk(b"abc").await.unwrap();
        let done = spool.finish().await.unwrap();
        let path = done.path.path().to_path_buf();
        assert!(path.exists());
        drop(done);
        assert!(!path.exists(), "spool file must not leak");
    }

    #[tokio::test]
    async fn early_drop_cleans_up_unfinished_spool() {
        let mut spool = UploadSpool::new(&std::env::temp_dir()).await.unwrap();
        spool.write_chunk(b"partial").await.unwrap();
        let path = spool.path.path().to_path_buf();
        assert!(path.exists());
        drop(spool); // simulates any early-return in the upload handler
        assert!(!path.exists(), "abandoned spool file must not leak");
    }
}
