//! Upload spooling: stream a multipart body to a temp file, hashing as it
//! goes. Before this, uploads buffered the entire artifact in RAM — a
//! torch-class (900 MB) wheel OOM-killed a 2 GiB box. Memory is now bounded
//! by the multipart chunk size regardless of artifact size.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use md5::{Digest as _, Md5};
use sha2::{Digest, Sha256};
use tokio::fs::{File, OpenOptions};
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
    /// Content MD5, computed in the same streaming pass. Not a security digest —
    /// it is the checksum S3 reports as a single-part ETag and GCS as `md5Hash`,
    /// captured so a later server-side copy can be verified against the
    /// provider's own reported digest (see [`crate::sidecar::StoreChecksum`]).
    md5: Md5,
    size: u64,
}

/// A fully spooled upload: temp file on disk (removed on drop), its SHA-256, its
/// content MD5 (for the server-side-copy checksum), and its size.
pub struct FinishedSpool {
    pub path: TempPath,
    pub sha256: String,
    pub md5: String,
    pub size: u64,
}

impl UploadSpool {
    /// Create a fresh spool file. The name carries wall-nanos + pid + a
    /// process-local counter (the house idiom, cf. `markers::marker_nonce`) so it
    /// is unique and unpredictable without a CSPRNG dependency. The file is opened
    /// `O_CREAT|O_EXCL` mode 0600: exclusive creation refuses to follow a
    /// pre-planted symlink or reuse an existing inode (killing the shared-tmp
    /// symlink-truncate vector), and owner-only mode keeps private package bytes
    /// unreadable to co-tenant users — a property the disk backend then carries
    /// into the stored artifact, which hard-links this inode. A name that already
    /// exists (a crashed run's leak after pid recycling, or a local attacker
    /// pre-creating the path) is handled by re-deriving and retrying, so exclusive
    /// creation can never wedge uploads.
    pub async fn new(dir: &Path) -> Result<Self> {
        let mut last_err = None;
        for _ in 0..8 {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let seq = SPOOL_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = dir.join(format!(
                "pypiron-upload-{nanos}-{}-{seq}.spool",
                std::process::id()
            ));
            let mut opts = OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            opts.mode(0o600);
            match opts.open(&path).await {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        path: TempPath(path),
                        hasher: Sha256::new(),
                        md5: Md5::new(),
                        size: 0,
                    })
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(anyhow::Error::from(e).context("open upload spool")),
            }
        }
        Err(match last_err {
            Some(e) => anyhow::Error::from(e).context("open upload spool: name kept colliding"),
            None => anyhow::anyhow!("open upload spool: no attempt made"),
        })
    }

    pub async fn write_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        self.hasher.update(chunk);
        self.md5.update(chunk);
        self.file.write_all(chunk).await?;
        self.size += chunk.len() as u64;
        Ok(())
    }

    /// Bytes written so far — lets a streaming caller enforce a size cap
    /// mid-download instead of after the whole body has landed.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The spool file itself, for a reader that wants to follow the bytes as
    /// they land (the proxy's streaming tee opens its own handle on this path).
    pub fn path(&self) -> &Path {
        self.path.path()
    }

    /// Hand every buffered byte to the OS so a second open handle can read them.
    /// `write_chunk` returns as soon as tokio has queued the write, so a reader
    /// following [`size`](Self::size) would otherwise race ahead of the file.
    /// Not an fsync — durability across a crash is still the rename's job.
    pub async fn flush(&mut self) -> Result<()> {
        self.file.flush().await?;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<FinishedSpool> {
        self.file.flush().await?;
        self.file.sync_data().await?;
        Ok(FinishedSpool {
            path: self.path,
            sha256: format!("{:x}", self.hasher.finalize()),
            md5: crate::hash::hex(self.md5.finalize().as_slice()),
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
        let expected_md5 = crate::hash::hex(Md5::digest(&payload).as_slice());

        let mut spool = UploadSpool::new(&std::env::temp_dir()).await.unwrap();
        // Uneven chunk sizes: hash and size must not depend on chunking.
        for chunk in payload.chunks(7919) {
            spool.write_chunk(chunk).await.unwrap();
        }
        let done = spool.finish().await.unwrap();

        assert_eq!(done.sha256, expected);
        assert_eq!(done.md5, expected_md5);
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
