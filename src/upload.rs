//! Upload spooling: stream a multipart body to a temp file, hashing as it
//! goes. Before this, uploads buffered the entire artifact in RAM — a
//! torch-class (900 MB) wheel OOM-killed a 2 GiB box. Memory is now bounded
//! by the multipart chunk size regardless of artifact size.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use md5::{Digest as _, Md5};
use sha2::{Digest, Sha256};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

static SPOOL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write buffer in front of the spool file. Network bodies arrive 8-16 KiB at a
/// time; batching them keeps each blocking-pool hop to a quarter-MiB write.
const SPOOL_WRITE_BUF: usize = 256 * 1024;

/// Temp-file path that cleans up after itself; survives every early-return
/// path of the upload handler without leaking spool files. It also keeps a
/// handle on the file that holds the spool's lock (see [`reap_stale_spools`])
/// for as long as the spool is in use.
pub struct TempPath {
    path: PathBuf,
    _lock: Option<std::fs::File>,
}

impl TempPath {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A spool file name, so the reaper only ever touches pypiron's own files.
const SPOOL_PREFIX: &str = "pypiron-upload-";
const SPOOL_SUFFIX: &str = ".spool";

/// How old an unowned spool must be before the reaper removes it. Far past any
/// live spool's life (the one-hour request deadline plus the store write), so
/// even a pypiron build that predates the spool lock, sharing the directory, is
/// safe.
const STALE_SPOOL_AGE: Duration = Duration::from_secs(24 * 3600);

/// Remove the spool files a crashed process left in `dir` and log how many.
/// Normal exits, errors and cancels remove their own; a crash (SIGKILL, OOM)
/// skips that. Touches only regular files named like a pypiron spool, more than
/// a day old, whose lock no live process holds: every spool's owner holds an
/// exclusive lock on it for its whole life and the kernel drops that lock when
/// the process dies, so a suspended or slow owner keeps its file. Best effort:
/// anything unreadable (another user's file in a shared /tmp) is left alone.
pub async fn reap_stale_spools(dir: PathBuf) {
    let reaped = tokio::task::spawn_blocking(move || reap_spools(&dir, STALE_SPOOL_AGE)).await;
    if let Ok(n @ 1..) = reaped {
        tracing::info!(removed = n, "removed upload spool files left by a crash");
    }
}

fn reap_spools(dir: &Path, min_age: Duration) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut reaped = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let ours = name
            .to_str()
            .is_some_and(|n| n.starts_with(SPOOL_PREFIX) && n.ends_with(SPOOL_SUFFIX));
        // A DirEntry's metadata never follows a symlink.
        let stale = entry.metadata().is_ok_and(|md| {
            md.is_file()
                && md
                    .modified()
                    .ok()
                    .and_then(|m| m.elapsed().ok())
                    .is_some_and(|age| age > min_age)
        });
        if !ours || !stale {
            continue;
        }
        let Ok(file) = std::fs::File::open(entry.path()) else {
            continue;
        };
        if file.try_lock().is_ok() && std::fs::remove_file(entry.path()).is_ok() {
            reaped += 1;
        }
    }
    reaped
}

pub struct UploadSpool {
    file: BufWriter<File>,
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
                "{SPOOL_PREFIX}{nanos}-{}-{seq}{SPOOL_SUFFIX}",
                std::process::id()
            ));
            let mut opts = OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            opts.mode(0o600);
            match opts.open(&path).await {
                Ok(file) => {
                    // Mark the spool live for its whole life (see
                    // reap_stale_spools): the lock rides on a duplicate handle in
                    // TempPath, which outlives this writer. A filesystem without
                    // locks only loses the reaper's liveness check.
                    let file = file.into_std().await;
                    let _ = file.try_lock();
                    let lock = file.try_clone().ok();
                    return Ok(Self {
                        file: BufWriter::with_capacity(SPOOL_WRITE_BUF, File::from_std(file)),
                        path: TempPath { path, _lock: lock },
                        hasher: Sha256::new(),
                        md5: Md5::new(),
                        size: 0,
                    });
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
        // Top the buffer up exactly, so every write reaching the file is a full
        // SPOOL_WRITE_BUF. tokio copies each write into a buffer of its own that
        // grows to fit; ragged flushes grew it to odd sizes up to double this.
        let mut rest = chunk;
        while !rest.is_empty() {
            let room = match SPOOL_WRITE_BUF - self.file.buffer().len() {
                0 => SPOOL_WRITE_BUF, // full: this write flushes it first
                room => room,
            };
            let (head, tail) = rest.split_at(room.min(rest.len()));
            self.file.write_all(head).await?;
            rest = tail;
        }
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

    /// Seal the spool. `durable` fsyncs it: required when the file may become
    /// stored truth (the disk backend hard-links it into the data dir), wasted
    /// when it is only a throwaway source for a network write that is deleted
    /// right after (the sync client, an object-store server).
    pub async fn finish(mut self, durable: bool) -> Result<FinishedSpool> {
        self.file.flush().await?;
        if durable {
            self.file.get_ref().sync_data().await?;
        }
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
        let payload: Vec<u8> = (0..300_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut expected = Sha256::new();
        expected.update(&payload);
        let expected = format!("{:x}", expected.finalize());
        let expected_md5 = crate::hash::hex(Md5::digest(&payload).as_slice());

        // Uneven chunk sizes, below, at and above the write buffer: hash, size
        // and bytes must not depend on chunking.
        for size in [7919, SPOOL_WRITE_BUF, SPOOL_WRITE_BUF * 3 / 2 + 1] {
            let mut spool = UploadSpool::new(&std::env::temp_dir()).await.unwrap();
            for chunk in payload.chunks(size) {
                spool.write_chunk(chunk).await.unwrap();
            }
            let done = spool.finish(true).await.unwrap();

            assert_eq!(done.sha256, expected);
            assert_eq!(done.md5, expected_md5);
            assert_eq!(done.size, payload.len() as u64);
            assert_eq!(std::fs::read(done.path.path()).unwrap(), payload);
        }
    }

    #[tokio::test]
    async fn temp_file_removed_on_drop() {
        let mut spool = UploadSpool::new(&std::env::temp_dir()).await.unwrap();
        spool.write_chunk(b"abc").await.unwrap();
        let done = spool.finish(true).await.unwrap();
        let path = done.path.path().to_path_buf();
        assert!(path.exists());
        drop(done);
        assert!(!path.exists(), "spool file must not leak");
    }

    #[tokio::test]
    async fn reaper_spares_a_live_spool_however_old() {
        let dir = std::env::temp_dir().join(format!("pypiron-reap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let old = SystemTime::now() - Duration::from_secs(2 * 24 * 3600);
        let age = |path: &Path| {
            let file = std::fs::File::options().write(true).open(path).unwrap();
            file.set_modified(old).unwrap();
        };
        let live = UploadSpool::new(&dir)
            .await
            .unwrap()
            .finish(false)
            .await
            .unwrap();
        age(live.path.path());
        // A crash's leftover: same name shape, nobody holding its lock.
        let leaked = dir.join(format!("{SPOOL_PREFIX}1-1-0{SPOOL_SUFFIX}"));
        std::fs::write(&leaked, b"x").unwrap();
        age(&leaked);

        assert_eq!(reap_spools(&dir, STALE_SPOOL_AGE), 1);
        assert!(
            live.path.path().exists(),
            "a live spool must survive the reaper"
        );
        assert!(!leaked.exists());
        drop(live);
        std::fs::remove_dir_all(&dir).unwrap();
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
