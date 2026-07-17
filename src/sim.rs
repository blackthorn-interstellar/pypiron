//! Deterministic in-memory storage and fleet helpers for the verification
//! harnesses: the deterministic simulator's backend (dev/MOONSHOT.md rung 1)
//! and the conformance suites' bucket (rung 2).
//!
//! Always compiled — like [`crate::storage::FaultInjectStorage`], the existing
//! precedent for test infrastructure living in the product tree. The server
//! never constructs any of it, so the linker drops it from the shipped binary;
//! keeping it out of `#[cfg(test)]` lets the `examples/`, `tests/`, and model
//! binaries link it the same way the shipped code does.
//!
//! Everything here is deterministic on purpose: a `BTreeMap` (never a
//! `HashMap`/`RandomState`) for stable ordering, versioned etags instead of
//! content hashes, and timestamps read from a virtual [`SimClock`] rather than
//! the wall clock. Given the same operations it produces byte-identical states.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use axum::body::Body;
use http::{header, Response, StatusCode};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::app::AppState;
use crate::buckets::{BucketHandle, BucketSet};
use crate::storage::{FileEntry, NotFound, ObjectMeta, Storage};

/// An Arc-shareable virtual clock. Independent from the global override in
/// [`crate::clock`]: many `SimStorage`s can share one `SimClock` for consistent
/// timestamps, while the simulator additionally installs it into the global
/// seam (see [`SimClock::install_global`]) so the server's own protocol-path
/// clock reads follow the same virtual time.
pub struct SimClock {
    unix_nanos: AtomicU64,
    /// True once this instance owns the global override, so [`advance`] mirrors
    /// each step into [`crate::clock`].
    ///
    /// [`advance`]: SimClock::advance
    installed: AtomicBool,
}

impl SimClock {
    pub fn new(start: OffsetDateTime) -> Arc<SimClock> {
        Arc::new(SimClock {
            unix_nanos: AtomicU64::new(to_unix_nanos(start)),
            installed: AtomicBool::new(false),
        })
    }

    /// Move virtual time forward by `d`. Saturates at the `u64` nanos ceiling
    /// rather than wrapping. If this clock installed the global override, the
    /// new absolute time is pushed there too, keeping the server's protocol-path
    /// reads in lockstep with the harness.
    pub fn advance(&self, d: Duration) {
        let add = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        let new = self.unix_nanos.load(Ordering::Relaxed).saturating_add(add);
        self.unix_nanos.store(new, Ordering::Relaxed);
        if self.installed.load(Ordering::Relaxed) {
            crate::clock::set_sim_unix_nanos(new);
        }
    }

    pub fn now_utc(&self) -> OffsetDateTime {
        let nanos = self.unix_nanos.load(Ordering::Relaxed);
        OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH)
    }

    /// Current virtual time as RFC 3339 at whole-second precision.
    pub fn now_rfc3339(&self) -> String {
        let now = self.now_utc();
        now.replace_nanosecond(0)
            .unwrap_or(now)
            .format(&Rfc3339)
            .unwrap_or_default()
    }

    /// Enable the [`crate::clock`] global override at this clock's current time
    /// and mark this instance as its owner, so subsequent [`advance`] calls
    /// mirror into the global seam. The returned guard disables the override on
    /// drop.
    ///
    /// [`advance`]: SimClock::advance
    pub fn install_global(self: &Arc<Self>) -> crate::clock::SimClockGuard {
        self.installed.store(true, Ordering::Relaxed);
        crate::clock::enable_sim(self.now_utc())
    }
}

/// Clamp an `OffsetDateTime` to the representable Unix-nanos range as `u64`.
fn to_unix_nanos(t: OffsetDateTime) -> u64 {
    t.unix_timestamp_nanos().clamp(0, u64::MAX as i128) as u64
}

/// One stored object: bytes plus an object-store-style versioned etag and a
/// last-modified stamped from the [`SimClock`].
struct Obj {
    bytes: Vec<u8>,
    etag: String,
    last_modified: OffsetDateTime,
}

struct Inner {
    objects: BTreeMap<String, Obj>,
    /// Per-storage version counter. Every successful write stamps `v{n}` and
    /// increments, so etags model an object store's opaque generation — one
    /// etag space shared by every conditional operation — rather than a content
    /// hash (where an ABA rewrite would revive a stale etag).
    next_version: u64,
}

/// Deterministic in-memory [`Storage`], modeled on
/// `storage::test_support::InMemStorage` but with versioned etags and
/// clock-stamped timestamps so it behaves like a real object store under the
/// conditional-write protocol.
pub struct SimStorage {
    clock: Arc<SimClock>,
    inner: Mutex<Inner>,
}

impl SimStorage {
    pub fn new(clock: Arc<SimClock>) -> Arc<SimStorage> {
        Arc::new(SimStorage {
            clock,
            inner: Mutex::new(Inner {
                objects: BTreeMap::new(),
                next_version: 0,
            }),
        })
    }

    /// Recover from a poisoned lock instead of panicking: a determinism run must
    /// surface as a failing invariant, never a torn-down process.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Write `bytes` at `key` with a fresh version and the current clock time.
    fn store(inner: &mut Inner, key: &str, bytes: Vec<u8>, now: OffsetDateTime) -> String {
        let etag = format!("v{}", inner.next_version);
        inner.next_version += 1;
        inner.objects.insert(
            key.to_string(),
            Obj {
                bytes,
                etag: etag.clone(),
                last_modified: now,
            },
        );
        etag
    }

    /// Seed an object, stamping the clock time and a fresh version.
    pub fn insert(&self, key: &str, bytes: Vec<u8>) {
        let now = self.clock.now_utc();
        let mut inner = self.lock();
        Self::store(&mut inner, key, bytes, now);
    }

    /// Snapshot of key -> bytes, for invariant checks and abstraction functions.
    pub fn dump(&self) -> BTreeMap<String, Vec<u8>> {
        self.lock()
            .objects
            .iter()
            .map(|(k, o)| (k.clone(), o.bytes.clone()))
            .collect()
    }

    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }
}

#[async_trait]
impl Storage for SimStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        Ok(self.lock().objects.contains_key(key))
    }

    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        Ok(self.lock().objects.get(key).map(|o| o.bytes.len() as u64))
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        if range.is_some() {
            anyhow::bail!("SimStorage.serve_artifact does not support Range requests");
        }
        let bytes = self
            .lock()
            .objects
            .get(key)
            .map(|o| o.bytes.clone())
            .ok_or_else(|| anyhow::Error::from(NotFound(key.to_string())))?;
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(Body::from(bytes))?)
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
        let now = self.clock.now_utc();
        let mut inner = self.lock();
        Self::store(&mut inner, key, bytes, now);
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bool> {
        let now = self.clock.now_utc();
        let mut inner = self.lock();
        if inner.objects.contains_key(key) {
            return Ok(false);
        }
        Self::store(&mut inner, key, bytes, now);
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
        self.lock()
            .objects
            .get(key)
            .map(|o| o.bytes.clone())
            .ok_or_else(|| NotFound(key.to_string()).into())
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        let inner = self.lock();
        let entries = inner
            .objects
            .range(dir_prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(dir_prefix))
            .filter(|(k, _)| !k[dir_prefix.len()..].contains('/'))
            .map(|(k, o)| FileEntry {
                key: k.clone(),
                size: o.bytes.len() as u64,
                last_modified: o
                    .last_modified
                    .replace_nanosecond(0)
                    .unwrap_or(o.last_modified)
                    .format(&Rfc3339)
                    .ok(),
            })
            .collect();
        Ok(entries)
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let inner = self.lock();
        let out = inner
            .objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, o)| ObjectMeta {
                key: k.clone(),
                size: o.bytes.len() as u64,
                etag: o.etag.clone(),
            })
            .collect();
        Ok(out)
    }

    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let inner = self.lock();
        let out = inner
            .objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter(|(k, _)| after.is_none_or(|a| k.as_str() > a))
            .take(limit)
            .map(|(k, o)| ObjectMeta {
                key: k.clone(),
                size: o.bytes.len() as u64,
                etag: o.etag.clone(),
            })
            .collect();
        Ok(out)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        let mut inner = self.lock();
        for k in keys {
            inner.objects.remove(k);
        }
        Ok(())
    }

    fn supports_leases(&self) -> bool {
        true
    }

    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        Ok(self
            .lock()
            .objects
            .get(key)
            .map(|o| (o.bytes.clone(), o.etag.clone())))
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let now = self.clock.now_utc();
        let mut inner = self.lock();
        if inner.objects.contains_key(key) {
            return Ok(None);
        }
        Ok(Some(Self::store(&mut inner, key, bytes, now)))
    }

    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let now = self.clock.now_utc();
        let mut inner = self.lock();
        match inner.objects.get(key) {
            Some(current) if current.etag == etag => {
                Ok(Some(Self::store(&mut inner, key, bytes, now)))
            }
            _ => Ok(None),
        }
    }
}

/// A single-bucket [`AppState`] over `storage` — a thin, named wrapper over
/// [`AppState::headless`].
pub fn single_bucket_state(storage: Arc<dyn Storage>) -> AppState {
    AppState::headless(storage)
}

/// A multi-bucket [`AppState`] over the named `storages`, mirroring the
/// `two_bucket_state` test helper: headless over the first bucket, then the
/// bucket set replaced with every handle. `bucket_health` stays `None`, so
/// eligibility defaults open (every bucket usable) — the harness drives health
/// itself.
pub fn multi_bucket_state(storages: Vec<(String, Arc<dyn Storage>)>) -> AppState {
    let handles: Vec<BucketHandle> = storages
        .into_iter()
        .map(|(name, storage)| BucketHandle { storage, name })
        .collect();
    let first = handles
        .first()
        .map(|h| h.storage.clone())
        .expect("multi_bucket_state requires at least one bucket");
    let mut state = AppState::headless(first);
    state.buckets = Arc::new(BucketSet::new(handles));
    state
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start() -> OffsetDateTime {
        // 2026-01-01T00:00:00Z
        OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap()
    }

    #[tokio::test]
    async fn cas_versions_advance_and_gate() {
        let s = SimStorage::new(SimClock::new(start()));

        // put_if_none_match creates the object once, at v0.
        assert_eq!(
            s.put_if_none_match("k", b"a".to_vec()).await.unwrap(),
            Some("v0".to_string())
        );
        // A second create loses to the existing object.
        assert!(s
            .put_if_none_match("k", b"b".to_vec())
            .await
            .unwrap()
            .is_none());

        // put_if_match only succeeds against the current etag.
        assert!(s
            .put_if_match("k", "v0-stale", b"c".to_vec())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            s.put_if_match("k", "v0", b"c".to_vec()).await.unwrap(),
            Some("v1".to_string())
        );

        // Etags advance v0, v1, ... across the one shared space.
        let (bytes, etag) = s.get_with_etag("k").await.unwrap().unwrap();
        assert_eq!(bytes, b"c");
        assert_eq!(etag, "v1");
    }

    #[tokio::test]
    async fn last_modified_follows_sim_clock() {
        let clock = SimClock::new(start());
        let s = SimStorage::new(clock.clone());

        s.insert("_dirty/a!n.intent", Vec::new());
        clock.advance(Duration::from_secs(10));
        s.insert("_dirty/b!n.intent", Vec::new());

        let entries = s.list_dir_entries("_dirty/").await.unwrap();
        assert_eq!(entries.len(), 2);
        let ta = entries[0].last_modified.as_deref().unwrap();
        let tb = entries[1].last_modified.as_deref().unwrap();
        assert_ne!(ta, tb);
        // Both are RFC 3339 and exactly 10s apart, tracking the clock advance.
        let pa = OffsetDateTime::parse(ta, &Rfc3339).unwrap();
        let pb = OffsetDateTime::parse(tb, &Rfc3339).unwrap();
        assert_eq!(pb - pa, time::Duration::seconds(10));
    }

    // The grace path (worker::consumable_dirty_work classifying an unpaired
    // intent) becomes driveable purely by advancing the virtual clock: no
    // sleeps, no wall time.
    #[tokio::test]
    async fn grace_path_is_driveable_by_advancing_the_clock() {
        let clock = SimClock::new(start());
        let s = SimStorage::new(clock.clone());
        let grace = time::Duration::seconds(900); // 15 minutes

        // An intent marker with no paired commit, written at T0.
        s.insert("_dirty/pkg!n1.intent", Vec::new());
        let entries = s.list_dir_entries("_dirty/").await.unwrap();

        // Just after T0: a fresh unpaired intent defers the whole package, so
        // there is no consumable work yet.
        let fresh = crate::worker::consumable_dirty_work(
            &entries,
            clock.now_utc() + time::Duration::seconds(1),
            grace,
        );
        assert!(fresh.is_empty());

        // 20 minutes on: the intent is past grace and classified stale.
        clock.advance(Duration::from_secs(20 * 60));
        let stale = crate::worker::consumable_dirty_work(&entries, clock.now_utc(), grace);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].stale_intents, 1);
    }
}
