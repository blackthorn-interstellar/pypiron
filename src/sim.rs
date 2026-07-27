//! Deterministic in-memory storage and fleet helpers for the verification
//! harnesses: the deterministic simulator's backend
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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use axum::body::Body;
use http::{header, Response, StatusCode};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::app::AppState;
use crate::buckets::{BucketHandle, BucketSet};
use crate::storage::{
    CopyOrigin, CopyOutcome, CopyProvider, FileEntry, NotFound, ObjectMeta, Storage,
};

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

/// Whether `key` addresses an artifact body rather than one of its companions:
/// exactly `packages/<pkg>/<filename>` with `<filename>` an artifact name.
fn is_canonical_artifact(key: &str) -> bool {
    key.strip_prefix("packages/")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(_, name)| !name.contains('/') && crate::sidecar::is_artifact(name))
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

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, Obj>,
    /// Every distinct body ever committed under each canonical
    /// `packages/<pkg>/<artifact>` key, as a sha256 hex digest — insert-only,
    /// so a delete does not erase it.
    ///
    /// A verification harness needs this because the final state cannot supply
    /// it and both proxies for it are erasable. An upload that crashed after
    /// its bytes landed and before its `200` never acked; a delete racing a
    /// merge freeze destroys the body before `freeze_side` can copy it to
    /// `_quarantine/`. The bytes were really there either way, under one
    /// immutable filename — which is the entire subject of a byte conflict —
    /// so the store simply remembers that they were. Artifact bodies only:
    /// sidecars, markers and views are rewritten constantly and nothing asks
    /// about their history.
    committed: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Per-storage version counter. Every successful write stamps `v{n}` and
    /// increments, so etags model an object store's opaque generation — one
    /// etag space shared by every conditional operation — rather than a content
    /// hash (where an ABA rewrite would revive a stale etag).
    next_version: u64,
}

/// A fault the sim injects into [`SimStorage::server_side_copy`], mirroring the
/// three real failure modes the replication transport ladder must survive:
/// a denied copy, a timed-out copy, and S3's phantom 200-with-error-body (the
/// verb reports success but writes nothing). Every one drives the ladder back to
/// streaming, so convergence is invariant to them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyFault {
    Denied,
    Timeout,
    /// Reports `Copied` but writes no bytes — the caller's post-copy size verify
    /// (or the boot probe's HEAD) catches it.
    Phantom,
}

/// Registry of copy-capable sim buckets by copy id. A server-side copy on the
/// destination has no access to the source bucket's `BTreeMap` (they are
/// independent, like two real buckets), so — exactly as a cloud provider bridges
/// two buckets it hosts — the destination looks the source up here and moves the
/// bytes. Test-only: only [`SimStorage::new_copy_source`] registers.
static SIM_COPY_REGISTRY: OnceLock<Mutex<HashMap<String, Weak<SimStorage>>>> = OnceLock::new();

fn sim_copy_registry() -> &'static Mutex<HashMap<String, Weak<SimStorage>>> {
    SIM_COPY_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Deterministic in-memory [`Storage`], modeled on
/// `storage::test_support::InMemStorage` but with versioned etags and
/// clock-stamped timestamps so it behaves like a real object store under the
/// conditional-write protocol.
pub struct SimStorage {
    clock: Arc<SimClock>,
    inner: Mutex<Inner>,
    /// This bucket's server-side-copy identity, when it participates in the copy
    /// transport (set by [`new_copy_source`](Self::new_copy_source)). `None`
    /// buckets never copy — the ladder streams, as in single-bucket mode.
    copy_id: Option<String>,
    /// A sticky fault applied to every [`server_side_copy`](Storage::server_side_copy).
    copy_fault: Mutex<Option<CopyFault>>,
}

impl SimStorage {
    pub fn new(clock: Arc<SimClock>) -> Arc<SimStorage> {
        Arc::new(SimStorage {
            clock,
            inner: Mutex::new(Inner::default()),
            copy_id: None,
            copy_fault: Mutex::new(None),
        })
    }

    /// A copy-capable sim bucket, registered under `copy_id` so a peer's
    /// server-side copy can find it. Ids must be unique within a run.
    pub fn new_copy_source(clock: Arc<SimClock>, copy_id: &str) -> Arc<SimStorage> {
        let this = Arc::new(SimStorage {
            clock,
            inner: Mutex::new(Inner::default()),
            copy_id: Some(copy_id.to_string()),
            copy_fault: Mutex::new(None),
        });
        let mut reg = sim_copy_registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        reg.retain(|_, weak| weak.strong_count() > 0);
        reg.insert(copy_id.to_string(), Arc::downgrade(&this));
        this
    }

    /// Inject (or clear, with `None`) the sticky copy fault for this bucket.
    pub fn set_copy_fault(&self, fault: Option<CopyFault>) {
        *self.copy_fault.lock().unwrap_or_else(|e| e.into_inner()) = fault;
    }

    /// Recover from a poisoned lock instead of panicking: a determinism run must
    /// surface as a failing invariant, never a torn-down process.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Write `bytes` at `key` with a fresh version and the current clock time.
    fn store(inner: &mut Inner, key: &str, bytes: Vec<u8>, now: OffsetDateTime) -> String {
        if is_canonical_artifact(key) {
            inner
                .committed
                .entry(key.to_string())
                .or_default()
                .insert(crate::hash::sha256_hex(&bytes));
        }
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

    /// The distinct byte-sets this bucket ever committed under one canonical
    /// artifact key, as sha256 hex digests. See [`Inner::committed`].
    pub fn committed_digests(&self, key: &str) -> std::collections::BTreeSet<String> {
        self.lock().committed.get(key).cloned().unwrap_or_default()
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

    fn copy_origin(&self) -> Option<CopyOrigin> {
        self.copy_id.as_ref().map(|id| CopyOrigin {
            // Real-cloud-like: provider S3 with no custom endpoint, so two sim
            // buckets read as a cross-region-eligible pair.
            provider: CopyProvider::S3,
            location: id.clone(),
            endpoint: None,
            account: None,
        })
    }

    async fn copy_credential_identity(&self) -> Result<Option<String>> {
        // All sim buckets share one identity, so any copy-capable pair passes
        // the static eligibility filter (the boot probe then verifies for real).
        Ok(self.copy_id.as_ref().map(|_| "sim".to_string()))
    }

    async fn server_side_copy(
        &self,
        src: &CopyOrigin,
        src_key: &str,
        dst_key: &str,
        _expected_size: u64,
    ) -> Result<CopyOutcome> {
        if self.copy_id.is_none() {
            return Ok(CopyOutcome::NotCopyable);
        }
        match *self.copy_fault.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(CopyFault::Denied) => bail!("sim server-side copy denied"),
            Some(CopyFault::Timeout) => bail!("sim server-side copy timed out"),
            // Report success but write nothing — the phantom 200-with-error-body.
            Some(CopyFault::Phantom) => return Ok(CopyOutcome::Copied),
            None => {}
        }
        let source = sim_copy_registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&src.location)
            .and_then(Weak::upgrade)
            .ok_or_else(|| anyhow!("sim copy source '{}' is not registered", src.location))?;
        let bytes = source.get_bytes(src_key).await?;
        self.put_bytes(dst_key, bytes, None).await?;
        Ok(CopyOutcome::Copied)
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

    #[test]
    fn only_artifact_bodies_are_history_tracked() {
        assert!(is_canonical_artifact("packages/p/p-1.0-py3-none-any.whl"));
        for companion in [
            ".meta.json",
            ".metadata",
            ".provenance",
            ".tombstone",
            ".frozen",
            ".mirror-quarantined",
        ] {
            let key = format!("packages/p/p-1.0-py3-none-any.whl{companion}");
            assert!(!is_canonical_artifact(&key), "{key} is not a body");
        }
        assert!(!is_canonical_artifact("packages/p/.origin"));
        assert!(!is_canonical_artifact("_quarantine/p/p-1.0.whl@abc"));
        assert!(!is_canonical_artifact("simple/p/index.html"));
    }

    #[tokio::test]
    async fn committed_digests_outlive_the_bytes() {
        // The whole point: a delete erases the object but not the fact that
        // those bytes were once committed under that name. Two byte-sets under
        // one filename is a byte conflict, and a merge freeze that races a
        // delete has no other way to prove it happened.
        let s = SimStorage::new(SimClock::new(start()));
        let key = "packages/p/p-1.0-py3-none-any.whl";
        s.put_bytes(key, b"one".to_vec(), None).await.unwrap();
        s.put_bytes(key, b"two".to_vec(), None).await.unwrap();
        s.put_bytes(&format!("{key}.meta.json"), b"{}".to_vec(), None)
            .await
            .unwrap();
        s.delete_keys(&[key.to_string()]).await.unwrap();

        assert!(s.get_bytes(key).await.is_err());
        assert_eq!(
            s.committed_digests(key),
            [
                crate::hash::sha256_hex(b"one"),
                crate::hash::sha256_hex(b"two")
            ]
            .into_iter()
            .collect()
        );
        // Companions keep no history, and neither does a key never written.
        assert!(s.committed_digests(&format!("{key}.meta.json")).is_empty());
        assert!(s.committed_digests("packages/p/other.whl").is_empty());
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
