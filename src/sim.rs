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

/// `(pkg, filename)` when `key` addresses an artifact body rather than one of
/// its companions: exactly `packages/<pkg>/<filename>` with `<filename>` an
/// artifact name.
fn split_artifact_key(key: &str) -> Option<(&str, &str)> {
    let (pkg, name) = key.strip_prefix("packages/")?.split_once('/')?;
    (!name.contains('/') && crate::sidecar::is_artifact(name)).then_some((pkg, name))
}

/// Whether `key` addresses an artifact body rather than one of its companions.
fn is_canonical_artifact(key: &str) -> bool {
    split_artifact_key(key).is_some()
}

/// Whether this bucket bars an upload of `key`'s filename right now: a
/// `.tombstone` or `.frozen` beside it, the two fences `publish_record` refuses
/// over. `.mirror-quarantined` is deliberately not one — handing the filename
/// to private truth *is* a demotion's intended resolution, so it leaves the
/// canonical key writable (see `settle_mirror_quarantine`).
fn upload_barred(inner: &Inner, key: &str) -> bool {
    inner
        .objects
        .contains_key(&crate::sidecar::tombstone_key(key))
        || inner.objects.contains_key(&crate::sidecar::frozen_key(key))
}

/// Whether any durable fence stands beside `key` — the three markers a delete
/// or a merge resolution plants *before* it drops the body it is retiring.
fn fence_stands(inner: &Inner, key: &str) -> bool {
    upload_barred(inner, key)
        || inner
            .objects
            .contains_key(&crate::sidecar::mirror_quarantined_key(key))
}

/// Whether anything durable authorizes removing `key`'s body, given the bytes
/// being removed — the predicate behind UNTYPED_DISAPPEARANCE.
///
/// Exactly two things authorize it, and the presence of a marker is only one of
/// them:
///
/// * the FILENAME is durably closed. `.tombstone` and `.frozen` both make
///   `publish_record` refuse ([`upload_barred`]), so no writer can have swapped
///   the bytes under the marker between the adjudication and this delete: what
///   is going away is what the fence judged.
/// * the BYTES are durably kept. `_quarantine/<pkg>/<file>@<sha12>` holds a
///   copy of exactly these bytes, so a later pass can still find them.
///
/// `.mirror-quarantined` is deliberately NOT sufficient, even though
/// [`fence_stands`] counts it: handing the filename to private truth IS a
/// demotion's intended resolution, so that marker is the one fence in the
/// system that leaves the canonical key writable (`settle_mirror_quarantine`,
/// src/replicate.rs). It can therefore stand over bytes it never adjudicated —
/// a private publish landing in the window — and a settle that then deleted
/// blind destroyed acked bytes held in no `_quarantine/` copy, under no
/// tombstone and no `.frozen`. Measured twice, on pinned seeds 86001009016 and
/// 40000042940. The product earns that delete by holding a verified copy of the
/// bytes actually standing there, so this asks for the copy, not the marker.
fn removal_authorized(inner: &Inner, key: &str, removed: &[u8]) -> bool {
    if upload_barred(inner, key) {
        return true;
    }
    split_artifact_key(key).is_some_and(|(pkg, filename)| {
        let qkey =
            crate::replicate::quarantine_key(pkg, filename, &crate::hash::sha256_hex(removed));
        inner.objects.contains_key(&qkey)
    })
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
    /// The distinct bodies that stood at each canonical
    /// `packages/<pkg>/<artifact>` key during the filename's CURRENT
    /// incarnation, as sha256 hex digests.
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
    ///
    /// *Incarnation*, not lifetime, and the distinction is the whole currency.
    /// The only question this set exists to answer is whether two byte-sets
    /// COEXISTED under one immutable filename — `decide` reaches
    /// `Verdict::Freeze` from two live records at the same instant, never from
    /// two that took the key in turn. So two rules bound it, each drawn from
    /// the product's own:
    ///
    /// - a body written while [`upload_barred`] never joins. `publish_record`
    ///   refuses over a `.tombstone`/`.frozen`, so those bytes were never a
    ///   record this bucket published — they are a refused writer's debris,
    ///   until something contests them ([`Inner::contested`]).
    /// - deleting the body with NO fence beside it clears the set. That is a
    ///   mirror cache eviction (never tombstoned, re-fillable by design) or a
    ///   rollback; either way the name is retired with nothing preserved, and
    ///   the next body starts a new incarnation.
    ///
    /// A delete UNDER a fence deliberately keeps the history: `freeze_side`,
    /// `settle_mirror_quarantine` and `delete_record` all plant their marker
    /// before they drop the body, so a fenced delete is a resolution, and the
    /// bodies it destroys are exactly the evidence a racing delete would
    /// otherwise erase. Clearing there would put back the false FREEZE_
    /// UNJUSTIFIED reds this set was added to remove.
    committed: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// The debris that stopped being debris: bodies a filename fence refused
    /// which another writer then LOST the immutable filename to, over the same
    /// incarnation. Recorded apart from [`Inner::committed`] because whether
    /// they are evidence is the CALLER's question, and only the caller knows
    /// its fleet:
    ///
    /// - single-bucket, `publish_record` deletes what it wrote the moment the
    ///   fence refuses it (`src/publish.rs`), and no merge runs at all, so the
    ///   loser's 409 is the end of it.
    /// - multi-bucket, that same call deliberately leaves the refused body: "a
    ///   fenced multi-bucket loser stays occupied and inert", because deleting
    ///   by key could erase a private replacement that landed after the
    ///   writer's cross-object read. The writer that loses the create there is
    ///   a replication copy leg: it reads the standing body back, finds bytes
    ///   its own sidecar does not name, and freezes both sides
    ///   (`replicate::freeze_copy_race`). Two byte-sets under one immutable
    ///   filename — and routinely the only trace of them, since the refused
    ///   publish never published a sidecar, so no ack names those bytes and
    ///   `freeze_side` quarantines the body it kept, not the one it lost.
    contested: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Canonical artifact bodies this bucket has removed, and the ones it freed
    /// with NOTHING standing to authorize the removal. See
    /// [`SimStorage::body_removals`] — the pair is one oracle's numerator and
    /// denominator, recorded here because the final state cannot supply either.
    body_removals: u64,
    untyped_disappearances: Vec<String>,
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
        if is_canonical_artifact(key) && !upload_barred(inner, key) {
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

    /// A conditional create that LOST: another writer went for this immutable
    /// filename and the body standing at `key` held it. A standing body no
    /// [`Inner::committed`] entry names is one a filename fence refused, and
    /// losing to it is exactly the moment it stops being debris — see
    /// [`Inner::contested`].
    fn create_lost(inner: &mut Inner, key: &str) {
        if !is_canonical_artifact(key) {
            return;
        }
        let Some(standing) = inner
            .objects
            .get(key)
            .map(|o| crate::hash::sha256_hex(&o.bytes))
        else {
            return;
        };
        if !inner
            .committed
            .get(key)
            .is_some_and(|published| published.contains(&standing))
        {
            inner
                .contested
                .entry(key.to_string())
                .or_default()
                .insert(standing);
        }
    }

    /// Seed an object, stamping the clock time and a fresh version.
    pub fn insert(&self, key: &str, bytes: Vec<u8>) {
        let now = self.clock.now_utc();
        let mut inner = self.lock();
        Self::store(&mut inner, key, bytes, now);
    }

    /// The distinct byte-sets that stood at one canonical artifact key during
    /// the filename's current incarnation, as sha256 hex digests. See
    /// [`Inner::committed`].
    pub fn committed_digests(&self, key: &str) -> std::collections::BTreeSet<String> {
        self.lock().committed.get(key).cloned().unwrap_or_default()
    }

    /// The fence-refused byte-sets another writer lost this filename to, over
    /// the same incarnation. Evidence only for a caller whose fleet has peers —
    /// see [`Inner::contested`].
    pub fn contested_digests(&self, key: &str) -> std::collections::BTreeSet<String> {
        self.lock().contested.get(key).cloned().unwrap_or_default()
    }

    /// How many canonical artifact bodies this bucket removed, and which of
    /// them it freed with nothing standing to authorize it — the filename not
    /// closed by a `.tombstone` or a `.frozen`, and the bytes in no
    /// `_quarantine/` copy. See [`removal_authorized`].
    ///
    /// The predicate is evaluated at the instant of the delete, not at the end,
    /// which is the whole point: an immutable filename freed in the middle of a
    /// run is corrupt *then*, and the final state cannot show it. A racing
    /// rebuild publishes a sidecar over the empty key, the next upload wins the
    /// create with different bytes, and the bucket serves body B under body A's
    /// published sha256 — permanently, because nothing re-hashes a stored body.
    /// Watching the free itself is what turns that into a depth-1 defect.
    pub fn body_removals(&self) -> (u64, Vec<String>) {
        let inner = self.lock();
        (inner.body_removals, inner.untyped_disappearances.clone())
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
            Self::create_lost(&mut inner, key);
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
            let Some(removed) = inner.objects.remove(k) else {
                continue;
            };
            if !is_canonical_artifact(k) {
                continue;
            }
            // Every body removal is a question the harness gets to ask, whether
            // or not the answer turns out to be wrong: the reach denominator.
            inner.body_removals += 1;
            if !removal_authorized(&inner, k, &removed.bytes) {
                // Nothing closed the filename and nothing kept the bytes, so
                // an immutable name was freed with no record that those bytes
                // ever stood there.
                inner.untyped_disappearances.push(k.clone());
            }
            if fence_stands(&inner, k) {
                continue;
            }
            // An unfenced body delete retires the filename with nothing
            // preserved, so the next body under it is a new incarnation and
            // cannot have conflicted with this one. See `Inner::committed`.
            inner.committed.remove(k);
            inner.contested.remove(k);
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

    async fn head_etag(&self, key: &str) -> Result<Option<String>> {
        Ok(self.lock().objects.get(key).map(|o| o.etag.clone()))
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let now = self.clock.now_utc();
        let mut inner = self.lock();
        if inner.objects.contains_key(key) {
            Self::create_lost(&mut inner, key);
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
            Some(CopyFault::Phantom) => return Ok(CopyOutcome::Copied { checksum: None }),
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
        Ok(CopyOutcome::Copied { checksum: None })
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

    const BODY_KEY: &str = "packages/p/p-1.0-py3-none-any.whl";

    fn digests(bodies: &[&[u8]]) -> std::collections::BTreeSet<String> {
        bodies.iter().map(|b| crate::hash::sha256_hex(b)).collect()
    }

    #[tokio::test]
    async fn a_fenced_delete_keeps_the_bodies_it_destroys() {
        // The whole point: a fence is a *resolution* — `freeze_side` plants
        // `.frozen` before it drops the losing body — so the delete erases the
        // object but not the fact that both byte-sets stood under that name.
        // Two coexisting byte-sets under one immutable filename IS the byte
        // conflict, and a merge freeze that races a delete has no other way to
        // prove it happened. Same for a tombstone and for a demotion fence.
        for fence in [".frozen", ".tombstone", ".mirror-quarantined"] {
            let s = SimStorage::new(SimClock::new(start()));
            s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
            s.put_bytes(BODY_KEY, b"two".to_vec(), None).await.unwrap();
            s.put_bytes(&format!("{BODY_KEY}.meta.json"), b"{}".to_vec(), None)
                .await
                .unwrap();
            s.insert(&format!("{BODY_KEY}{fence}"), b"{}".to_vec());
            s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();

            assert!(s.get_bytes(BODY_KEY).await.is_err());
            assert_eq!(
                s.committed_digests(BODY_KEY),
                digests(&[b"one", b"two"]),
                "{fence} is a resolution, not a retirement"
            );
        }
        // Companions keep no history, and neither does a key never written.
        let s = SimStorage::new(SimClock::new(start()));
        s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
        assert!(s
            .committed_digests(&format!("{BODY_KEY}.meta.json"))
            .is_empty());
        assert!(s.committed_digests("packages/p/other.whl").is_empty());
    }

    #[tokio::test]
    async fn an_unfenced_delete_ends_the_incarnation() {
        // A mirror cache eviction is never tombstoned — the filename stays
        // re-fillable by design (`delete_record`) — so a name can hold body A,
        // be evicted, and later hold body B with nothing ever having
        // conflicted. Counting that succession as two coexisting byte-sets is
        // what let a spurious `.frozen` excuse itself.
        let s = SimStorage::new(SimClock::new(start()));
        s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
        s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
        assert!(s.committed_digests(BODY_KEY).is_empty());

        s.put_bytes(BODY_KEY, b"two".to_vec(), None).await.unwrap();
        assert_eq!(s.committed_digests(BODY_KEY), digests(&[b"two"]));

        // Both halves of the history describe ONE incarnation, so they end
        // together: a retired name may not still be carrying a contested body
        // from the life before it. (Truth never drops a tombstone, but
        // `--break resurrect` does, and an invariant that holds only while
        // nothing unusual happens is not one.)
        s.insert(&format!("{BODY_KEY}.tombstone"), b"{}".to_vec());
        s.put_bytes(BODY_KEY, b"three".to_vec(), None)
            .await
            .unwrap();
        assert!(!s
            .put_if_absent(BODY_KEY, b"four".to_vec(), None)
            .await
            .unwrap());
        assert_eq!(s.contested_digests(BODY_KEY), digests(&[b"three"]));
        s.delete_keys(&[format!("{BODY_KEY}.tombstone")])
            .await
            .unwrap();
        s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
        assert!(s.contested_digests(BODY_KEY).is_empty());
        assert!(s.committed_digests(BODY_KEY).is_empty());
    }

    #[tokio::test]
    async fn a_refused_writers_debris_attests_only_once_something_loses_to_it() {
        // `publish_record` stores the bytes and only then reads the filename
        // fence. Over a `.tombstone` or `.frozen` it refuses — and single-bucket
        // it deletes what it wrote, so those bytes were never a record the fleet
        // served. MULTI-bucket it deliberately leaves them ("a fenced
        // multi-bucket loser stays occupied and inert"), and the writer that
        // then loses the immutable filename to them is a replication copy leg,
        // which reads them back and freezes both sides on the difference
        // (`replicate::freeze_copy_race`). So the losing create is the event
        // that separates debris from evidence — not the fence, and not the
        // write.
        for fence in [".tombstone", ".frozen"] {
            let s = SimStorage::new(SimClock::new(start()));
            s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
            s.insert(&format!("{BODY_KEY}{fence}"), b"{}".to_vec());
            s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();

            s.put_bytes(BODY_KEY, b"refused".to_vec(), None)
                .await
                .unwrap();
            assert_eq!(
                s.committed_digests(BODY_KEY),
                digests(&[b"one"]),
                "a write {fence} refused is not a record the fleet served"
            );
            assert!(
                s.contested_digests(BODY_KEY).is_empty(),
                "and nothing has collided with it yet"
            );

            // Now a peer's copy leg goes for the same filename and loses.
            assert!(!s
                .put_if_absent(BODY_KEY, b"peer".to_vec(), None)
                .await
                .unwrap());
            assert_eq!(
                s.contested_digests(BODY_KEY),
                digests(&[b"refused"]),
                "the body it lost to is what it will read back and freeze on"
            );
            // Still not a published record: the two answers stay separate, and
            // a fenced delete may not erase either.
            s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
            assert_eq!(s.committed_digests(BODY_KEY), digests(&[b"one"]));
            assert_eq!(s.contested_digests(BODY_KEY), digests(&[b"refused"]));
        }
        // The demotion fence is the exception in the other direction: it
        // deliberately does not bar an upload, because handing the filename to
        // private truth is the settle's intended outcome. That body is real —
        // a completed mirror->private supersede is a succession, not a refusal,
        // and it must land in the plain history with nothing held back.
        let s = SimStorage::new(SimClock::new(start()));
        s.put_bytes(BODY_KEY, b"mirror".to_vec(), None)
            .await
            .unwrap();
        s.insert(&format!("{BODY_KEY}.mirror-quarantined"), b"{}".to_vec());
        s.put_bytes(BODY_KEY, b"private".to_vec(), None)
            .await
            .unwrap();
        assert_eq!(
            s.committed_digests(BODY_KEY),
            digests(&[b"mirror", b"private"])
        );
        // ...and a later create losing to it adds nothing: a published record
        // is already attested, so the losing-create rule can only ever promote
        // a body a fence refused. Otherwise every ordinary 409 on an immutable
        // filename would read as a byte conflict.
        assert!(!s
            .put_if_absent(BODY_KEY, b"another".to_vec(), None)
            .await
            .unwrap());
        assert!(s.contested_digests(BODY_KEY).is_empty());
    }

    /// Where `quarantine_bytes` would have preserved `bytes` from `BODY_KEY`.
    fn quarantine_copy_key(bytes: &[u8]) -> String {
        let (pkg, filename) = split_artifact_key(BODY_KEY).expect("BODY_KEY is an artifact");
        crate::replicate::quarantine_key(pkg, filename, &crate::hash::sha256_hex(bytes))
    }

    #[tokio::test]
    async fn a_body_freed_with_nothing_beside_it_is_recorded() {
        // The predicate the UNTYPED_DISAPPEARANCE oracle reads. A fence that
        // CLOSES the filename authorizes the removal; a bare delete does not.
        for fence in [".tombstone", ".frozen"] {
            let s = SimStorage::new(SimClock::new(start()));
            s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
            s.insert(&format!("{BODY_KEY}{fence}"), b"{}".to_vec());
            s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
            assert_eq!(
                s.body_removals(),
                (1, Vec::new()),
                "{fence} bars the upload, so it authorizes the removal"
            );
        }
        let s = SimStorage::new(SimClock::new(start()));
        s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
        s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
        assert_eq!(s.body_removals(), (1, vec![BODY_KEY.to_string()]));

        // Companions, views and claims are not bodies, and a key that was
        // already absent was not removed by this call — neither is even asked
        // about, so neither moves the denominator.
        let s = SimStorage::new(SimClock::new(start()));
        s.insert(&format!("{BODY_KEY}.meta.json"), b"{}".to_vec());
        s.insert("packages/p/.origin", b"mirror".to_vec());
        s.insert("simple/p/index.json", b"{}".to_vec());
        s.delete_keys(&[
            format!("{BODY_KEY}.meta.json"),
            "packages/p/.origin".to_string(),
            "simple/p/index.json".to_string(),
            BODY_KEY.to_string(),
        ])
        .await
        .unwrap();
        assert_eq!(s.body_removals(), (0, Vec::new()));
    }

    #[tokio::test]
    async fn a_demotion_fence_excuses_only_the_bytes_it_preserved() {
        // `.mirror-quarantined` is the one fence in the system that
        // deliberately does not bar an upload — handing the filename to private
        // truth IS the demotion's intended resolution — so it can stand over
        // bytes it never adjudicated. Reading it as authorization is how a
        // settle destroyed acked bytes on seeds 86001009016 and 40000042940.
        // What earns the delete is the `_quarantine/` copy, and only for the
        // bytes actually going away.
        let marker = format!("{BODY_KEY}.mirror-quarantined");

        let s = SimStorage::new(SimClock::new(start()));
        s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
        s.insert(&marker, b"{}".to_vec());
        s.insert(&quarantine_copy_key(b"one"), b"one".to_vec());
        s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
        assert_eq!(s.body_removals(), (1, Vec::new()));

        // The marker alone is not authorization: this is the settle that
        // dropped before it preserved, and `--break demote-lossy` is its kill.
        let s = SimStorage::new(SimClock::new(start()));
        s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
        s.insert(&marker, b"{}".to_vec());
        s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
        assert_eq!(s.body_removals(), (1, vec![BODY_KEY.to_string()]));

        // Nor is a copy of somebody ELSE's bytes: a private publish landing
        // under the standing marker replaces the body the settle read, and
        // deleting on the strength of the old copy destroys bytes no
        // `_quarantine/` object holds.
        let s = SimStorage::new(SimClock::new(start()));
        s.put_bytes(BODY_KEY, b"raced".to_vec(), None)
            .await
            .unwrap();
        s.insert(&marker, b"{}".to_vec());
        s.insert(&quarantine_copy_key(b"one"), b"one".to_vec());
        s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
        assert_eq!(s.body_removals(), (1, vec![BODY_KEY.to_string()]));
    }

    #[tokio::test]
    async fn a_standing_sidecar_excuses_nothing() {
        // There is no sidecar carve-out. A mirror upload publishes its sidecar
        // BEFORE the fence check that can still refuse it (`src/publish.rs`),
        // so excusing a delete because a mirror sidecar stands would waive the
        // invariant over exactly the 115b9ca shape this oracle exists for — a
        // publish freeing the immutable key it had just won. Mirror cache
        // eviction, the product's one unfenced body delete, is single-bucket
        // only (`delete_record` 409s it with more than one bucket) and this
        // workload draws mirror records only on partitioned seeds, so no
        // legitimate execution here needs the excuse.
        for origin in ["mirror", "private"] {
            let s = SimStorage::new(SimClock::new(start()));
            s.put_bytes(BODY_KEY, b"one".to_vec(), None).await.unwrap();
            s.insert(
                &format!("{BODY_KEY}.meta.json"),
                format!(r#"{{"sha256":"x","size":3,"origin":"{origin}"}}"#).into_bytes(),
            );
            s.insert("packages/p/.origin", br#"{"origin":"mirror"}"#.to_vec());
            s.delete_keys(&[BODY_KEY.to_string()]).await.unwrap();
            assert_eq!(
                s.body_removals(),
                (1, vec![BODY_KEY.to_string()]),
                "a {origin} sidecar is not authorization to destroy the body"
            );
        }
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
