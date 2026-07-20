//! Multi-bucket selection primitives (design: dev/MULTIBUCKET.md).
//!
//! P0 lays the types with zero behavior change: with one configured bucket a
//! [`BucketSet`] is a thin wrapper that always pins index 0, [`BucketSet::is_multi`]
//! is false, and none of the multi-bucket machinery (topology stamps, switching)
//! runs at all. The wide sweep that makes every request capture [`BucketSet::pin`]
//! at operation entry is P1; the selection health view that drives
//! [`BucketSet::switch`] is P4.

use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::storage::Storage;

/// Topology stamps are tiny control records. Never let their GET/CAS operations
/// inherit the data path's deliberately generous transfer timeout: one hung
/// bucket must not prevent a node from starting on the reachable topology.
const TOPOLOGY_IO_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct TopologyIoTimeout;

impl std::fmt::Display for TopologyIoTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "topology control I/O timed out after {} second",
            TOPOLOGY_IO_TIMEOUT.as_secs()
        )
    }
}

impl std::error::Error for TopologyIoTimeout {}

async fn bounded_topology_io<T>(operation: impl Future<Output = Result<T>>) -> Result<T> {
    match tokio::time::timeout(TOPOLOGY_IO_TIMEOUT, operation).await {
        Ok(result) => result,
        Err(_) => Err(TopologyIoTimeout.into()),
    }
}

fn topology_error_is_availability<F>(
    index: usize,
    error: &anyhow::Error,
    is_availability: &F,
) -> bool
where
    F: Fn(usize, &anyhow::Error) -> bool,
{
    error.is::<TopologyIoTimeout>() || is_availability(index, error)
}

/// One configured bucket: its storage handle and its identity (the bucket name,
/// used for topology stamping and log lines).
pub struct BucketHandle {
    pub storage: Arc<dyn Storage>,
    pub name: String,
}

/// The immutable storage context an operation runs against: a single handle, the
/// selection generation it was captured under, and the bucket's position in the
/// configured list. Pinned once at operation entry so a selection switch can
/// never tear an in-flight operation (design §3). Cheap to clone — one `Arc`.
#[derive(Clone)]
pub struct Pinned {
    pub storage: Arc<dyn Storage>,
    /// The selection generation this context was captured under. Threaded into
    /// the generation-tagged caches (src/cache.rs) so a switch can't serve one
    /// bucket's bytes for another. Zero until the first health-driven switch.
    pub generation: u64,
    /// This handle's position in the configured list — the replicator's source
    /// index, used to address every *other* bucket (design §4).
    pub index: usize,
}

/// The configured buckets in preference order plus the currently-selected one.
///
/// One bucket is the common case and stays completely dormant: [`pin`](Self::pin)
/// returns index 0, [`is_multi`](Self::is_multi) is false, and nothing ever calls
/// [`switch`](Self::switch). Selection state lives behind a single
/// `RwLock<Arc<Pinned>>` so a switch is one atomic swap and a reader never pairs a
/// stale handle with a fresh generation.
pub struct BucketSet {
    handles: Vec<BucketHandle>,
    /// The fleet-wide *write* selection: the bucket every write, first-write
    /// claim, and coordination decision serializes on. [`pin`](Self::pin).
    current: RwLock<Arc<Pinned>>,
    /// The node's *read* selection: its region bucket while that bucket is
    /// healthy and caught up, otherwise the write selection (read affinity,
    /// dev/READ_AFFINITY_VISION.md). Only consulted when `read_active` is set; in
    /// the common mode [`read_pin`](Self::read_pin) returns the write pin itself.
    read_current: RwLock<Arc<Pinned>>,
    /// Whether this node maintains a distinct read pin. `false` (no region match
    /// or single bucket) makes `read_pin` an alias of `pin` at the same cost.
    read_active: AtomicBool,
    /// One shared selection generation for both pins: any switch of either bumps
    /// it, and each pin carries the value inside its own `Arc<Pinned>`. Keeping
    /// both pins on one generation is what stops the shared caches (src/cache.rs)
    /// from thrashing between them.
    generation: AtomicU64,
    /// The topology generation established by startup verification or a local
    /// migration. `None` until a multi-bucket topology has been verified.
    topology_generation: RwLock<Option<u64>>,
    /// Serializes this process's verify/migrate operations. Cross-process races
    /// are still settled by storage CAS.
    topology_lock: tokio::sync::Mutex<()>,
    /// `new` cannot return `Result` without breaking the existing construction
    /// surface. Preserve the error and fail startup before the first topology I/O.
    duplicate_identity: Option<String>,
}

/// What a topology operation proved or changed. Unreachable indices are
/// reported explicitly so health/retry machinery can revisit them later.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopologyReport {
    pub generation: Option<u64>,
    pub verified_indices: Vec<usize>,
    pub stamped_indices: Vec<usize>,
    pub unreachable_indices: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyIndexStatus {
    Dormant,
    Verified,
    Stamped,
    Restamped,
    Unreachable,
}

impl BucketSet {
    /// A single-bucket set — the overwhelming common case. Pinned to index 0.
    pub fn single(storage: Arc<dyn Storage>) -> Self {
        Self::new(vec![BucketHandle {
            storage,
            name: String::new(),
        }])
    }

    /// Build from ordered handles, pinned to the preferred bucket (index 0,
    /// generation 0). Requires at least one handle — `StorageArgs::build_all`
    /// guarantees it.
    pub fn new(handles: Vec<BucketHandle>) -> Self {
        assert!(
            !handles.is_empty(),
            "BucketSet requires at least one configured bucket"
        );
        let mut seen = HashSet::with_capacity(handles.len());
        let duplicate_identity = handles
            .iter()
            .find(|handle| !seen.insert(handle.name.clone()))
            .map(|handle| handle.name.clone());
        let pinned = Arc::new(Pinned {
            storage: handles[0].storage.clone(),
            generation: 0,
            index: 0,
        });
        Self {
            handles,
            current: RwLock::new(pinned.clone()),
            read_current: RwLock::new(pinned),
            read_active: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            topology_generation: RwLock::new(None),
            topology_lock: tokio::sync::Mutex::new(()),
            duplicate_identity,
        }
    }

    /// Capture the current storage context. Every operation calls this once at
    /// entry and performs all its I/O against the returned handle; the wide
    /// wiring that enforces that is P1. Recovers a poisoned lock rather than
    /// panicking on a request path.
    pub fn pin(&self) -> Arc<Pinned> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The storage context request *reads* should use. When read affinity is
    /// active this is the node's region bucket (while healthy and caught up);
    /// otherwise it is byte-for-byte [`pin`](Self::pin) — no distinct selection
    /// is maintained, so single-bucket and no-region-match nodes pay nothing and
    /// read from the same bucket they write. Never used for writes or any
    /// origin-claim decision that could reach upstream (dev/READ_AFFINITY_VISION.md).
    pub fn read_pin(&self) -> Arc<Pinned> {
        if self.read_active.load(Ordering::Acquire) {
            self.read_current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        } else {
            self.pin()
        }
    }

    /// Whether this node maintains a distinct read pin.
    #[cfg(test)]
    pub(crate) fn read_affinity_active(&self) -> bool {
        self.read_active.load(Ordering::Acquire)
    }

    fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Republish a pin at a new generation without moving its bucket, so both
    /// pins always report the same (latest) generation.
    fn republish_generation(lock: &RwLock<Arc<Pinned>>, generation: u64) {
        let mut current = lock.write().unwrap_or_else(PoisonError::into_inner);
        *current = Arc::new(Pinned {
            storage: current.storage.clone(),
            generation,
            index: current.index,
        });
    }

    /// Number of configured buckets.
    #[allow(clippy::len_without_is_empty)] // a BucketSet is never empty (see `new`)
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// The configured buckets in preference order. The replicator (src/replicate.rs)
    /// reads these to address destinations by index; `Pinned::index` names the
    /// source. Kept read-only — selection state lives behind the `RwLock` above.
    pub fn handles(&self) -> &[BucketHandle] {
        &self.handles
    }

    /// More than one bucket configured — the multi-bucket mechanisms are live.
    pub fn is_multi(&self) -> bool {
        self.handles.len() > 1
    }

    /// The topology generation this process has verified. Single-bucket mode and
    /// a multi-bucket set not yet verified return `None` without any I/O.
    pub(crate) fn topology_generation(&self) -> Option<u64> {
        *self
            .topology_generation
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn set_topology_generation(&self, generation: u64) {
        *self
            .topology_generation
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(generation);
    }

    fn validate_topology_config(&self) -> Result<()> {
        if let Some(duplicate) = &self.duplicate_identity {
            bail!(
                "duplicate bucket identity '{duplicate}' in multi-bucket topology; each configured bucket must be unique"
            );
        }
        for handle in &self.handles {
            if !handle.storage.supports_leases() {
                bail!(
                    "multi-bucket requires a backend with conditional writes; \
                     bucket '{}' does not support them",
                    handle.name
                );
            }
        }
        Ok(())
    }

    fn topology_identity(&self) -> (Vec<String>, String) {
        let names: Vec<String> = self
            .handles
            .iter()
            .map(|handle| handle.name.clone())
            .collect();
        let hash = topology_hash(&names);
        (names, hash)
    }

    /// Select a different *write* bucket, bumping the shared generation so caches
    /// and any already-pinned contexts from the old selection are recognizably
    /// stale. Driven by the worker's per-node health view. Kept crate-visible so
    /// only selection orchestration, not request handlers, can change it.
    pub(crate) fn switch(&self, index: usize) -> Arc<Pinned> {
        let generation = self.bump_generation();
        let next = Arc::new(Pinned {
            storage: self.handles[index].storage.clone(),
            generation,
            index,
        });
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = next.clone();
        // Lift the read pin to the same generation (its bucket unchanged) so the
        // two pins never diverge; harmless when the read pin will itself switch
        // this tick. A no-op in the common mode where no read pin is active.
        if self.read_active.load(Ordering::Acquire) {
            Self::republish_generation(&self.read_current, generation);
        }
        next
    }

    /// Select a different *read* bucket, bumping the shared generation. The write
    /// pin is republished at the same generation (its bucket unchanged) so a
    /// handler never pairs a storage handle from one pin with a generation from
    /// the other. Crate-private like [`switch`](Self::switch).
    pub(crate) fn switch_read(&self, index: usize) -> Arc<Pinned> {
        let generation = self.bump_generation();
        let next = Arc::new(Pinned {
            storage: self.handles[index].storage.clone(),
            generation,
            index,
        });
        *self
            .read_current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = next.clone();
        self.read_active.store(true, Ordering::Release);
        Self::republish_generation(&self.current, generation);
        next
    }

    /// Activate read affinity and point the read pin at `index` at the *current*
    /// generation — startup seeding, once the node's region bucket is known. No
    /// generation bump: nothing is cached yet, and both pins must share one
    /// generation.
    pub(crate) fn seed_read_pin(&self, index: usize) {
        let generation = self.generation.load(Ordering::Acquire);
        let seeded = Arc::new(Pinned {
            storage: self.handles[index].storage.clone(),
            generation,
            index,
        });
        *self
            .read_current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = seeded;
        self.read_active.store(true, Ordering::Release);
    }

    /// Conservative startup verification: no error is guessed to mean
    /// availability. The observed-storage integration should call
    /// [`verify_topology_with`](Self::verify_topology_with) with its strict error
    /// classifier. Single-bucket mode performs zero I/O.
    #[cfg(test)]
    pub(crate) async fn verify_topology(&self) -> Result<()> {
        self.verify_topology_with(|_, _| false).await.map(drop)
    }

    /// Verify the ordered topology and one consistent generation across every
    /// reachable bucket. Each tiny control operation has a one-second bound;
    /// timeout means that bucket is unreachable. Returned storage errors are
    /// skipped only when `is_availability(index, error)` says so, so auth, KMS,
    /// quota, configuration, and unknown failures still fail closed.
    pub async fn verify_topology_with<F>(&self, is_availability: F) -> Result<TopologyReport>
    where
        F: Fn(usize, &anyhow::Error) -> bool,
    {
        if !self.is_multi() {
            return Ok(TopologyReport::default());
        }
        let _guard = self.topology_lock.lock().await;
        self.validate_topology_config()?;
        let (names, expected_hash) = self.topology_identity();
        let mut report = TopologyReport::default();
        let mut reachable = Vec::new();
        let mut generation = self.topology_generation();

        // Read every reachable stamp before writing anything. The highest
        // generation is authoritative: a lower generation may be a bucket that
        // was unreachable during a partial migration and can still carry the
        // previous topology. Only stamps tied at the highest generation must
        // agree with the configured topology; lower stamps are CAS-restamped.
        for (index, handle) in self.handles.iter().enumerate() {
            match bounded_topology_io(handle.storage.get_with_etag(TOPOLOGY_STAMP_KEY)).await {
                Ok(Some((bytes, etag))) => {
                    let found = parse_stamp(&handle.name, &bytes)?;
                    generation = Some(
                        generation
                            .map_or(found.generation, |current| current.max(found.generation)),
                    );
                    reachable.push((index, Some((found, etag))));
                }
                Ok(None) => reachable.push((index, None)),
                Err(error) if topology_error_is_availability(index, &error, &is_availability) => {
                    warn!(bucket=%handle.name, error=%error, "bucket unavailable during topology verification; deferring");
                    report.unreachable_indices.push(index);
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("read topology stamp from bucket '{}'", handle.name)
                    })
                }
            }
        }

        // Fail closed when nothing answered: committing a fabricated generation 0
        // here permanently write-fences this node once the buckets recover at a
        // higher generation (v1 review finding). Mirror `migrate_topology_with`'s
        // no-reachable-bucket refusal.
        if reachable.is_empty() {
            bail!("cannot verify topology: no configured bucket is reachable at startup");
        }
        let generation = generation.unwrap_or(0);
        let body = encode_stamp(&names, &expected_hash, generation)?;
        // A conflict at the highest generation has no deterministic winner.
        // Validate every such stamp before repairing even one laggard.
        for (index, current) in &reachable {
            if let Some((found, _)) = current {
                if found.generation == generation {
                    check_stamp_identity(
                        &self.handles[*index].name,
                        found,
                        &expected_hash,
                        &names,
                    )?;
                    report.verified_indices.push(*index);
                }
            }
        }

        for (index, current) in reachable {
            let handle = &self.handles[index];
            let result = match current {
                Some((found, _)) if found.generation == generation => continue,
                Some((_, etag)) => {
                    bounded_topology_io(handle.storage.put_if_match(
                        TOPOLOGY_STAMP_KEY,
                        &etag,
                        body.clone(),
                    ))
                    .await
                }
                None => {
                    bounded_topology_io(
                        handle
                            .storage
                            .put_if_none_match(TOPOLOGY_STAMP_KEY, body.clone()),
                    )
                    .await
                }
            };
            match result {
                Ok(Some(_)) => report.stamped_indices.push(index),
                Ok(None) => {
                    if verify_raced_stamp(
                        handle,
                        index,
                        &expected_hash,
                        &names,
                        generation,
                        &is_availability,
                    )
                    .await?
                    {
                        report.verified_indices.push(index);
                    } else {
                        report.unreachable_indices.push(index);
                    }
                }
                Err(error) if topology_error_is_availability(index, &error, &is_availability) => {
                    warn!(bucket=%handle.name, error=%error, "bucket became unavailable while stamping topology; deferring");
                    report.unreachable_indices.push(index);
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("conditionally stamp topology on bucket '{}'", handle.name)
                    })
                }
            }
        }
        report.unreachable_indices.sort_unstable();
        report.unreachable_indices.dedup();
        report.generation = Some(generation);
        self.set_topology_generation(generation);
        Ok(report)
    }

    /// The member identities recorded by the highest-generation topology stamp on
    /// any *reachable* bucket in this set, or `None` when no reachable bucket
    /// carries a stamp yet. `migrate` uses this to learn the *previous* topology
    /// so a bucket being dropped can be checked for undrained notes before it is
    /// removed. Availability failures are skipped; every other error fails closed.
    pub async fn stamped_member_names_with<F>(
        &self,
        is_availability: F,
    ) -> Result<Option<Vec<String>>>
    where
        F: Fn(usize, &anyhow::Error) -> bool,
    {
        let mut best: Option<(u64, Vec<String>)> = None;
        for (index, handle) in self.handles.iter().enumerate() {
            match bounded_topology_io(handle.storage.get_with_etag(TOPOLOGY_STAMP_KEY)).await {
                Ok(Some((bytes, _etag))) => {
                    let found = parse_stamp(&handle.name, &bytes)?;
                    if best
                        .as_ref()
                        .is_none_or(|(gen, _)| found.generation >= *gen)
                    {
                        best = Some((found.generation, found.buckets));
                    }
                }
                Ok(None) => {}
                Err(error) if topology_error_is_availability(index, &error, &is_availability) => {
                    warn!(bucket=%handle.name, error=%error, "bucket unreachable while reading topology stamp; skipping");
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("read topology stamp from bucket '{}'", handle.name)
                    })
                }
            }
        }
        Ok(best.map(|(_, names)| names))
    }

    /// Conservatively verify one bucket after a reachability transition. Use
    /// [`verify_topology_index_with`](Self::verify_topology_index_with) once the
    /// observed-storage error classifier is available.
    #[cfg(test)]
    pub(crate) async fn verify_topology_index(&self, index: usize) -> Result<TopologyIndexStatus> {
        self.verify_topology_index_with(index, |_, _| false).await
    }

    /// Verify or conditionally re-stamp one bucket against the process's current
    /// topology generation. A lower generation is a bucket that missed a local
    /// migration and is CAS-restamped; equal/higher conflicting state fails.
    pub async fn verify_topology_index_with<F>(
        &self,
        index: usize,
        is_availability: F,
    ) -> Result<TopologyIndexStatus>
    where
        F: Fn(usize, &anyhow::Error) -> bool,
    {
        let Some(handle) = self.handles.get(index) else {
            bail!("bucket index {index} is out of range");
        };
        if !self.is_multi() {
            return Ok(TopologyIndexStatus::Dormant);
        }
        let _guard = self.topology_lock.lock().await;
        self.validate_topology_config()?;
        let generation = self.topology_generation().ok_or_else(|| {
            anyhow!("topology has not been verified; run startup verification first")
        })?;
        let (names, expected_hash) = self.topology_identity();
        let target = encode_stamp(&names, &expected_hash, generation)?;

        let current = match bounded_topology_io(handle.storage.get_with_etag(TOPOLOGY_STAMP_KEY))
            .await
        {
            Ok(current) => current,
            Err(error) if topology_error_is_availability(index, &error, &is_availability) => {
                return Ok(TopologyIndexStatus::Unreachable)
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read topology stamp from bucket '{}'", handle.name))
            }
        };

        match current {
            None => match bounded_topology_io(
                handle.storage.put_if_none_match(TOPOLOGY_STAMP_KEY, target),
            )
            .await
            {
                Ok(Some(_)) => Ok(TopologyIndexStatus::Stamped),
                Ok(None) => {
                    if verify_raced_stamp(
                        handle,
                        index,
                        &expected_hash,
                        &names,
                        generation,
                        &is_availability,
                    )
                    .await?
                    {
                        Ok(TopologyIndexStatus::Verified)
                    } else {
                        Ok(TopologyIndexStatus::Unreachable)
                    }
                }
                Err(error) if topology_error_is_availability(index, &error, &is_availability) => {
                    Ok(TopologyIndexStatus::Unreachable)
                }
                Err(error) => Err(error)
                    .with_context(|| format!("stamp topology on bucket '{}'", handle.name)),
            },
            Some((bytes, etag)) => {
                let found = parse_stamp(&handle.name, &bytes)?;
                if found.generation == generation {
                    check_stamp_identity(&handle.name, &found, &expected_hash, &names)?;
                    return Ok(TopologyIndexStatus::Verified);
                }
                if found.generation > generation {
                    bail!(
                        "bucket '{}' has newer topology generation {}, local generation is {}",
                        handle.name,
                        found.generation,
                        generation
                    );
                }
                match bounded_topology_io(handle.storage.put_if_match(
                    TOPOLOGY_STAMP_KEY,
                    &etag,
                    target,
                ))
                .await
                {
                    Ok(Some(_)) => Ok(TopologyIndexStatus::Restamped),
                    Ok(None) => {
                        if verify_raced_stamp(
                            handle,
                            index,
                            &expected_hash,
                            &names,
                            generation,
                            &is_availability,
                        )
                        .await?
                        {
                            Ok(TopologyIndexStatus::Verified)
                        } else {
                            Ok(TopologyIndexStatus::Unreachable)
                        }
                    }
                    Err(error)
                        if topology_error_is_availability(index, &error, &is_availability) =>
                    {
                        Ok(TopologyIndexStatus::Unreachable)
                    }
                    Err(error) => Err(error)
                        .with_context(|| format!("re-stamp topology on bucket '{}'", handle.name)),
                }
            }
        }
    }

    /// Deliberately migrate the configured topology. Every reachable stamp is
    /// replaced with CAS (or create-if-absent), at one greater than the maximum
    /// generation observed either in storage or previously verified locally.
    /// Availability alone is tolerated and reported; every other error aborts.
    #[cfg(test)]
    pub(crate) async fn migrate_topology(&self) -> Result<TopologyReport> {
        self.migrate_topology_with(|_, _| false).await
    }

    pub async fn migrate_topology_with<F>(&self, is_availability: F) -> Result<TopologyReport>
    where
        F: Fn(usize, &anyhow::Error) -> bool,
    {
        if !self.is_multi() {
            return Ok(TopologyReport::default());
        }
        let _guard = self.topology_lock.lock().await;
        self.validate_topology_config()?;
        let (names, expected_hash) = self.topology_identity();
        let mut report = TopologyReport::default();
        let mut reachable = Vec::new();
        let local_generation = self.topology_generation();
        let mut max_observed_generation: Option<u64> = None;

        for (index, handle) in self.handles.iter().enumerate() {
            match bounded_topology_io(handle.storage.get_with_etag(TOPOLOGY_STAMP_KEY)).await {
                Ok(Some((bytes, etag))) => {
                    let found = parse_stamp(&handle.name, &bytes)?;
                    max_observed_generation = Some(
                        max_observed_generation
                            .map_or(found.generation, |value| value.max(found.generation)),
                    );
                    reachable.push((index, Some(etag)));
                }
                Ok(None) => reachable.push((index, None)),
                Err(error) if topology_error_is_availability(index, &error, &is_availability) => {
                    warn!(bucket=%handle.name, error=%error, "bucket unavailable during topology migration; deferring");
                    report.unreachable_indices.push(index);
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("read topology stamp from bucket '{}'", handle.name)
                    })
                }
            }
        }
        if reachable.is_empty() {
            bail!("cannot migrate topology: no configured bucket is reachable");
        }
        if let (Some(local), Some(observed)) = (local_generation, max_observed_generation) {
            if observed < local {
                bail!(
                    "reachable topology generation regressed to {observed}; this process previously verified {local}"
                );
            }
        }
        let generation = max_observed_generation
            .or(local_generation)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("topology generation overflow"))?;
        let target = encode_stamp(&names, &expected_hash, generation)?;

        for (index, etag) in reachable {
            let handle = &self.handles[index];
            let result = match etag {
                Some(etag) => {
                    bounded_topology_io(handle.storage.put_if_match(
                        TOPOLOGY_STAMP_KEY,
                        &etag,
                        target.clone(),
                    ))
                    .await
                }
                None => {
                    bounded_topology_io(
                        handle
                            .storage
                            .put_if_none_match(TOPOLOGY_STAMP_KEY, target.clone()),
                    )
                    .await
                }
            };
            match result {
                Ok(Some(_)) => report.stamped_indices.push(index),
                Ok(None) => {
                    if verify_raced_stamp(
                        handle,
                        index,
                        &expected_hash,
                        &names,
                        generation,
                        &is_availability,
                    )
                    .await?
                    {
                        report.verified_indices.push(index);
                    } else {
                        report.unreachable_indices.push(index);
                    }
                }
                Err(error) if topology_error_is_availability(index, &error, &is_availability) => {
                    warn!(bucket=%handle.name, error=%error, "bucket became unavailable during topology migration; deferring");
                    report.unreachable_indices.push(index);
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("migrate topology on bucket '{}'", handle.name))
                }
            }
        }
        report.unreachable_indices.sort_unstable();
        report.unreachable_indices.dedup();
        report.generation = Some(generation);
        self.set_topology_generation(generation);
        Ok(report)
    }
}

/// Storage key of the topology stamp — the fail-closed record of which buckets a
/// deployment was configured with, in what order (design §7).
pub const TOPOLOGY_STAMP_KEY: &str = "_topology/stamp.json";

/// The stored topology stamp. `hash` is derived from `buckets` alone;
/// `generation` is bumped by an operator re-stamp (`buckets migrate`, a later
/// phase) and does not participate in the hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TopologyStamp {
    pub buckets: Vec<String>,
    pub hash: String,
    pub generation: u64,
}

fn encode_stamp(names: &[String], hash: &str, generation: u64) -> Result<Vec<u8>> {
    serde_json::to_vec(&TopologyStamp {
        buckets: names.to_vec(),
        hash: hash.to_string(),
        generation,
    })
    .context("serialize topology stamp")
}

fn parse_stamp(bucket: &str, bytes: &[u8]) -> Result<TopologyStamp> {
    serde_json::from_slice(bytes)
        .with_context(|| format!("parse topology stamp on bucket '{bucket}'"))
}

/// Deterministic identity of an ordered bucket list: sha256 over the names joined
/// by a NUL byte (which no bucket name can contain), so two deployments agree iff
/// they list the same buckets in the same order.
pub(crate) fn topology_hash(names: &[String]) -> String {
    let mut hasher = Sha256::new();
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            hasher.update([0u8]);
        }
        hasher.update(name.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Compare both redundant identity fields. Checking the list as well as its hash
/// catches corrupt/manual stamps without leaning on collision resistance.
fn check_stamp_identity(
    bucket: &str,
    found: &TopologyStamp,
    expected_hash: &str,
    expected: &[String],
) -> Result<()> {
    if found.hash != expected_hash || found.buckets != expected {
        bail!(
            "bucket '{bucket}' was stamped for a different bucket topology ({:?}) than this \
             node is configured with ({:?}); refusing to start. If you intend to change the \
             bucket set, re-stamp it deliberately.",
            found.buckets,
            expected
        );
    }
    Ok(())
}

fn check_exact_stamp(
    bucket: &str,
    found: &TopologyStamp,
    expected_hash: &str,
    expected: &[String],
    generation: u64,
) -> Result<()> {
    check_stamp_identity(bucket, found, expected_hash, expected)?;
    if found.generation != generation {
        bail!(
            "bucket '{bucket}' has topology generation {}, expected {generation}",
            found.generation
        );
    }
    Ok(())
}

/// A failed conditional write means another process won. Accept it only when a
/// fresh read proves that process wrote the exact intended stamp. Availability
/// may defer the check; every other result fails closed.
async fn verify_raced_stamp<F>(
    handle: &BucketHandle,
    index: usize,
    expected_hash: &str,
    expected: &[String],
    generation: u64,
    is_availability: &F,
) -> Result<bool>
where
    F: Fn(usize, &anyhow::Error) -> bool,
{
    match bounded_topology_io(handle.storage.get_with_etag(TOPOLOGY_STAMP_KEY)).await {
        Ok(Some((bytes, _))) => {
            let found = parse_stamp(&handle.name, &bytes)?;
            check_exact_stamp(&handle.name, &found, expected_hash, expected, generation)?;
            Ok(true)
        }
        Ok(None) => bail!(
            "topology stamp on bucket '{}' vanished after a conditional-write race",
            handle.name
        ),
        Err(error) if topology_error_is_availability(index, &error, is_availability) => {
            warn!(bucket=%handle.name, error=%error, "bucket unavailable while resolving topology CAS race; deferring");
            Ok(false)
        }
        Err(error) => Err(error)
            .with_context(|| format!("re-read topology stamp on bucket '{}'", handle.name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{test_support::InMemStorage, DiskStorage, FileEntry, ObjectMeta};
    use std::sync::atomic::{AtomicU8, Ordering};

    const NORMAL: u8 = 0;
    const HANG: u8 = 1;
    const FAIL: u8 = 2;

    struct TopologyTestStorage {
        inner: Arc<InMemStorage>,
        read_mode: AtomicU8,
        write_mode: AtomicU8,
    }

    impl TopologyTestStorage {
        fn new() -> Self {
            Self {
                inner: Arc::new(InMemStorage::default()),
                read_mode: AtomicU8::new(NORMAL),
                write_mode: AtomicU8::new(NORMAL),
            }
        }

        fn hang_reads(&self) {
            self.read_mode.store(HANG, Ordering::SeqCst);
        }

        fn fail_reads(&self) {
            self.read_mode.store(FAIL, Ordering::SeqCst);
        }

        fn hang_writes(&self) {
            self.write_mode.store(HANG, Ordering::SeqCst);
        }

        async fn read_behavior(&self) -> Result<()> {
            match self.read_mode.load(Ordering::SeqCst) {
                HANG => std::future::pending().await,
                FAIL => bail!("AccessDenied: test credential rejected"),
                _ => Ok(()),
            }
        }

        async fn write_behavior(&self) -> Result<()> {
            match self.write_mode.load(Ordering::SeqCst) {
                HANG => std::future::pending().await,
                FAIL => bail!("AccessDenied: test credential rejected"),
                _ => Ok(()),
            }
        }
    }

    #[async_trait::async_trait]
    impl Storage for TopologyTestStorage {
        async fn head_exists(&self, key: &str) -> Result<bool> {
            self.inner.head_exists(key).await
        }

        async fn serve_artifact(
            &self,
            key: &str,
            range: Option<&str>,
        ) -> Result<axum::response::Response<axum::body::Body>> {
            self.inner.serve_artifact(key, range).await
        }

        async fn presign_get(&self, key: &str, expires: Duration) -> Result<Option<String>> {
            self.inner.presign_get(key, expires).await
        }

        async fn put_bytes(
            &self,
            key: &str,
            bytes: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<()> {
            self.inner.put_bytes(key, bytes, content_type).await
        }

        async fn put_if_absent(
            &self,
            key: &str,
            bytes: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<bool> {
            self.inner.put_if_absent(key, bytes, content_type).await
        }

        async fn put_file_if_absent(
            &self,
            key: &str,
            path: &std::path::Path,
            content_type: Option<&str>,
        ) -> Result<bool> {
            self.inner.put_file_if_absent(key, path, content_type).await
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

        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            self.inner.delete_keys(keys).await
        }

        fn supports_leases(&self) -> bool {
            true
        }

        async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.read_behavior().await?;
            self.inner.get_with_etag(key).await
        }

        async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
            self.write_behavior().await?;
            self.inner.put_if_none_match(key, bytes).await
        }

        async fn put_if_match(
            &self,
            key: &str,
            etag: &str,
            bytes: Vec<u8>,
        ) -> Result<Option<String>> {
            self.write_behavior().await?;
            self.inner.put_if_match(key, etag, bytes).await
        }
    }

    fn handle(name: &str, root: &str) -> BucketHandle {
        BucketHandle {
            storage: Arc::new(DiskStorage::new(root)),
            name: name.to_string(),
        }
    }

    fn memory_handle(name: &str, storage: &Arc<InMemStorage>) -> BucketHandle {
        BucketHandle {
            storage: storage.clone(),
            name: name.to_string(),
        }
    }

    fn topology_test_handle(name: &str, storage: &Arc<TopologyTestStorage>) -> BucketHandle {
        BucketHandle {
            storage: storage.clone(),
            name: name.to_string(),
        }
    }

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn stamp_bytes(values: &[&str], generation: u64) -> Vec<u8> {
        let buckets = names(values);
        let hash = topology_hash(&buckets);
        encode_stamp(&buckets, &hash, generation).unwrap()
    }

    async fn seed_stamp(storage: &InMemStorage, values: &[&str], generation: u64) {
        assert!(storage
            .put_if_none_match(TOPOLOGY_STAMP_KEY, stamp_bytes(values, generation))
            .await
            .unwrap()
            .is_some());
    }

    async fn stored_stamp(storage: &InMemStorage) -> TopologyStamp {
        let (bytes, _) = storage
            .get_with_etag(TOPOLOGY_STAMP_KEY)
            .await
            .unwrap()
            .unwrap();
        parse_stamp("test", &bytes).unwrap()
    }

    #[test]
    fn single_is_index_zero_generation_zero() {
        let s: Arc<dyn Storage> = Arc::new(DiskStorage::new("/tmp/pypiron-bucketset-a"));
        let set = BucketSet::single(s.clone());
        assert!(!set.is_multi());
        assert_eq!(set.len(), 1);
        let pinned = set.pin();
        assert_eq!(pinned.index, 0);
        assert_eq!(pinned.generation, 0);
        assert!(Arc::ptr_eq(&pinned.storage, &s));
    }

    #[test]
    fn pin_is_stable() {
        let set = BucketSet::single(Arc::new(DiskStorage::new("/tmp/pypiron-bucketset-b")));
        let a = set.pin();
        let b = set.pin();
        assert_eq!(a.generation, b.generation);
        assert_eq!(a.index, b.index);
        assert!(Arc::ptr_eq(&a.storage, &b.storage));
    }

    #[tokio::test]
    async fn single_bucket_topology_is_dormant_without_conditional_storage() {
        let set = BucketSet::single(Arc::new(DiskStorage::new(
            "/tmp/pypiron-bucketset-topology-single",
        )));
        set.verify_topology().await.unwrap();
        assert_eq!(
            set.migrate_topology().await.unwrap(),
            TopologyReport::default()
        );
        assert_eq!(
            set.verify_topology_index(0).await.unwrap(),
            TopologyIndexStatus::Dormant
        );
        assert_eq!(set.topology_generation(), None);
    }

    #[tokio::test]
    async fn duplicate_bucket_identities_are_rejected_before_stamping() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let set = BucketSet::new(vec![memory_handle("east", &a), memory_handle("east", &b)]);

        let error = set.verify_topology().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("duplicate bucket identity 'east'"));
        assert!(a.get_with_etag(TOPOLOGY_STAMP_KEY).await.unwrap().is_none());
        assert!(b.get_with_etag(TOPOLOGY_STAMP_KEY).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn startup_bounds_a_hung_bucket_as_unreachable() {
        let reachable = Arc::new(InMemStorage::default());
        let hung = Arc::new(TopologyTestStorage::new());
        hung.hang_reads();
        let set = BucketSet::new(vec![
            memory_handle("east", &reachable),
            topology_test_handle("west", &hung),
        ]);

        // The caller's classifier rejects every returned error. The internal
        // control-I/O timeout is still availability, so east can start alone.
        let report = set.verify_topology_with(|_, _| false).await.unwrap();
        assert_eq!(report.generation, Some(0));
        assert_eq!(report.stamped_indices, vec![0]);
        assert_eq!(report.unreachable_indices, vec![1]);
        assert_eq!(set.topology_generation(), Some(0));
    }

    #[tokio::test]
    async fn startup_refuses_when_no_bucket_is_reachable() {
        // Both buckets hang: the internal control-I/O timeout classes each as
        // unreachable, so nothing answers. Committing a fabricated generation 0
        // would write-fence the node once they recover higher; fail closed.
        let east = Arc::new(TopologyTestStorage::new());
        let west = Arc::new(TopologyTestStorage::new());
        east.hang_reads();
        west.hang_reads();
        let set = BucketSet::new(vec![
            topology_test_handle("east", &east),
            topology_test_handle("west", &west),
        ]);

        let error = set.verify_topology_with(|_, _| false).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("no configured bucket is reachable"));
        assert_eq!(set.topology_generation(), None);
    }

    #[tokio::test]
    async fn runtime_verification_and_race_proof_bound_hung_reads() {
        let east = Arc::new(InMemStorage::default());
        let west = Arc::new(TopologyTestStorage::new());
        let set = BucketSet::new(vec![
            memory_handle("east", &east),
            topology_test_handle("west", &west),
        ]);
        set.verify_topology_with(|_, _| false).await.unwrap();

        west.hang_reads();
        assert_eq!(
            set.verify_topology_index_with(1, |_, _| false)
                .await
                .unwrap(),
            TopologyIndexStatus::Unreachable
        );

        let expected = names(&["east", "west"]);
        let expected_hash = topology_hash(&expected);
        assert!(!verify_raced_stamp(
            &set.handles()[1],
            1,
            &expected_hash,
            &expected,
            0,
            &|_, _| false,
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn migration_bounds_a_hung_conditional_write() {
        let east = Arc::new(InMemStorage::default());
        let west = Arc::new(TopologyTestStorage::new());
        let set = BucketSet::new(vec![
            memory_handle("east", &east),
            topology_test_handle("west", &west),
        ]);
        set.verify_topology_with(|_, _| false).await.unwrap();

        west.hang_writes();
        let report = set.migrate_topology_with(|_, _| false).await.unwrap();
        assert_eq!(report.generation, Some(1));
        assert_eq!(report.stamped_indices, vec![0]);
        assert_eq!(report.unreachable_indices, vec![1]);
        assert_eq!(set.topology_generation(), Some(1));
    }

    #[tokio::test]
    async fn returned_non_availability_errors_still_fail_closed() {
        let east = Arc::new(InMemStorage::default());
        let rejected = Arc::new(TopologyTestStorage::new());
        rejected.fail_reads();
        let set = BucketSet::new(vec![
            memory_handle("east", &east),
            topology_test_handle("west", &rejected),
        ]);

        let error = set.verify_topology_with(|_, _| false).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("read topology stamp from bucket 'west'"));
        assert!(error.root_cause().to_string().contains("AccessDenied"));
        assert_eq!(set.topology_generation(), None);
        assert!(east
            .get_with_etag(TOPOLOGY_STAMP_KEY)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn startup_requires_one_generation_and_stamps_missing_with_it() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        seed_stamp(a.as_ref(), &["east", "west"], 7).await;
        let set = BucketSet::new(vec![memory_handle("east", &a), memory_handle("west", &b)]);

        let report = set.verify_topology_with(|_, _| false).await.unwrap();
        assert_eq!(report.generation, Some(7));
        assert_eq!(report.verified_indices, vec![0]);
        assert_eq!(report.stamped_indices, vec![1]);
        assert!(report.unreachable_indices.is_empty());
        assert_eq!(set.topology_generation(), Some(7));
        assert_eq!(stored_stamp(b.as_ref()).await.generation, 7);
    }

    #[tokio::test]
    async fn startup_restamps_a_reachable_generation_laggard() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        seed_stamp(a.as_ref(), &["east", "west"], 3).await;
        seed_stamp(b.as_ref(), &["east", "west"], 4).await;
        let set = BucketSet::new(vec![memory_handle("east", &a), memory_handle("west", &b)]);

        let report = set.verify_topology_with(|_, _| false).await.unwrap();
        assert_eq!(report.generation, Some(4));
        assert_eq!(report.verified_indices, vec![1]);
        assert_eq!(report.stamped_indices, vec![0]);
        assert_eq!(stored_stamp(a.as_ref()).await.generation, 4);
        assert_eq!(set.topology_generation(), Some(4));
    }

    #[tokio::test]
    async fn startup_repairs_an_old_topology_below_the_highest_generation() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        seed_stamp(a.as_ref(), &["east", "west"], 8).await;
        // West missed the migration and returns with both the old identity and
        // the old generation. Generation 8 is the unambiguous authority.
        seed_stamp(b.as_ref(), &["old-east", "old-west"], 7).await;
        let set = BucketSet::new(vec![memory_handle("east", &a), memory_handle("west", &b)]);

        let report = set.verify_topology_with(|_, _| false).await.unwrap();
        assert_eq!(report.generation, Some(8));
        assert_eq!(report.verified_indices, vec![0]);
        assert_eq!(report.stamped_indices, vec![1]);
        let repaired = stored_stamp(b.as_ref()).await;
        assert_eq!(repaired.buckets, names(&["east", "west"]));
        assert_eq!(repaired.generation, 8);
    }

    #[tokio::test]
    async fn startup_rejects_conflicting_topologies_at_the_highest_generation() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        seed_stamp(a.as_ref(), &["east", "west"], 8).await;
        seed_stamp(b.as_ref(), &["west", "east"], 8).await;
        let set = BucketSet::new(vec![memory_handle("east", &a), memory_handle("west", &b)]);

        let error = set.verify_topology().await.unwrap_err();
        assert!(error.to_string().contains("different bucket topology"));
        assert_eq!(set.topology_generation(), None);
        // Fail before any repair: equal-generation conflicts have no winner.
        assert_eq!(
            stored_stamp(a.as_ref()).await.buckets,
            names(&["east", "west"])
        );
        assert_eq!(
            stored_stamp(b.as_ref()).await.buckets,
            names(&["west", "east"])
        );
    }

    #[tokio::test]
    async fn migration_uses_max_generation_plus_one_and_runtime_heals_a_laggard() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        seed_stamp(a.as_ref(), &["old-east", "old-west"], 7).await;
        seed_stamp(b.as_ref(), &["east", "west"], 4).await;
        let set = BucketSet::new(vec![memory_handle("east", &a), memory_handle("west", &b)]);

        let report = set.migrate_topology().await.unwrap();
        assert_eq!(report.generation, Some(8));
        assert_eq!(report.stamped_indices, vec![0, 1]);
        assert_eq!(set.topology_generation(), Some(8));
        for storage in [&a, &b] {
            let stamp = stored_stamp(storage.as_ref()).await;
            assert_eq!(stamp.buckets, names(&["east", "west"]));
            assert_eq!(stamp.hash, topology_hash(&stamp.buckets));
            assert_eq!(stamp.generation, 8);
        }

        // Simulate west having been unreachable during the migration and
        // returning with its older stamp. Runtime verification must CAS it up.
        b.put_bytes(
            TOPOLOGY_STAMP_KEY,
            stamp_bytes(&["old-east", "old-west"], 7),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            set.verify_topology_index(1).await.unwrap(),
            TopologyIndexStatus::Restamped
        );
        assert_eq!(stored_stamp(b.as_ref()).await.generation, 8);
    }

    #[tokio::test]
    async fn cas_race_accepts_only_the_exact_intended_stamp() {
        let storage = Arc::new(InMemStorage::default());
        seed_stamp(storage.as_ref(), &["old"], 2).await;
        let (_, stale_etag) = storage
            .get_with_etag(TOPOLOGY_STAMP_KEY)
            .await
            .unwrap()
            .unwrap();
        let intended_names = names(&["east", "west"]);
        let intended_hash = topology_hash(&intended_names);
        let intended = encode_stamp(&intended_names, &intended_hash, 3).unwrap();

        // Another process wins with the same target. Our stale CAS loses, and
        // the required re-read accepts exactly that result.
        assert!(storage
            .put_if_match(TOPOLOGY_STAMP_KEY, &stale_etag, intended.clone())
            .await
            .unwrap()
            .is_some());
        assert!(storage
            .put_if_match(TOPOLOGY_STAMP_KEY, &stale_etag, intended)
            .await
            .unwrap()
            .is_none());
        let handle = memory_handle("east", &storage);
        assert!(
            verify_raced_stamp(&handle, 0, &intended_hash, &intended_names, 3, &|_, _| {
                false
            },)
            .await
            .unwrap()
        );

        // A conflicting winner is never accepted as a successful race.
        storage
            .put_bytes(TOPOLOGY_STAMP_KEY, stamp_bytes(&["foreign"], 3), None)
            .await
            .unwrap();
        let error = verify_raced_stamp(&handle, 0, &intended_hash, &intended_names, 3, &|_, _| {
            false
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("different bucket topology"));
    }

    #[test]
    fn switch_bumps_generation_and_selects() {
        let set = BucketSet::new(vec![
            handle("east", "/tmp/pypiron-bucketset-east"),
            handle("west", "/tmp/pypiron-bucketset-west"),
        ]);
        assert!(set.is_multi());
        let before = set.pin();
        assert_eq!(before.index, 0);
        assert_eq!(before.generation, 0);

        let after = set.switch(1);
        assert_eq!(after.index, 1);
        assert_eq!(after.generation, 1);
        // A context pinned before the switch keeps its old handle (design §3).
        assert!(!Arc::ptr_eq(&before.storage, &after.storage));
        // New pins observe the new selection.
        assert_eq!(set.pin().index, 1);
        assert_eq!(set.pin().generation, 1);
    }

    #[test]
    fn read_pin_aliases_write_pin_until_read_affinity_is_active() {
        let set = BucketSet::new(vec![
            handle("east", "/tmp/pypiron-bucketset-read-a"),
            handle("west", "/tmp/pypiron-bucketset-read-b"),
        ]);
        assert!(!set.read_affinity_active());
        // No distinct read selection: read_pin observes the same context as pin,
        // and a write switch carries reads with it.
        assert!(Arc::ptr_eq(&set.read_pin().storage, &set.pin().storage));
        set.switch(1);
        assert_eq!(set.read_pin().index, 1);
        assert_eq!(set.read_pin().generation, set.pin().generation);
    }

    #[test]
    fn read_switch_moves_read_pin_and_shares_one_generation() {
        let set = BucketSet::new(vec![
            handle("east", "/tmp/pypiron-bucketset-read-c"),
            handle("west", "/tmp/pypiron-bucketset-read-d"),
        ]);
        let write_before = set.pin();
        assert_eq!(write_before.index, 0);

        let read = set.switch_read(1);
        assert!(set.read_affinity_active());
        assert_eq!(read.index, 1);
        assert_eq!(read.generation, 1);

        // The write pin's *bucket* is unchanged, but it adopts the bumped
        // generation so both pins carry one generation from within their Arc.
        let write_after = set.pin();
        assert_eq!(write_after.index, 0);
        assert!(Arc::ptr_eq(&write_after.storage, &write_before.storage));
        assert_eq!(write_after.generation, 1);
        assert_eq!(set.read_pin().index, 1);
        assert_eq!(set.read_pin().generation, write_after.generation);

        // A subsequent write switch keeps the read pin's bucket and lifts both to
        // the same new generation.
        set.switch(1);
        assert_eq!(set.pin().generation, set.read_pin().generation);
        assert_eq!(set.read_pin().index, 1);
    }

    #[test]
    fn seed_read_pin_activates_reads_without_bumping_generation() {
        let set = BucketSet::new(vec![
            handle("east", "/tmp/pypiron-bucketset-read-e"),
            handle("west", "/tmp/pypiron-bucketset-read-f"),
        ]);
        set.seed_read_pin(1);
        assert!(set.read_affinity_active());
        assert_eq!(set.read_pin().index, 1);
        // No switch happened, so the shared generation stays 0 and both pins agree.
        assert_eq!(set.read_pin().generation, 0);
        assert_eq!(set.pin().generation, 0);
    }

    #[test]
    fn topology_hash_is_deterministic_and_order_sensitive() {
        let a = vec!["east".to_string(), "west".to_string()];
        let b = vec!["west".to_string(), "east".to_string()];
        assert_eq!(topology_hash(&a), topology_hash(&a));
        assert_ne!(topology_hash(&a), topology_hash(&b));
        // The NUL join is unambiguous: ["ab","c"] must not collide with ["a","bc"].
        let x = vec!["ab".to_string(), "c".to_string()];
        let y = vec!["a".to_string(), "bc".to_string()];
        assert_ne!(topology_hash(&x), topology_hash(&y));
    }

    #[test]
    fn scheme_qualified_identities_distinguish_same_name_across_backends() {
        let s3 = Arc::new(InMemStorage::default());
        let gs = Arc::new(InMemStorage::default());
        // A legal mixed list: same bare name, different backend. The
        // scheme-qualified identities differ, so duplicate detection accepts it.
        let ok = BucketSet::new(vec![
            memory_handle("s3://shared", &s3),
            memory_handle("gs://shared", &gs),
        ]);
        assert!(ok.validate_topology_config().is_ok());

        // The same identity twice is a real duplicate and stays refused.
        let dup = BucketSet::new(vec![
            memory_handle("s3://shared", &s3),
            memory_handle("s3://shared", &gs),
        ]);
        let err = dup.validate_topology_config().unwrap_err();
        assert!(err.to_string().contains("duplicate bucket identity"));
    }

    #[test]
    fn cross_scheme_stamp_is_refused() {
        // s3://iron and gs://iron are different buckets: neither the hash nor
        // the identity check may treat a stamp from one as the other.
        let expected = names(&["s3://iron"]);
        let expected_hash = topology_hash(&expected);
        let foreign_names = names(&["gs://iron"]);
        assert_ne!(expected_hash, topology_hash(&foreign_names));
        let foreign = TopologyStamp {
            buckets: foreign_names.clone(),
            hash: topology_hash(&foreign_names),
            generation: 0,
        };
        let err = check_stamp_identity("iron", &foreign, &expected_hash, &expected).unwrap_err();
        assert!(err.to_string().contains("different bucket topology"));
    }

    #[test]
    fn stamp_round_trips_through_json() {
        let names = vec!["iron-east".to_string(), "iron-west".to_string()];
        let stamp = TopologyStamp {
            buckets: names.clone(),
            hash: topology_hash(&names),
            generation: 0,
        };
        let bytes = serde_json::to_vec(&stamp).unwrap();
        let found = parse_stamp("iron-east", &bytes).unwrap();
        check_stamp_identity("iron-east", &found, &stamp.hash, &names).unwrap();

        let wrong = topology_hash(&["other".to_string()]);
        let err = check_stamp_identity("iron-east", &found, &wrong, &names).unwrap_err();
        assert!(err.to_string().contains("different bucket topology"));
    }
}
