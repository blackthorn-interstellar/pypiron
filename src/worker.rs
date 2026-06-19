//! Index rebuild worker: dirty markers, not a queue.
//!
//! Markers are unique, create-only event keys:
//! `_dirty/<pkg>!<nonce>.intent` written *before* a writer touches truth, and
//! `_dirty/<pkg>!<nonce>.commit` written *after*. Because every event is its
//! own key, the worker can rebuild FIRST and then delete exactly the keys it
//! observed — a concurrent writer's new marker is a new key and survives
//! untouched, and a crash mid-rebuild leaves the keys in place for the next
//! tick. At-least-once processing is free: rebuilds derive views from current
//! truth, so duplicates converge.
//!
//! The intent/commit pair is what makes a crashed writer heal without any
//! sweep: a commit (or an intent whose pair arrived) rebuilds immediately; an
//! unpaired intent younger than the grace period means a writer is still in
//! flight, so the package is skipped this tick; an unpaired intent older than
//! the grace period is a crashed writer — rebuild and consume it. Markers are
//! never deleted unprocessed, so no event is ever lost.
//!
//! Legacy flat markers (`_dirty/<pkg>`, no `!`) are treated as commits so an
//! upgraded node drains what an old node wrote.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, bail, Result};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::lease::LeaseManager;
use crate::names::infer_version_from_filename;
use crate::render::{
    pep503_global_html, pep503_package_html, pep691_global_json, pep691_package_json, FileMetadata,
    SIMPLE_HTML_CONTENT_TYPE, SIMPLE_JSON_CONTENT_TYPE,
};
use crate::sidecar::{
    is_artifact, sidecar_key, Sidecar, Yanked, METADATA_SUFFIX, PROVENANCE_SUFFIX, SIDECAR_SUFFIX,
};
use crate::storage::{FileEntry, ObjectMeta, Storage};
use crate::{AppState, DIRTY_PREFIX, PACKAGES_PREFIX, SIMPLE_PREFIX};

/// Bounded fan-out for storage round-trips during rebuilds and sweeps.
/// High enough to collapse per-file latency, low enough to never matter
/// against S3 request limits or this process's memory. 64 sidecar reads in
/// flight took a 5,000-file package rebuild from 17 s to a few seconds;
/// sidecars are sub-KB objects, far below any S3 prefix limit.
const SIDECAR_READ_CONCURRENCY: usize = 64;
const PACKAGE_SWEEP_CONCURRENCY: usize = 8;

const INTENT_SUFFIX: &str = ".intent";
const COMMIT_SUFFIX: &str = ".commit";

/// Unique per-event marker id: wall nanos + pid + process-local counter.
/// Uniqueness is what makes delete-after-rebuild race-free.
fn marker_nonce() -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{nanos}-{}-{seq}", std::process::id())
}

/// Declare "I am about to change truth for `pkg`". Returns the nonce the
/// writer must commit with. If the writer dies, the intent goes stale and the
/// worker rebuilds anyway after the grace period.
pub async fn mark_intent(storage: &dyn Storage, pkg: &str) -> Result<String> {
    let nonce = marker_nonce();
    put_marker(storage, pkg, &nonce, INTENT_SUFFIX).await?;
    Ok(nonce)
}

/// Declare "truth changed for `pkg`": rebuild as soon as possible.
pub async fn mark_commit(storage: &dyn Storage, pkg: &str, nonce: &str) -> Result<()> {
    put_marker(storage, pkg, nonce, COMMIT_SUFFIX).await
}

/// Write an empty event marker `_dirty/<pkg>!<nonce><suffix>`.
async fn put_marker(storage: &dyn Storage, pkg: &str, nonce: &str, suffix: &str) -> Result<()> {
    storage
        .put_bytes(
            &format!("{DIRTY_PREFIX}{pkg}!{nonce}{suffix}"),
            Vec::new(),
            None,
        )
        .await
}

/// Mark a package as needing an index rebuild (an unpaired commit event, for
/// callers whose truth change already happened).
pub async fn mark_dirty(storage: &dyn Storage, pkg: &str) -> Result<()> {
    mark_commit(storage, pkg, &marker_nonce()).await
}

/// One parsed `_dirty/` entry.
struct Marker {
    key: String,
    nonce: Option<String>,
    is_commit: bool,
    /// Storage last-modified — staleness comes from the storage clock.
    written_at: Option<time::OffsetDateTime>,
}

/// Split a marker key into (package, marker). Legacy `_dirty/<pkg>` keys
/// parse as nonce-less commits.
fn parse_marker(entry: &FileEntry) -> Option<(String, Marker)> {
    let rest = entry.key.strip_prefix(DIRTY_PREFIX)?;
    let written_at = entry.last_modified.as_deref().and_then(|ts| {
        time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339).ok()
    });
    let Some((pkg, event)) = rest.split_once('!') else {
        return Some((
            rest.to_string(),
            Marker {
                key: entry.key.clone(),
                nonce: None,
                is_commit: true,
                written_at,
            },
        ));
    };
    let (nonce, is_commit) = if let Some(n) = event.strip_suffix(COMMIT_SUFFIX) {
        (n, true)
    } else if let Some(n) = event.strip_suffix(INTENT_SUFFIX) {
        (n, false)
    } else {
        // Unknown suffix: treat as a commit so nothing rots in the prefix.
        (event, true)
    };
    Some((
        pkg.to_string(),
        Marker {
            key: entry.key.clone(),
            nonce: Some(nonce.to_string()),
            is_commit,
            written_at,
        },
    ))
}

pub async fn run_worker_until(
    state: Arc<AppState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    // Only the index writer is singular, and only as a cost optimization:
    // rebuilds are idempotent, so the lease is sloppy. Disk is single-node
    // and skips leasing entirely.
    let lease = state
        .storage
        .supports_leases()
        .then(|| LeaseManager::new(state.storage.clone(), state.lease_ttl));

    // Markers are the primary freshness mechanism; the audit is the safety
    // net for what events cannot see (restores, out-of-band storage changes,
    // a peer that died without committing). The first leader audit runs
    // immediately (unless --audit-on-boot=false), so a restored backup heals
    // without waiting an interval. The audit runs on its own task: a deep
    // pass over a large corpus takes minutes of storage round-trips, and
    // running it inline starved dirty-marker processing for its whole
    // duration. Concurrent audit + tick rebuilds of the same package are
    // safe — rebuilds are idempotent.
    let mut last_audit: Option<Instant> = if state.audit_on_boot {
        None
    } else {
        Some(Instant::now())
    };
    // Adaptive spacing: never spend more than ~1/10th of wall time auditing,
    // no matter how the interval is configured relative to corpus size.
    let last_audit_secs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sweep_running = Arc::new(AtomicBool::new(false));
    // Clears the in-flight flag on drop — including a panic unwind inside the
    // spawned audit. Without it, a panicking sweep leaves the flag stuck `true`
    // and no further sweep is ever scheduled, silently disabling self-healing.
    struct SweepGuard(Arc<AtomicBool>);
    impl Drop for SweepGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    loop {
        let is_leader = match &lease {
            None => true,
            Some(lm) => lm.is_leader().await,
        };
        if is_leader {
            let spacing = state.reconcile_interval.max(std::time::Duration::from_secs(
                last_audit_secs.load(Ordering::Relaxed) * 10,
            ));
            let due = last_audit.is_none_or(|t| t.elapsed() >= spacing);
            if due && !sweep_running.swap(true, Ordering::SeqCst) {
                last_audit = Some(Instant::now());
                let state = state.clone();
                let duration_out = last_audit_secs.clone();
                let guard = SweepGuard(sweep_running.clone());
                tokio::spawn(async move {
                    // Held for the task's lifetime; its Drop clears the flag on
                    // normal return or panic. Bound to a name (not `_`) so it
                    // isn't dropped immediately.
                    let _guard = guard;
                    let started = Instant::now();
                    if let Err(e) = audit(&state, false).await {
                        error!(error=?e, "audit failed");
                    }
                    duration_out.store(started.elapsed().as_secs(), Ordering::Relaxed);
                });
            }
            if let Err(e) = tick(&state).await {
                error!(error=?e, "worker tick failed");
            }
        }
        tokio::select! {
            _ = sleep(state.worker_interval) => {}
            _ = state.worker_nudge.notified() => {}
            _ = shutdown.changed() => break,
        }
    }
    // Graceful exit: hand leadership over instead of leaving successors to
    // wait out the lease TTL (a restart used to be a TTL-long write outage).
    if let Some(lm) = &lease {
        lm.release().await;
    }
}

/// Fingerprint shards live here, one JSON map per [`SHARD_CHARS`] character:
/// package → hash of the (key, size, etag) listing its views were built
/// from. They are views of views — regenerable, never trusted over truth. A
/// lost shard merely means its packages rebuild once.
const STATE_PREFIX: &str = "_state/";

/// Audit sweep: detect-and-repair with cost proportional to *churn*, not
/// corpus size. One flat listing per shard (1,000 keys per S3 request)
/// covers truth and views; a package whose listing fingerprint matches the
/// one stored at its last rebuild is provably unchanged — zero reads. Only
/// the diff gets the deep treatment (sidecar reads, view rewrite, sidecar
/// backfill, orphan pruning). `force_deep` ignores stored fingerprints and
/// rebuilds everything — that is `pypiron resync`.
pub async fn audit(state: &AppState, force_deep: bool) -> Result<()> {
    let started = Instant::now();
    let mut live: Vec<String> = Vec::new();
    let mut dead: Vec<String> = Vec::new();
    let mut failures = 0usize;
    let mut rebuilt = 0usize;
    let mut skipped = 0usize;
    let mut files = 0u64;
    let mut releases = 0u64;

    // Shards enumerate in parallel — that is what the sharding is for. The
    // bound keeps peak memory at a few shards' worth of listings (a shard is
    // ~1/36th of the corpus).
    const SHARD_CONCURRENCY: usize = 6;
    for chunk in crate::storage::SHARD_CHARS.chunks(SHARD_CONCURRENCY) {
        let audits = chunk
            .iter()
            .map(|shard| audit_shard(state, *shard, force_deep));
        for (shard, result) in chunk.iter().zip(futures::future::join_all(audits).await) {
            match result {
                Ok(result) => {
                    live.extend(result.live);
                    dead.extend(result.dead);
                    rebuilt += result.rebuilt;
                    skipped += result.skipped;
                    failures += result.failures;
                    files += result.files;
                    releases += result.releases;
                }
                Err(e) => {
                    error!(shard=%shard, error=?e, "audit: shard failed");
                    failures += 1;
                }
            }
        }
    }

    live.sort();
    live.dedup();
    // Delta + CAS, not a blind overwrite: a package born mid-audit (its name
    // added by the tick) must not be clobbered by our older observation.
    update_global_index(state, &live, &dead).await?;
    if failures > 0 {
        return Err(anyhow!("audit finished with {failures} failure(s)"));
    }
    let duration_secs = started.elapsed().as_secs_f64();
    let m = &state.metrics;
    m.reconcile_sweeps
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    m.audit_packages_rebuilt
        .fetch_add(rebuilt as u64, std::sync::atomic::Ordering::Relaxed);
    m.audit_packages_skipped
        .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
    m.set_audit_duration(duration_secs);
    // `live` is sorted+deduped above, so its length is the distinct project
    // count; releases/files were summed straight off the shard listings.
    m.set_inventory(live.len() as u64, releases, files);
    info!(
        packages = live.len(),
        rebuilt, skipped, duration_secs, "reconcile: sweep complete"
    );
    Ok(())
}

struct ShardAudit {
    live: Vec<String>,
    /// Observed with no artifacts: must not be listed globally.
    dead: Vec<String>,
    rebuilt: usize,
    /// Provably unchanged (fingerprint hit): zero reads spent.
    skipped: usize,
    failures: usize,
    /// Inventory derived from the shard listing (no extra reads): artifact
    /// files (sidecars excluded) and distinct `(project, version)` releases.
    files: u64,
    releases: u64,
}

/// Audit every package whose name starts with `shard`.
async fn audit_shard(state: &AppState, shard: char, force_deep: bool) -> Result<ShardAudit> {
    let (truth, views) = futures::future::try_join(
        state.storage.list_all(&format!("{PACKAGES_PREFIX}{shard}")),
        state.storage.list_all(&format!("{SIMPLE_PREFIX}{shard}")),
    )
    .await?;

    // Group listings by package; the global index files ("index.json" under
    // simple/i...) have no '/' and are skipped — they are handled globally.
    let mut by_pkg: std::collections::BTreeMap<String, (Vec<&ObjectMeta>, Vec<&ObjectMeta>)> =
        std::collections::BTreeMap::new();
    for obj in &truth {
        if let Some(pkg) = key_package(&obj.key, PACKAGES_PREFIX) {
            by_pkg.entry(pkg.to_string()).or_default().0.push(obj);
        }
    }
    for obj in &views {
        if let Some(pkg) = key_package(&obj.key, SIMPLE_PREFIX) {
            by_pkg.entry(pkg.to_string()).or_default().1.push(obj);
        }
    }

    let fp_key = format!("{STATE_PREFIX}fp-{shard}.json");
    let stored: std::collections::HashMap<String, String> = if force_deep {
        Default::default()
    } else {
        match state.storage.get_bytes(&fp_key).await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => Default::default(),
        }
    };

    let mut out = ShardAudit {
        live: Vec::new(),
        dead: Vec::new(),
        rebuilt: 0,
        skipped: 0,
        failures: 0,
        files: 0,
        releases: 0,
    };
    let mut fresh: std::collections::HashMap<String, String> = Default::default();
    let mut packages: Vec<(String, String, bool)> = Vec::with_capacity(by_pkg.len());
    for (pkg, (t, v)) in by_pkg {
        let fp = fingerprint(&t, &v);
        // Count artifacts and distinct versions straight off the listing — the
        // same bytes the fingerprint already walked, so the inventory is free.
        let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
        let mut versions: HashSet<String> = HashSet::new();
        let mut file_count = 0u64;
        for obj in &t {
            if let Some(filename) = obj.key.strip_prefix(&prefix) {
                if is_artifact(filename) {
                    file_count += 1;
                    if let Some(version) = infer_version_from_filename(filename) {
                        versions.insert(version);
                    }
                }
            }
        }
        out.files += file_count;
        out.releases += versions.len() as u64;
        packages.push((pkg, fp, file_count > 0));
    }

    for chunk in packages.chunks(PACKAGE_SWEEP_CONCURRENCY) {
        let jobs = chunk.iter().map(|(pkg, fp, has_artifacts)| {
            let unchanged = stored.get(pkg.as_str()) == Some(fp);
            let (pkg, fp, has_artifacts) = (pkg.clone(), fp.clone(), *has_artifacts);
            async move {
                if unchanged {
                    // Provably unchanged since the fingerprint was written.
                    return (pkg, Some(fp), has_artifacts, false, false);
                }
                match rebuild_package(state, &pkg).await {
                    Ok(live_now) => {
                        // Fingerprint what the rebuild actually saw/wrote, not
                        // the pre-rebuild listing — two cheap per-package lists.
                        let new_fp = package_fingerprint(state, &pkg).await.ok();
                        (pkg, new_fp, live_now, true, false)
                    }
                    Err(e) => {
                        // Conservative on failure: keep the package listed and
                        // its views rather than pruning on a bad observation.
                        error!(package=%pkg, error=?e, "audit: package rebuild failed");
                        (pkg, None, has_artifacts, false, true)
                    }
                }
            }
        });
        for (pkg, fp, live_now, was_rebuilt, failed) in futures::future::join_all(jobs).await {
            if let Some(fp) = fp {
                fresh.insert(pkg.clone(), fp);
            }
            if live_now || failed {
                out.live.push(pkg);
            } else {
                out.dead.push(pkg);
            }
            out.rebuilt += was_rebuilt as usize;
            out.skipped += (!was_rebuilt && !failed) as usize;
            out.failures += failed as usize;
        }
    }

    // `fresh` now holds exactly the packages that exist; anything left in
    // `stored` is gone and simply drops out of the rewritten shard.
    let bytes = serde_json::to_vec(&std::collections::BTreeMap::from_iter(fresh.iter()))?;
    put_if_changed(state, &fp_key, bytes, "application/json").await?;
    Ok(out)
}

/// The package a key belongs to: first path segment after `prefix`.
fn key_package<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    key.strip_prefix(prefix)?.split_once('/').map(|(p, _)| p)
}

/// Hash of everything a package's views are derived from, as observed in a
/// flat listing: truth objects (artifacts decide membership, sidecar etags
/// carry yank/metadata changes) plus the view objects themselves (so
/// out-of-band view deletion or tampering is also caught).
fn fingerprint(truth: &[&ObjectMeta], views: &[&ObjectMeta]) -> String {
    let mut hasher = Sha256::new();
    for obj in truth.iter().chain(views.iter()) {
        hasher.update(&obj.key);
        hasher.update(obj.size.to_le_bytes());
        hasher.update(&obj.etag);
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

/// Re-derive one package's fingerprint from fresh listings (post-rebuild).
async fn package_fingerprint(state: &AppState, pkg: &str) -> Result<String> {
    let (truth, views) = futures::future::try_join(
        state.storage.list_all(&format!("{PACKAGES_PREFIX}{pkg}/")),
        state.storage.list_all(&format!("{SIMPLE_PREFIX}{pkg}/")),
    )
    .await?;
    Ok(fingerprint(
        &truth.iter().collect::<Vec<_>>(),
        &views.iter().collect::<Vec<_>>(),
    ))
}

async fn tick(state: &Arc<AppState>) -> Result<()> {
    let entries = state.storage.list_dir_entries(DIRTY_PREFIX).await?;
    if entries.is_empty() {
        return Ok(());
    }

    // Group events per package and decide what is consumable now.
    let now = time::OffsetDateTime::now_utc();
    let mut per_pkg: std::collections::HashMap<String, Vec<Marker>> =
        std::collections::HashMap::new();
    for entry in &entries {
        if let Some((pkg, marker)) = parse_marker(entry) {
            per_pkg.entry(pkg).or_default().push(marker);
        }
    }

    let mut work: Vec<(String, Vec<String>)> = Vec::new();
    // Per package, how many unpaired-but-stale intents we are about to heal —
    // counted into the metric only once the rebuild actually consumes them.
    let mut stale_by_pkg: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (pkg, markers) in per_pkg {
        let commit_nonces: HashSet<&str> = markers
            .iter()
            .filter(|m| m.is_commit)
            .filter_map(|m| m.nonce.as_deref())
            .collect();
        let mut stale_healed = 0u64;
        let consumable: Vec<String> = markers
            .iter()
            .filter(|m| {
                if m.is_commit {
                    return true;
                }
                // Intent: consumable once its commit arrived (the pair is
                // done) or once it is stale (the writer crashed mid-flight).
                let paired = m
                    .nonce
                    .as_deref()
                    .is_some_and(|n| commit_nonces.contains(n));
                let stale = m.written_at.is_none_or(|t| now - t >= state.intent_grace);
                // A stale, never-committed intent is a crashed writer healing.
                if !paired && stale {
                    stale_healed += 1;
                }
                paired || stale
            })
            .map(|m| m.key.clone())
            .collect();
        // Only fresh unpaired intents: a writer is mid-flight; its commit (or
        // staleness) will bring the package back.
        if !consumable.is_empty() {
            if stale_healed > 0 {
                stale_by_pkg.insert(pkg.clone(), stale_healed);
            }
            work.push((pkg, consumable));
        }
    }
    if work.is_empty() {
        return Ok(());
    }
    info!(
        packages = work.len(),
        markers = entries.len(),
        "worker: processing dirty markers"
    );

    // Packages drain with bounded concurrency: rebuilds are idempotent, so
    // parallelism across packages is free. A semaphore (not chunked join_all)
    // so one slow 5,000-file rebuild never head-of-line blocks the tiny
    // rebuilds behind it — that stall showed up as a 73s visibility p99 for
    // unrelated packages. One failing package must not starve the namespace.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(PACKAGE_SWEEP_CONCURRENCY));
    let mut handles = Vec::with_capacity(work.len());
    for (pkg, keys) in work {
        let state = state.clone();
        let semaphore = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await;
            let rebuilt = match rebuild_package(&state, &pkg).await {
                Ok(has_artifacts) => Some(has_artifacts),
                Err(e) => {
                    error!(package=%pkg, error=?e, "rebuild failed; markers retained for retry");
                    None
                }
            };
            (pkg, keys, rebuilt)
        }));
    }
    let mut failures = 0usize;
    let mut healed = 0u64;
    let (mut adds, mut removes) = (Vec::new(), Vec::new());
    let mut consumed: Vec<String> = Vec::new();
    for handle in handles {
        match handle.await {
            Ok((pkg, keys, Some(live_now))) => {
                // Markers for this package are now consumed; the stale intents
                // among them are healed crashed writers.
                healed += stale_by_pkg.get(&pkg).copied().unwrap_or(0);
                if live_now {
                    adds.push(pkg);
                } else {
                    removes.push(pkg);
                }
                consumed.extend(keys);
            }
            _ => failures += 1,
        }
    }
    if healed > 0 {
        state
            .metrics
            .stale_intents_healed
            .fetch_add(healed, std::sync::atomic::Ordering::Relaxed);
    }
    // One batched global-index pass per tick: mass ingest of N new packages
    // rewrites the (corpus-sized) global views once, not N times.
    update_global_index(state, &adds, &removes).await?;
    // Markers are consumed LAST — they are the transaction log, and must
    // outlive every write they announce (package views above, global index
    // here). Rebuild-then-delete is race-free because keys are unique: an
    // event arriving during the rebuild is a new key and survives. A crash
    // anywhere before this line replays the whole tick — idempotent, so the
    // only cost is repeated work, never a lost update.
    if let Err(e) = state.storage.delete_keys(&consumed).await {
        warn!(error=?e, "could not consume markers; rebuilds will repeat");
    }
    if failures > 0 {
        return Err(anyhow!("{failures} package(s) failed this tick"));
    }
    Ok(())
}

/// Regenerate one package's indexes from a storage listing.
/// Returns whether the package still has artifacts; with none, its indexes
/// are removed (index first, per the ordering invariant).
pub async fn rebuild_package(state: &AppState, pkg: &str) -> Result<bool> {
    rebuild_package_excluding(state, pkg, None).await
}

/// Like `rebuild_package`, but omitting one filename from the views. Deletion
/// uses this to drop the file from the index *before* removing the artifact —
/// views may lag truth but never lead it.
pub async fn rebuild_package_excluding(
    state: &AppState,
    pkg: &str,
    omit: Option<&str>,
) -> Result<bool> {
    state
        .metrics
        .index_rebuilds
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut files = list_artifacts(state, pkg).await?;
    if let Some(omit) = omit {
        files.retain(|f| f.filename != omit);
    }
    if files.is_empty() {
        let keys = [
            format!("{SIMPLE_PREFIX}{pkg}/index.html"),
            format!("{SIMPLE_PREFIX}{pkg}/index.json"),
        ];
        state.storage.delete_keys(&keys).await?;
        for key in &keys {
            state.index_cache.invalidate(key);
        }
        return Ok(false);
    }
    write_pkg_indexes(state, pkg, &files).await?;
    Ok(true)
}

/// The in-memory copy of the global index's name set, pinned to the ETag of
/// the materialized JSON it was loaded from (None on backends without ETags).
/// At 780k names a membership check against storage costs a 45 MB GET +
/// parse; against this it costs a hash lookup.
pub(crate) struct GlobalNames {
    etag: Option<String>,
    names: HashSet<String>,
}

impl GlobalNames {
    /// Number of package names in the materialized global index — the
    /// dashboard's "packages hosted" figure, a free in-memory count.
    pub(crate) fn len(&self) -> usize {
        self.names.len()
    }
}

/// All hosted package names, sorted — the human package browser's listing.
/// Loads the global name set into memory on first use (same source and cache as
/// the dashboard's count), so a freshly booted node still answers.
pub async fn global_package_names(state: &AppState) -> Result<Vec<String>> {
    let mut guard = state.global_names.lock().await;
    if guard.is_none() {
        *guard = Some(load_global_names(state).await?);
    }
    let mut names: Vec<String> = guard
        .as_ref()
        .expect("just loaded")
        .names
        .iter()
        .cloned()
        .collect();
    names.sort();
    Ok(names)
}

/// The global index only changes when the *set of package names* changes —
/// check membership in memory first; the common case (an upload to a known
/// package) costs nothing. Real changes are applied as a delta and written
/// back under CAS (`If-Match`) where the backend supports it, so two nodes
/// adding different names can never clobber each other: the loser reloads
/// and reapplies. Deltas batch per worker tick, so mass ingest rewrites the
/// (large) global index once per tick, not once per package.
async fn update_global_index(state: &AppState, adds: &[String], removes: &[String]) -> Result<()> {
    if adds.is_empty() && removes.is_empty() {
        return Ok(());
    }
    let mut guard = state.global_names.lock().await;
    // Once we lose a CAS we have already written an optimistic HTML for a name
    // set that lost. If the reload then makes our delta a no-op (the winner
    // already added our name), `changed` is false and we would return leaving
    // that stale HTML as the final write — a drift nothing else heals (the
    // audit reaches the same `changed` gate). So on that path, reconcile HTML
    // to the now-canonical set before returning.
    let mut wrote_optimistic_html = false;
    for _attempt in 0..4 {
        if guard.is_none() {
            *guard = Some(load_global_names(state).await?);
        }
        let cached = guard.as_mut().expect("just loaded");
        let mut changed = false;
        for pkg in adds {
            changed |= cached.names.insert(pkg.clone());
        }
        for pkg in removes {
            changed |= cached.names.remove(pkg);
        }
        if !changed {
            if wrote_optimistic_html {
                let mut packages: Vec<String> = cached.names.iter().cloned().collect();
                packages.sort();
                put_if_changed(
                    state,
                    &format!("{SIMPLE_PREFIX}index.html"),
                    pep503_global_html(&packages).into_bytes(),
                    SIMPLE_HTML_CONTENT_TYPE,
                )
                .await?;
            }
            return Ok(());
        }
        let mut packages: Vec<String> = cached.names.iter().cloned().collect();
        packages.sort();
        match write_global_indexes_cas(state, &packages, &cached.etag.clone()).await? {
            CasOutcome::Won(new_etag) => {
                if let Some(cached) = guard.as_mut() {
                    // Pin the ETag the conditional write itself returned, not one
                    // from a follow-up GET — a peer could land a write between the
                    // two and we'd pin its ETag against our stale name set.
                    cached.etag = new_etag;
                }
                return Ok(());
            }
            CasOutcome::Lost => {}
        }
        // Lost the CAS to a peer: another node updated the name set under us.
        // We already wrote an optimistic HTML this iteration; remember that so
        // a subsequent no-op reload still reconciles it. Count the conflict
        // (operators watch this to confirm dual leadership converges rather
        // than corrupts), then drop the cache, reload, reapply the delta.
        wrote_optimistic_html = true;
        state
            .metrics
            .global_cas_conflicts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!(
            adds = adds.len(),
            removes = removes.len(),
            "global index CAS lost to a peer; reloading and retrying"
        );
        *guard = None;
    }
    bail!("global index CAS retries exhausted")
}

/// Load the global name set (and its ETag) from the materialized JSON.
async fn load_global_names(state: &AppState) -> Result<GlobalNames> {
    let key = format!("{SIMPLE_PREFIX}index.json");
    let (bytes, etag) = if state.storage.supports_leases() {
        match state.storage.get_with_etag(&key).await? {
            Some((bytes, etag)) => (bytes, Some(etag)),
            None => (Vec::new(), None),
        }
    } else {
        (
            state.storage.get_bytes(&key).await.unwrap_or_default(),
            None,
        )
    };
    #[derive(serde::Deserialize)]
    struct Global {
        projects: Vec<Project>,
    }
    #[derive(serde::Deserialize)]
    struct Project {
        name: String,
    }
    let names = match serde_json::from_slice::<Global>(&bytes) {
        Ok(g) => g.projects.into_iter().map(|p| p.name).collect(),
        Err(_) => HashSet::new(),
    };
    Ok(GlobalNames { etag, names })
}

/// Outcome of a global-index conditional write.
enum CasOutcome {
    /// Won; carries the authoritative new ETag (`None` on non-CAS disk backends).
    Won(Option<String>),
    /// Lost the conditional write to a concurrent leader; caller should reload.
    Lost,
}

/// Write both global views. The canonical JSON — the one `changed` detection
/// reloads from — is written LAST, under CAS where supported, so that a crash
/// between the two writes is healed by replay: stale JSON re-detects the change
/// and rewrites both. (JSON-first stranded a stale HTML that the name-set-change
/// gate never revisited without an audit; the disk path already orders it this
/// way.) HTML is last-writer-wins; a racing loser rewrites it on its retry, so
/// the iteration whose JSON CAS finally wins always left a matching HTML.
/// Returns `CasOutcome::Lost` when the conditional write lost the race, else
/// `CasOutcome::Won` with the ETag the put itself returned.
async fn write_global_indexes_cas(
    state: &AppState,
    packages: &[String],
    expected_etag: &Option<String>,
) -> Result<CasOutcome> {
    let json_key = format!("{SIMPLE_PREFIX}index.json");
    let json = pep691_global_json(packages).into_bytes();
    if state.storage.supports_leases() {
        // HTML first: derived from the same list, unconditional, idempotent.
        let html_key = format!("{SIMPLE_PREFIX}index.html");
        state
            .storage
            .put_bytes(
                &html_key,
                pep503_global_html(packages).into_bytes(),
                Some(SIMPLE_HTML_CONTENT_TYPE),
            )
            .await?;
        state.index_cache.invalidate(&html_key);
        // Canonical JSON last, under CAS: its success is what consumes markers.
        let outcome = match expected_etag {
            Some(etag) => state.storage.put_if_match(&json_key, etag, json).await?,
            None => state.storage.put_if_none_match(&json_key, json).await?,
        };
        let Some(new_etag) = outcome else {
            return Ok(CasOutcome::Lost);
        };
        state.index_cache.invalidate(&json_key);
        return Ok(CasOutcome::Won(Some(new_etag)));
    }
    write_global_indexes(state, packages).await?;
    Ok(CasOutcome::Won(None))
}

/// List a package's artifacts with metadata from sidecars — O(files), no hashing.
/// Artifacts without a sidecar (legacy files) get one backfilled, hashing once.
pub async fn list_artifacts(state: &AppState, pkg: &str) -> Result<Vec<FileMetadata>> {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let entries = state.storage.list_dir_entries(&prefix).await?;
    let names: HashSet<&str> = entries
        .iter()
        .filter_map(|e| e.key.strip_prefix(&prefix))
        .collect();

    // Sidecar reads fan out with bounded concurrency: a 2,000-file package
    // costs 2,000 GETs, and doing them serially put rebuilds at minutes of
    // wall clock on S3. Chunked join_all keeps listing order — index output
    // must stay deterministic.
    let artifacts: Vec<(&FileEntry, &str)> = entries
        .iter()
        .filter_map(|entry| {
            let filename = entry.key.strip_prefix(&prefix)?;
            is_artifact(filename).then_some((entry, filename))
        })
        .collect();
    let mut metadata = Vec::with_capacity(artifacts.len());
    for chunk in artifacts.chunks(SIDECAR_READ_CONCURRENCY) {
        let loaded = futures::future::join_all(
            chunk
                .iter()
                .map(|(entry, filename)| load_file_metadata(state, entry, filename, &names)),
        )
        .await;
        metadata.extend(loaded.into_iter().flatten());
    }
    Ok(metadata)
}

/// Load one artifact's index entry from its sidecar (backfilling if absent).
/// None means "leave it out of the index" — reasons logged inside.
async fn load_file_metadata(
    state: &AppState,
    entry: &FileEntry,
    filename: &str,
    names: &HashSet<&str>,
) -> Option<FileMetadata> {
    let has_sidecar = names.contains(format!("{filename}{SIDECAR_SUFFIX}").as_str());
    let sc = if has_sidecar {
        match read_sidecar(state, &entry.key).await {
            Ok(sc) => sc,
            Err(e) => {
                // A present-but-unreadable sidecar is corruption, not a
                // legacy file. Backfilling would fabricate fresh metadata
                // over it — silently resetting a security yank to false.
                // Leave the file out of the index until an operator looks.
                error!(error=?e, key=%entry.key, "corrupt sidecar; omitting file from index (will not fabricate metadata)");
                return None;
            }
        }
    } else {
        match backfill_sidecar(state, entry, filename).await {
            Ok(sc) => sc,
            Err(e) => {
                warn!(error=?e, key=%entry.key, "could not backfill sidecar; skipping file");
                return None;
            }
        }
    };
    let core_metadata = names.contains(format!("{filename}{METADATA_SUFFIX}").as_str());
    let provenance = names.contains(format!("{filename}{PROVENANCE_SUFFIX}").as_str());
    Some(FileMetadata::from_sidecar(
        filename,
        sc,
        core_metadata,
        provenance,
    ))
}

async fn read_sidecar(state: &AppState, artifact_key: &str) -> Result<Sidecar> {
    let bytes = state.storage.get_bytes(&sidecar_key(artifact_key)).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Hash-once-and-backfill for files that predate write-time sidecars.
/// Storage last-modified is the upload-time fallback (correct by construction
/// for direct uploads — filenames are immutable, so written exactly once).
///
/// Create-only, never overwrite: "missing" was observed in a listing that may
/// already be stale, and a concurrent upload's real sidecar (true timestamp,
/// yank state) must always beat this fabricated one. Losing the race means
/// the real sidecar exists — read and use it.
async fn backfill_sidecar(state: &AppState, entry: &FileEntry, filename: &str) -> Result<Sidecar> {
    let bytes = state.storage.get_bytes(&entry.key).await?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let sc = Sidecar {
        sha256: format!("{:x}", hasher.finalize()),
        size: entry.size,
        version: infer_version_from_filename(filename).unwrap_or_default(),
        upload_time: entry.last_modified.clone().unwrap_or_default(),
        requires_python: None,
        yanked: Yanked::Flag(false),
    };
    let created = state
        .storage
        .put_if_absent(
            &sidecar_key(&entry.key),
            serde_json::to_vec(&sc)?,
            Some("application/json"),
        )
        .await?;
    if !created {
        return read_sidecar(state, &entry.key).await;
    }
    info!(key=%entry.key, "backfilled sidecar");
    Ok(sc)
}

/// Write only if the stored object differs — idempotent rebuilds shouldn't
/// touch storage (or bump mtimes/ETags) when nothing changed. A real write
/// invalidates the in-process index cache so same-node reads are fresh
/// immediately (other nodes are bounded by the cache TTL).
async fn put_if_changed(state: &AppState, key: &str, bytes: Vec<u8>, ct: &str) -> Result<()> {
    if let Ok(current) = state.storage.get_bytes(key).await {
        if current == bytes {
            return Ok(());
        }
    }
    state.storage.put_bytes(key, bytes, Some(ct)).await?;
    state.index_cache.invalidate(key);
    Ok(())
}

async fn write_pkg_indexes(state: &AppState, pkg: &str, files: &[FileMetadata]) -> Result<()> {
    // Status is per-project truth (PEP 792). A read error propagates — we
    // re-render against the prior index rather than assume `active` and, say,
    // re-expose links for a project that should be quarantined.
    let status = crate::status::read_status(state.storage.as_ref(), pkg).await?;
    // Quarantine omits file links; the delete-vs-render decision upstream still
    // keys on the real artifact count, so a quarantined project keeps a
    // status-bearing (link-free) page instead of 404ing.
    let render_files: &[FileMetadata] = if status.status.blocks_downloads() {
        &[]
    } else {
        files
    };
    let html = pep503_package_html(pkg, render_files, &status);
    let json = pep691_package_json(pkg, render_files, &status);

    let base = format!("{SIMPLE_PREFIX}{pkg}/");
    put_if_changed(
        state,
        &format!("{base}index.html"),
        html.into_bytes(),
        SIMPLE_HTML_CONTENT_TYPE,
    )
    .await?;
    put_if_changed(
        state,
        &format!("{base}index.json"),
        json.into_bytes(),
        SIMPLE_JSON_CONTENT_TYPE,
    )
    .await?;
    Ok(())
}

async fn write_global_indexes(state: &AppState, packages: &[String]) -> Result<()> {
    let html = pep503_global_html(packages);
    let json = pep691_global_json(packages);

    put_if_changed(
        state,
        &format!("{SIMPLE_PREFIX}index.html"),
        html.into_bytes(),
        SIMPLE_HTML_CONTENT_TYPE,
    )
    .await?;
    put_if_changed(
        state,
        &format!("{SIMPLE_PREFIX}index.json"),
        json.into_bytes(),
        SIMPLE_JSON_CONTENT_TYPE,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArtifactDelivery;
    use axum::body::Body;
    use http::Response;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool as StdAtomicBool;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Storage stub whose `list_all("packages/...")` never returns — an audit
    /// sweep that takes forever. Everything else is a tiny in-memory object map.
    struct SweepStallsStorage {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        sweep_entered: StdAtomicBool,
    }

    #[async_trait::async_trait]
    impl Storage for SweepStallsStorage {
        async fn head_exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        async fn serve_artifact(&self, _key: &str, _range: Option<&str>) -> Result<Response<Body>> {
            bail!("not used in this test")
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
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
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
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| crate::storage::NotFound(key.to_string()).into())
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
        async fn list_all(&self, prefix: &str) -> Result<Vec<crate::storage::ObjectMeta>> {
            if prefix.starts_with(PACKAGES_PREFIX) {
                // Same stall for the flat-enumeration path the audit uses.
                self.sweep_entered.store(true, Ordering::SeqCst);
                futures::future::pending::<()>().await;
            }
            Ok(Vec::new())
        }
        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            let mut map = self.objects.lock().unwrap();
            for k in keys {
                map.remove(k);
            }
            Ok(())
        }
    }

    /// Regression for write-visibility latency: a dirty marker dropped while
    /// the worker is parked in its sleep must be processed via the nudge in
    /// far less than the tick interval (10s here; the nudge makes it ~ms).
    #[tokio::test]
    async fn nudge_wakes_worker_before_tick() {
        let pkg = "fastpkg";
        let wheel = "fastpkg-1.0-py3-none-any.whl";
        let mut objects = HashMap::new();
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}"),
            b"not-a-real-wheel".to_vec(),
        );
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}{SIDECAR_SUFFIX}"),
            serde_json::to_vec(&Sidecar {
                sha256: "ab".repeat(32),
                size: 16,
                version: "1.0".into(),
                upload_time: "2026-01-01T00:00:00Z".into(),
                requires_python: None,
                yanked: Yanked::Flag(false),
            })
            .unwrap(),
        );
        objects.insert(
            format!("{SIMPLE_PREFIX}index.json"),
            br#"{"projects":[{"name":"fastpkg"}]}"#.to_vec(),
        );
        let storage = Arc::new(SweepStallsStorage {
            objects: Mutex::new(objects),
            sweep_entered: StdAtomicBool::new(false),
        });
        let state = Arc::new(AppState {
            storage: storage.clone(),
            uploader_user: None,
            uploader_pass: None,
            admin_user: None,
            admin_pass: None,
            read_user: None,
            read_pass: None,
            private_prefix: None,
            artifact_delivery: ArtifactDelivery::Auto,
            worker_interval: Duration::from_secs(10),
            reconcile_interval: Duration::from_secs(3600),
            intent_grace: time::Duration::seconds(900),
            audit_on_boot: true,
            sync_uploads: false,
            sync_upload_timeout: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(30),
            index_cache: Arc::new(crate::cache::IndexCache::new(crate::cache::INDEX_CACHE_TTL)),
            presign_cache: Arc::new(crate::cache::PresignCache::new(
                crate::cache::PRESIGN_CACHE_TTL,
            )),
            spool_dir: std::env::temp_dir(),
            global_names: Arc::new(tokio::sync::Mutex::new(None)),
            worker_nudge: Arc::new(tokio::sync::Notify::new()),
            metrics: Arc::new(crate::metrics::Metrics::new()),
            proxy: None,
        });

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(run_worker_until(state.clone(), shutdown_rx));
        // Let the first (empty) tick pass; the worker parks in a 10s sleep.
        tokio::time::sleep(Duration::from_millis(300)).await;

        storage
            .objects
            .lock()
            .unwrap()
            .insert(format!("{DIRTY_PREFIX}{pkg}"), Vec::new());
        state.worker_nudge.notify_one();

        let index_key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut rebuilt = false;
        while Instant::now() < deadline {
            if storage.objects.lock().unwrap().contains_key(&index_key) {
                rebuilt = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        worker.abort();
        assert!(
            rebuilt,
            "nudged marker not processed within 2s — visibility is stuck on the 10s tick"
        );
    }

    /// Regression: a long reconcile sweep must not starve dirty-marker
    /// processing. Before the fix, the sweep ran inline ahead of tick() —
    /// uploads stayed invisible (and sync uploads timed out) for the whole
    /// sweep. Here the sweep literally never finishes, and the marker must
    /// still be processed.
    #[tokio::test]
    async fn dirty_markers_processed_while_sweep_runs() {
        let pkg = "fastpkg";
        let wheel = "fastpkg-1.0-py3-none-any.whl";
        let mut objects = HashMap::new();
        objects.insert(format!("{DIRTY_PREFIX}{pkg}"), Vec::new());
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}"),
            b"not-a-real-wheel".to_vec(),
        );
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}{SIDECAR_SUFFIX}"),
            serde_json::to_vec(&Sidecar {
                sha256: "ab".repeat(32),
                size: 16,
                version: "1.0".into(),
                upload_time: "2026-01-01T00:00:00Z".into(),
                requires_python: None,
                yanked: Yanked::Flag(false),
            })
            .unwrap(),
        );
        // Global index already lists the package, so the tick path skips the
        // global rebuild (which would also hit the stalled list_all).
        objects.insert(
            format!("{SIMPLE_PREFIX}index.json"),
            br#"{"projects":[{"name":"fastpkg"}]}"#.to_vec(),
        );

        let storage = Arc::new(SweepStallsStorage {
            objects: Mutex::new(objects),
            sweep_entered: StdAtomicBool::new(false),
        });
        let state = Arc::new(AppState {
            storage: storage.clone(),
            uploader_user: None,
            uploader_pass: None,
            admin_user: None,
            admin_pass: None,
            read_user: None,
            read_pass: None,
            private_prefix: None,
            artifact_delivery: ArtifactDelivery::Auto,
            worker_interval: Duration::from_millis(10),
            reconcile_interval: Duration::from_secs(3600),
            intent_grace: time::Duration::seconds(900),
            audit_on_boot: true,
            sync_uploads: false,
            sync_upload_timeout: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(30),
            index_cache: Arc::new(crate::cache::IndexCache::new(crate::cache::INDEX_CACHE_TTL)),
            presign_cache: Arc::new(crate::cache::PresignCache::new(
                crate::cache::PRESIGN_CACHE_TTL,
            )),
            spool_dir: std::env::temp_dir(),
            global_names: Arc::new(tokio::sync::Mutex::new(None)),
            worker_nudge: Arc::new(tokio::sync::Notify::new()),
            metrics: Arc::new(crate::metrics::Metrics::new()),
            proxy: None,
        });

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(run_worker_until(state, shutdown_rx));

        let index_key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut rebuilt = false;
        while Instant::now() < deadline {
            if storage.objects.lock().unwrap().contains_key(&index_key) {
                rebuilt = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        worker.abort();

        assert!(
            storage.sweep_entered.load(Ordering::SeqCst),
            "test setup broken: sweep never started"
        );
        assert!(
            rebuilt,
            "dirty marker was not processed while the sweep was running — sweep starves the event path"
        );
    }
}
