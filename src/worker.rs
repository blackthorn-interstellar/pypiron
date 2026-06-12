//! Index rebuild worker: dirty markers, not a queue.
//!
//! Uploads and deletes drop an empty marker at `_dirty/<pkg>` — always after
//! the truth change it announces. Each tick lists the markers, deletes each
//! marker FIRST, then rebuilds that package from a fresh listing. Deleting
//! first matters: the key is shared, so deleting after the rebuild would
//! destroy marks written concurrently during it — and a swallowed delete-mark
//! leaves a listed-but-missing file, the one harmful state. With delete-first,
//! truth-before-marker guarantees the rebuild sees whatever prompted any
//! swallowed mark. A crash between delete and rebuild merely defers to the
//! reconciler. Duplicate markers still collapse into one rebuild for free.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::Result;
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::lease::LeaseManager;
use crate::names::infer_version_from_filename;
use crate::render::{
    pep503_global_html, pep503_package_html, pep691_global_json, pep691_package_json, FileMetadata,
};
use crate::sidecar::{is_artifact, sidecar_key, Sidecar, Yanked, METADATA_SUFFIX, SIDECAR_SUFFIX};
use crate::storage::{FileEntry, Storage};
use crate::{AppState, DIRTY_PREFIX, PACKAGES_PREFIX, SIMPLE_PREFIX};

/// Bounded fan-out for storage round-trips during rebuilds and sweeps.
/// High enough to collapse per-file latency, low enough to never matter
/// against S3 request limits or this process's memory. 64 sidecar reads in
/// flight took a 5,000-file package rebuild from 17 s to a few seconds;
/// sidecars are sub-KB objects, far below any S3 prefix limit.
const SIDECAR_READ_CONCURRENCY: usize = 64;
const PACKAGE_SWEEP_CONCURRENCY: usize = 8;

/// Mark a package as needing an index rebuild (empty object at `_dirty/<pkg>`).
pub async fn mark_dirty(storage: &dyn Storage, pkg: &str) -> Result<()> {
    storage
        .put_bytes(&format!("{DIRTY_PREFIX}{pkg}"), Vec::new(), None)
        .await
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

    let mut last_reconcile: Option<Instant> = None;
    // The sweep runs on its own task: a full reconcile over a large corpus
    // takes minutes of storage round-trips, and running it inline starved
    // dirty-marker processing for its whole duration (sync uploads timed
    // out, upload→visible p99 went to tens of seconds). Concurrent sweep +
    // tick rebuilds of the same package are safe — rebuilds are idempotent.
    let sweep_running = Arc::new(AtomicBool::new(false));
    loop {
        let is_leader = match &lease {
            None => true,
            Some(lm) => lm.is_leader().await,
        };
        if is_leader {
            // The reconciler is the backbone; markers merely accelerate it.
            // The first leader sweep runs immediately, so a restored backup
            // (or a fresh leader) heals without waiting an interval.
            let due = last_reconcile.is_none_or(|t| t.elapsed() >= state.reconcile_interval);
            if due && !sweep_running.swap(true, Ordering::SeqCst) {
                last_reconcile = Some(Instant::now());
                let state = state.clone();
                let running = sweep_running.clone();
                tokio::spawn(async move {
                    if let Err(e) = reconcile(&state).await {
                        error!(error=?e, "reconcile failed");
                    }
                    running.store(false, Ordering::SeqCst);
                });
            }
            if let Err(e) = tick(&state).await {
                error!(error=?e, "worker tick failed");
            }
        }
        tokio::select! {
            _ = sleep(state.worker_interval) => {}
            _ = shutdown.changed() => break,
        }
    }
    // Graceful exit: hand leadership over instead of leaving successors to
    // wait out the lease TTL (a restart used to be a TTL-long write outage).
    if let Some(lm) = &lease {
        lm.release().await;
    }
}

/// Full sweep: regenerate every package index from truth (backfilling missing
/// sidecars), prune views whose package is gone, and refresh the global index.
/// Writes only happen where the materialized view disagrees with truth.
pub async fn reconcile(state: &AppState) -> Result<()> {
    let mut live: Vec<String> = Vec::new();
    let mut failures = 0usize;
    let packages = state.storage.list_dirs(PACKAGES_PREFIX).await?;
    for chunk in packages.chunks(PACKAGE_SWEEP_CONCURRENCY) {
        let results =
            futures::future::join_all(chunk.iter().map(|pkg| rebuild_package(state, pkg))).await;
        for (pkg, result) in chunk.iter().zip(results) {
            match result {
                Ok(true) => live.push(pkg.clone()),
                Ok(false) => {}
                Err(e) => {
                    // Conservative on failure: keep the package's existing views
                    // and global listing rather than pruning on a bad observation.
                    error!(package=%pkg, error=?e, "reconcile: package rebuild failed");
                    failures += 1;
                    live.push(pkg.clone());
                }
            }
        }
    }

    // Views whose truth directory disappeared entirely. Destructive, so
    // re-check truth at delete time: a package born after our first listing
    // (or built by a split-brain peer) must not have its fresh view pruned.
    let live_set: HashSet<&str> = live.iter().map(String::as_str).collect();
    for view in state.storage.list_dirs(SIMPLE_PREFIX).await? {
        if live_set.contains(view.as_str()) {
            continue;
        }
        let prefix = format!("{PACKAGES_PREFIX}{view}/");
        match state.storage.list_dir_entries(&prefix).await {
            Ok(entries) => {
                let has_artifact = entries.iter().any(|e| {
                    e.key
                        .strip_prefix(&prefix)
                        .map(is_artifact)
                        .unwrap_or(false)
                });
                if has_artifact {
                    continue;
                }
            }
            Err(e) => {
                error!(view=%view, error=?e, "reconcile: cannot verify orphan view; keeping it");
                failures += 1;
                continue;
            }
        }
        let keys = [
            format!("{SIMPLE_PREFIX}{view}/index.html"),
            format!("{SIMPLE_PREFIX}{view}/index.json"),
        ];
        state.storage.delete_keys(&keys).await?;
        for key in &keys {
            state.index_cache.invalidate(key);
        }
    }

    live.sort();
    live.dedup();
    write_global_indexes(state, &live).await?;
    if failures > 0 {
        return Err(anyhow::anyhow!(
            "sweep finished with {failures} package failure(s)"
        ));
    }
    info!(packages = live.len(), "reconcile: sweep complete");
    Ok(())
}

async fn tick(state: &Arc<AppState>) -> Result<()> {
    let markers = state.storage.list_dir_entries(DIRTY_PREFIX).await?;
    if markers.is_empty() {
        return Ok(());
    }
    info!(count = markers.len(), "worker: processing dirty markers");

    // Markers drain with bounded concurrency: they are per-package and
    // rebuilds are idempotent, so parallelism across packages is free. The
    // serial loop capped mass ingest (10k freshly seeded packages) at a few
    // packages per second of S3 round-trips. A semaphore (not chunked
    // join_all) so one slow 5,000-file rebuild never head-of-line blocks the
    // tiny rebuilds behind it — that stall showed up as a 73s visibility p99
    // for unrelated packages. One failing package still must not starve the
    // rest of the namespace.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(PACKAGE_SWEEP_CONCURRENCY));
    let mut handles = Vec::with_capacity(markers.len());
    for marker in markers {
        let state = state.clone();
        let semaphore = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await;
            drain_one_marker(&state, &marker).await
        }));
    }
    let mut failures = 0usize;
    for handle in handles {
        if !handle.await.unwrap_or(false) {
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(anyhow::anyhow!("{failures} package(s) failed this tick"));
    }
    Ok(())
}

/// Delete one marker then rebuild its package; returns success.
async fn drain_one_marker(state: &AppState, marker: &FileEntry) -> bool {
    let Some(pkg) = marker.key.strip_prefix(DIRTY_PREFIX) else {
        return true;
    };
    // Marker first (see module docs): marks landing during the rebuild
    // survive for the next tick instead of being deleted unprocessed.
    if let Err(e) = state
        .storage
        .delete_keys(std::slice::from_ref(&marker.key))
        .await
    {
        error!(package=%pkg, error=?e, "could not delete dirty marker; will retry");
        return false;
    }
    if let Err(e) = process_dirty(state, pkg).await {
        error!(package=%pkg, error=?e, "rebuild failed; re-marking");
        // Restore the event so the next tick retries promptly instead of
        // waiting for the reconcile sweep.
        let _ = mark_dirty(state.storage.as_ref(), pkg).await;
        return false;
    }
    true
}

async fn process_dirty(state: &AppState, pkg: &str) -> Result<()> {
    let has_artifacts = rebuild_package(state, pkg).await?;
    maybe_rebuild_global(state, pkg, has_artifacts).await
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

/// The global index only changes when the *set of package names* changes.
/// Check membership in the current index first; most uploads skip any write.
/// When it does change, update incrementally — the one changed name is known,
/// and re-listing every package directory put a new name 57 s away from the
/// global index at 10k packages. Lost updates (peer nodes racing) are healed
/// by the reconcile sweep's full rebuild; in-process racers serialize on a
/// lock. Views may lag truth; the backbone repairs.
async fn maybe_rebuild_global(state: &AppState, pkg: &str, has_artifacts: bool) -> Result<()> {
    let _guard = state.global_index_lock.lock().await;
    let mut projects = global_index_projects(state).await;
    let listed = projects.contains(pkg);
    if listed == has_artifacts {
        return Ok(());
    }
    if has_artifacts {
        projects.insert(pkg.to_string());
    } else {
        projects.remove(pkg);
    }
    let mut packages: Vec<String> = projects.into_iter().collect();
    packages.sort();
    write_global_indexes(state, &packages).await
}

/// Package names in the current materialized global JSON index (empty if unreadable).
async fn global_index_projects(state: &AppState) -> HashSet<String> {
    let Ok(bytes) = state
        .storage
        .get_bytes(&format!("{SIMPLE_PREFIX}index.json"))
        .await
    else {
        return HashSet::new();
    };
    #[derive(serde::Deserialize)]
    struct Global {
        projects: Vec<Project>,
    }
    #[derive(serde::Deserialize)]
    struct Project {
        name: String,
    }
    match serde_json::from_slice::<Global>(&bytes) {
        Ok(g) => g.projects.into_iter().map(|p| p.name).collect(),
        Err(_) => HashSet::new(),
    }
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
    Some(FileMetadata {
        filename: filename.to_string(),
        sha256: sc.sha256,
        size: sc.size,
        upload_time: Some(sc.upload_time),
        version: Some(sc.version).filter(|v| !v.is_empty()),
        yanked: sc.yanked,
        requires_python: sc.requires_python,
        core_metadata: names.contains(format!("{filename}{METADATA_SUFFIX}").as_str()),
    })
}

async fn read_sidecar(state: &AppState, artifact_key: &str) -> Result<Sidecar> {
    let bytes = state.storage.get_bytes(&sidecar_key(artifact_key)).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Hash-once-and-backfill for files that predate write-time sidecars.
/// Storage last-modified is the upload-time fallback (correct by construction
/// for direct uploads — filenames are immutable, so written exactly once).
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
    state
        .storage
        .put_bytes(
            &sidecar_key(&entry.key),
            serde_json::to_vec(&sc)?,
            Some("application/json"),
        )
        .await?;
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
    let html = pep503_package_html(pkg, files);
    let json = pep691_package_json(pkg, files);

    let base = format!("{SIMPLE_PREFIX}{pkg}/");
    put_if_changed(
        state,
        &format!("{base}index.html"),
        html.into_bytes(),
        "text/html; charset=utf-8",
    )
    .await?;
    put_if_changed(
        state,
        &format!("{base}index.json"),
        json.into_bytes(),
        "application/vnd.pypi.simple.v1+json",
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
        "text/html; charset=utf-8",
    )
    .await?;
    put_if_changed(
        state,
        &format!("{SIMPLE_PREFIX}index.json"),
        json.into_bytes(),
        "application/vnd.pypi.simple.v1+json",
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

    /// Storage stub whose `list_dirs("packages/")` never returns — a reconcile
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
            anyhow::bail!("not used in this test")
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
        async fn list_dirs(&self, dir_prefix: &str) -> Result<Vec<String>> {
            if dir_prefix == PACKAGES_PREFIX {
                // The sweep is now "running" and will never finish.
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
        // global rebuild (which would also hit the stalled list_dirs).
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
            private_prefix: None,
            artifact_delivery: ArtifactDelivery::Auto,
            worker_interval: Duration::from_millis(10),
            reconcile_interval: Duration::from_secs(3600),
            sync_uploads: false,
            sync_upload_timeout: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(30),
            index_cache: Arc::new(crate::cache::IndexCache::new(crate::cache::INDEX_CACHE_TTL)),
            presign_cache: Arc::new(crate::cache::PresignCache::new(
                crate::cache::PRESIGN_CACHE_TTL,
            )),
            spool_dir: std::env::temp_dir(),
            global_index_lock: Arc::new(tokio::sync::Mutex::new(())),
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
