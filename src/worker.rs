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

use std::{collections::HashSet, sync::Arc, time::Instant};

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

/// Mark a package as needing an index rebuild (empty object at `_dirty/<pkg>`).
pub async fn mark_dirty(storage: &dyn Storage, pkg: &str) -> Result<()> {
    storage
        .put_bytes(&format!("{DIRTY_PREFIX}{pkg}"), Vec::new(), None)
        .await
}

pub async fn run_worker(state: Arc<AppState>) {
    // Only the index writer is singular, and only as a cost optimization:
    // rebuilds are idempotent, so the lease is sloppy. Disk is single-node
    // and skips leasing entirely.
    let lease = state
        .storage
        .supports_leases()
        .then(|| LeaseManager::new(state.storage.clone(), state.lease_ttl));

    let mut last_reconcile: Option<Instant> = None;
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
            if due {
                if let Err(e) = reconcile(&state).await {
                    error!(error=?e, "reconcile failed");
                }
                last_reconcile = Some(Instant::now());
            }
            if let Err(e) = tick(&state).await {
                error!(error=?e, "worker tick failed");
            }
        }
        sleep(state.worker_interval).await;
    }
}

/// Full sweep: regenerate every package index from truth (backfilling missing
/// sidecars), prune views whose package is gone, and refresh the global index.
/// Writes only happen where the materialized view disagrees with truth.
pub async fn reconcile(state: &AppState) -> Result<()> {
    let mut live: Vec<String> = Vec::new();
    let mut failures = 0usize;
    for pkg in state.storage.list_dirs(PACKAGES_PREFIX).await? {
        match rebuild_package(state, &pkg).await {
            Ok(true) => live.push(pkg),
            Ok(false) => {}
            Err(e) => {
                // Conservative on failure: keep the package's existing views
                // and global listing rather than pruning on a bad observation.
                error!(package=%pkg, error=?e, "reconcile: package rebuild failed");
                failures += 1;
                live.push(pkg);
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
        state
            .storage
            .delete_keys(&[
                format!("{SIMPLE_PREFIX}{view}/index.html"),
                format!("{SIMPLE_PREFIX}{view}/index.json"),
            ])
            .await?;
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

async fn tick(state: &AppState) -> Result<()> {
    let markers = state.storage.list_dir_entries(DIRTY_PREFIX).await?;
    if markers.is_empty() {
        return Ok(());
    }
    info!(count = markers.len(), "worker: processing dirty markers");

    // One failing package must not starve the rest of the namespace.
    let mut failures = 0usize;
    for marker in &markers {
        let Some(pkg) = marker.key.strip_prefix(DIRTY_PREFIX) else {
            continue;
        };
        // Marker first (see module docs): marks landing during the rebuild
        // survive for the next tick instead of being deleted unprocessed.
        if let Err(e) = state
            .storage
            .delete_keys(std::slice::from_ref(&marker.key))
            .await
        {
            error!(package=%pkg, error=?e, "could not delete dirty marker; will retry");
            failures += 1;
            continue;
        }
        if let Err(e) = process_dirty(state, pkg).await {
            error!(package=%pkg, error=?e, "rebuild failed; re-marking");
            // Restore the event so the next tick retries promptly instead of
            // waiting for the reconcile sweep.
            let _ = mark_dirty(state.storage.as_ref(), pkg).await;
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(anyhow::anyhow!("{failures} package(s) failed this tick"));
    }
    Ok(())
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
        state
            .storage
            .delete_keys(&[
                format!("{SIMPLE_PREFIX}{pkg}/index.html"),
                format!("{SIMPLE_PREFIX}{pkg}/index.json"),
            ])
            .await?;
        return Ok(false);
    }
    write_pkg_indexes(state, pkg, &files).await?;
    Ok(true)
}

/// The global index only changes when the *set of package names* changes.
/// Check membership in the current index first; most uploads skip the rebuild.
async fn maybe_rebuild_global(state: &AppState, pkg: &str, has_artifacts: bool) -> Result<()> {
    let listed = global_index_projects(state).await.contains(pkg);
    if listed != has_artifacts {
        rebuild_global(state).await?;
    }
    Ok(())
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

/// Regenerate the global index from a full listing: every package directory
/// that still contains at least one artifact.
pub async fn rebuild_global(state: &AppState) -> Result<()> {
    let mut packages = Vec::new();
    for dir in state.storage.list_dirs(PACKAGES_PREFIX).await? {
        let prefix = format!("{PACKAGES_PREFIX}{dir}/");
        let entries = state.storage.list_dir_entries(&prefix).await?;
        let has_artifact = entries.iter().any(|e| {
            e.key
                .strip_prefix(&prefix)
                .map(is_artifact)
                .unwrap_or(false)
        });
        if has_artifact {
            packages.push(dir);
        }
    }
    packages.sort();
    write_global_indexes(state, &packages).await
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

    let mut metadata = Vec::new();
    for entry in &entries {
        let Some(filename) = entry.key.strip_prefix(&prefix) else {
            continue;
        };
        if !is_artifact(filename) {
            continue;
        }

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
                    continue;
                }
            }
        } else {
            match backfill_sidecar(state, entry, filename).await {
                Ok(sc) => sc,
                Err(e) => {
                    warn!(error=?e, key=%entry.key, "could not backfill sidecar; skipping file");
                    continue;
                }
            }
        };

        metadata.push(FileMetadata {
            filename: filename.to_string(),
            sha256: sc.sha256,
            size: sc.size,
            upload_time: Some(sc.upload_time),
            version: Some(sc.version).filter(|v| !v.is_empty()),
            yanked: sc.yanked,
            requires_python: sc.requires_python,
            core_metadata: names.contains(format!("{filename}{METADATA_SUFFIX}").as_str()),
        });
    }
    Ok(metadata)
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
/// touch storage (or bump mtimes/ETags) when nothing changed.
async fn put_if_changed(state: &AppState, key: &str, bytes: Vec<u8>, ct: &str) -> Result<()> {
    if let Ok(current) = state.storage.get_bytes(key).await {
        if current == bytes {
            return Ok(());
        }
    }
    state.storage.put_bytes(key, bytes, Some(ct)).await
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
