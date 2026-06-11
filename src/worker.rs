//! Index rebuild worker: dirty markers, not a queue.
//!
//! Uploads and deletes drop an empty marker at `_dirty/<pkg>`. Each tick lists
//! the markers, rebuilds every marked package from a storage listing, and
//! deletes markers only after the indexes are written — at-least-once
//! processing, no claims, no races worth having. Duplicate markers collapse
//! into one rebuild for free.

use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::names::infer_version_from_filename;
use crate::render::{
    pep503_global_html, pep503_package_html, pep691_global_json, pep691_package_json, FileMetadata,
};
use crate::sidecar::{is_artifact, sidecar_key, Sidecar, Yanked, SIDECAR_SUFFIX};
use crate::storage::FileEntry;
use crate::{AppState, DIRTY_PREFIX, PACKAGES_PREFIX, SIMPLE_PREFIX};

pub async fn run_worker(state: Arc<AppState>) {
    loop {
        if let Err(e) = tick(&state).await {
            error!(error=?e, "worker tick failed");
        }
        sleep(state.worker_interval).await;
    }
}

async fn tick(state: &AppState) -> Result<()> {
    let markers = state.storage.list_dir_entries(DIRTY_PREFIX).await?;
    if markers.is_empty() {
        return Ok(());
    }
    info!(count = markers.len(), "worker: processing dirty markers");

    for marker in &markers {
        let Some(pkg) = marker.key.strip_prefix(DIRTY_PREFIX) else {
            continue;
        };
        let has_artifacts = rebuild_package(state, pkg).await?;
        maybe_rebuild_global(state, pkg, has_artifacts).await?;
        // Marker goes last: a crash above leaves it, and the next tick redoes
        // the (idempotent) work.
        state
            .storage
            .delete_keys(std::slice::from_ref(&marker.key))
            .await?;
    }
    Ok(())
}

/// Regenerate one package's indexes from a storage listing.
/// Returns whether the package still has artifacts; with none, its indexes
/// are removed (index first, per the ordering invariant).
pub async fn rebuild_package(state: &AppState, pkg: &str) -> Result<bool> {
    let files = list_artifacts(state, pkg).await?;
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
                    warn!(error=?e, key=%entry.key, "unreadable sidecar; backfilling");
                    match backfill_sidecar(state, entry, filename).await {
                        Ok(sc) => sc,
                        Err(e) => {
                            warn!(error=?e, key=%entry.key, "could not backfill sidecar; skipping file");
                            continue;
                        }
                    }
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

async fn write_pkg_indexes(state: &AppState, pkg: &str, files: &[FileMetadata]) -> Result<()> {
    let html = pep503_package_html(pkg, files);
    let json = pep691_package_json(pkg, files);

    let base = format!("{SIMPLE_PREFIX}{pkg}/");
    state
        .storage
        .put_bytes(
            &format!("{base}index.html"),
            html.into_bytes(),
            Some("text/html; charset=utf-8"),
        )
        .await?;
    state
        .storage
        .put_bytes(
            &format!("{base}index.json"),
            json.into_bytes(),
            Some("application/vnd.pypi.simple.v1+json"),
        )
        .await?;
    Ok(())
}

async fn write_global_indexes(state: &AppState, packages: &[String]) -> Result<()> {
    let html = pep503_global_html(packages);
    let json = pep691_global_json(packages);

    state
        .storage
        .put_bytes(
            &format!("{SIMPLE_PREFIX}index.html"),
            html.into_bytes(),
            Some("text/html; charset=utf-8"),
        )
        .await?;
    state
        .storage
        .put_bytes(
            &format!("{SIMPLE_PREFIX}index.json"),
            json.into_bytes(),
            Some("application/vnd.pypi.simple.v1+json"),
        )
        .await?;
    Ok(())
}
