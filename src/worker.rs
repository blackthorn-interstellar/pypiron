use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

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
use crate::{
    AppState, PACKAGES_PREFIX, QUEUE_PENDING_PREFIX, QUEUE_PROCESSING_PREFIX, SIMPLE_PREFIX,
};

pub async fn run_worker(state: Arc<AppState>) {
    loop {
        if let Err(e) = tick(&state).await {
            error!(error=?e, "worker tick failed");
        }
        sleep(state.worker_interval).await;
    }
}

async fn tick(state: &AppState) -> Result<()> {
    // 1) list up to batch_size from pending/
    let mut jobs = state
        .storage
        .list_prefix_files_limited(QUEUE_PENDING_PREFIX, state.job_batch_size)
        .await?;
    // Filter json job files only
    jobs.retain(|k| k.ends_with(".json"));

    if jobs.is_empty() {
        info!("worker: no jobs");
        return Ok(());
    }

    info!(count = jobs.len(), "worker: claiming jobs");

    // 2) move to processing/
    let mut claimed = Vec::new();
    for key in &jobs {
        let dst = key.replace(QUEUE_PENDING_PREFIX, QUEUE_PROCESSING_PREFIX);
        state.storage.copy_then_delete(key, &dst).await?;
        claimed.push(dst);
    }

    // 3) aggregate packages from job content or filename
    let mut touched: HashSet<String> = HashSet::new();
    for key in &claimed {
        match read_job_package(state, key).await {
            Ok(pkg) => {
                touched.insert(pkg);
            }
            Err(e) => {
                warn!(error=?e, key=%key, "could not read job; attempting to infer from key");
                if let Some(pkg) = infer_pkg_from_job_key(key) {
                    touched.insert(pkg);
                }
            }
        }
    }

    if touched.is_empty() {
        warn!("worker: no packages inferred; cleaning up claimed jobs");
        state.storage.delete_keys(&claimed).await?;
        return Ok(());
    }

    // 4) per-package index regeneration
    for pkg in &touched {
        let files = list_artifacts(state, pkg).await?;
        write_pkg_indexes(state, pkg, &files).await?;
    }

    // 5) global index regeneration (once per batch)
    let packages = list_all_packages(state).await?;
    write_global_indexes(state, &packages).await?;

    // 6) cleanup processed jobs
    state.storage.delete_keys(&claimed).await?;
    info!(count=?claimed.len(), "worker: jobs processed");
    Ok(())
}

async fn read_job_package(state: &AppState, key: &str) -> Result<String> {
    let out = state.storage.get_bytes(key).await?;
    // Minimal JSON: {"package":"<name>", ...}
    #[derive(serde::Deserialize)]
    struct JobPkg {
        package: String,
    }
    let job: JobPkg = serde_json::from_slice(&out.bytes)?;
    Ok(job.package)
}

fn infer_pkg_from_job_key(key: &str) -> Option<String> {
    // .../processing/<epoch>-<package>-<filename>.json
    let fname = key.split('/').next_back()?;
    let mut parts = fname.splitn(3, '-'); // epoch, package, rest
    let _ = parts.next()?;
    let package = parts.next()?.to_string();
    Some(package)
}

/// List a package's artifacts with metadata from sidecars — O(files), no hashing.
/// Artifacts without a sidecar (legacy files) get one backfilled, hashing once.
async fn list_artifacts(state: &AppState, pkg: &str) -> Result<Vec<FileMetadata>> {
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
    let out = state.storage.get_bytes(&sidecar_key(artifact_key)).await?;
    Ok(serde_json::from_slice(&out.bytes)?)
}

/// Hash-once-and-backfill for files that predate write-time sidecars.
/// Storage last-modified is the upload-time fallback (correct by construction
/// for direct uploads — filenames are immutable, so written exactly once).
async fn backfill_sidecar(state: &AppState, entry: &FileEntry, filename: &str) -> Result<Sidecar> {
    let obj = state.storage.get_bytes(&entry.key).await?;
    let mut hasher = Sha256::new();
    hasher.update(&obj.bytes);
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

    // /simple/<pkg>/index.html, index.json
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

async fn list_all_packages(state: &AppState) -> Result<Vec<String>> {
    let mut pkgs = BTreeSet::new();
    for name in state.storage.list_dirs(PACKAGES_PREFIX).await? {
        pkgs.insert(name);
    }
    Ok(pkgs.into_iter().collect())
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
