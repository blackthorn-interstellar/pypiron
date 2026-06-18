//! `pypiron verify`: the read-only oracle. Recompute every materialized view
//! from truth (artifacts + sidecars) and diff against what storage actually
//! serves. Divergence means a healing bug, an interrupted write, or
//! out-of-band storage surgery — exit nonzero so CI and chaos tests can
//! assert convergence.
//!
//! Strictly read-only: where the worker would backfill a missing sidecar,
//! verify reports it instead.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use clap::Args as ClapArgs;

use crate::names::normalize_pkg_name;
use crate::render::{
    pep503_global_html, pep503_package_html, pep691_global_json, pep691_package_json, FileMetadata,
};
use crate::sidecar::{is_artifact, Sidecar, METADATA_SUFFIX, PROVENANCE_SUFFIX, SIDECAR_SUFFIX};
use crate::storage::{is_not_found, ObjectMeta, Storage, StorageArgs, SHARD_CHARS};
use crate::{DIRTY_PREFIX, PACKAGES_PREFIX, SIMPLE_PREFIX};

const SHARD_CONCURRENCY: usize = 8;
const PACKAGE_CONCURRENCY: usize = 16;
const SIDECAR_READ_CONCURRENCY: usize = 64;

#[derive(ClapArgs, Debug)]
pub struct VerifyArgs {
    #[command(flatten)]
    storage: StorageArgs,
}

/// One observed divergence, printed as `kind\tpackage\tdetail`.
struct Divergence {
    kind: &'static str,
    package: String,
    detail: String,
}

pub async fn run_verify(args: VerifyArgs) -> Result<()> {
    let storage = args.storage.build().await?;

    let pending = storage.list_dir_entries(DIRTY_PREFIX).await?;
    if !pending.is_empty() {
        eprintln!(
            "warning: {} dirty marker(s) pending — in-flight packages may report stale views",
            pending.len()
        );
    }

    let truth = enumerate_grouped(storage.as_ref(), PACKAGES_PREFIX).await?;
    let views = enumerate_grouped(storage.as_ref(), SIMPLE_PREFIX).await?;

    let mut divergences: Vec<Divergence> = Vec::new();
    let mut live_packages: Vec<String> = Vec::new();

    let packages: Vec<(&String, &Vec<ObjectMeta>)> = truth.iter().collect();
    for chunk in packages.chunks(PACKAGE_CONCURRENCY) {
        let checks = chunk
            .iter()
            .map(|(pkg, objects)| check_package(storage.as_ref(), pkg, objects));
        for result in futures::future::join_all(checks).await {
            let (pkg, has_artifacts, mut divs) = result?;
            if has_artifacts {
                live_packages.push(pkg);
            }
            divergences.append(&mut divs);
        }
    }

    // Views must not outlive their package ("orphan view" — worker prunes these).
    for view_pkg in views.keys() {
        if !truth.contains_key(view_pkg) {
            divergences.push(Divergence {
                kind: "orphan-view",
                package: view_pkg.clone(),
                detail: "materialized view exists but the package has no files".into(),
            });
        }
    }

    live_packages.sort();
    check_global(storage.as_ref(), &live_packages, &mut divergences).await;

    for d in &divergences {
        println!("{}\t{}\t{}", d.kind, d.package, d.detail);
    }
    println!(
        "verify: {} packages, {} files, {} divergence(s)",
        truth.len(),
        truth.values().map(Vec::len).sum::<usize>(),
        divergences.len()
    );
    if !divergences.is_empty() {
        bail!("{} divergence(s) found", divergences.len());
    }
    Ok(())
}

/// Flat-list `prefix` across shards and group objects by first path segment
/// (the package name). Objects directly under the prefix (the global index
/// files) land under the empty-string key.
async fn enumerate_grouped(
    storage: &dyn Storage,
    prefix: &str,
) -> Result<BTreeMap<String, Vec<ObjectMeta>>> {
    let mut grouped: BTreeMap<String, Vec<ObjectMeta>> = BTreeMap::new();
    let shards: Vec<String> = SHARD_CHARS.iter().map(|c| format!("{prefix}{c}")).collect();
    for chunk in shards.chunks(SHARD_CONCURRENCY) {
        let lists = chunk.iter().map(|shard| storage.list_all(shard));
        for listed in futures::future::join_all(lists).await {
            for obj in listed? {
                let rest = obj.key.strip_prefix(prefix).unwrap_or(&obj.key);
                let group = match rest.split_once('/') {
                    Some((pkg, _)) => pkg.to_string(),
                    None => String::new(),
                };
                grouped.entry(group).or_default().push(obj);
            }
        }
    }
    grouped.remove("");
    Ok(grouped)
}

/// Recompute one package's views from its truth objects and diff them
/// against storage. Returns (pkg, has_artifacts, divergences).
async fn check_package(
    storage: &dyn Storage,
    pkg: &str,
    objects: &[ObjectMeta],
) -> Result<(String, bool, Vec<Divergence>)> {
    let mut divs = Vec::new();
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");

    if normalize_pkg_name(pkg) != pkg {
        divs.push(Divergence {
            kind: "bad-package-dir",
            package: pkg.to_string(),
            detail: "directory name is not PEP 503 normalized".into(),
        });
    }

    let names: std::collections::HashSet<&str> = objects
        .iter()
        .filter_map(|o| o.key.strip_prefix(&prefix))
        .collect();
    let artifacts: Vec<(&ObjectMeta, &str)> = objects
        .iter()
        .filter_map(|o| {
            let filename = o.key.strip_prefix(&prefix)?;
            (!filename.contains('/') && is_artifact(filename)).then_some((o, filename))
        })
        .collect();

    // Assemble expected index entries exactly as the worker does
    // (worker::load_file_metadata), minus the backfill write.
    let mut files: Vec<FileMetadata> = Vec::with_capacity(artifacts.len());
    let mut comparable = true;
    for chunk in artifacts.chunks(SIDECAR_READ_CONCURRENCY) {
        let reads = chunk.iter().map(|(_, filename)| {
            let key = format!("{prefix}{filename}{SIDECAR_SUFFIX}");
            async move { storage.get_bytes(&key).await }
        });
        let loaded = futures::future::join_all(reads).await;
        for ((_, filename), bytes) in chunk.iter().zip(loaded) {
            let bytes = match bytes {
                Ok(b) => b,
                Err(e) if is_not_found(&e) => {
                    divs.push(Divergence {
                        kind: "missing-sidecar",
                        package: pkg.to_string(),
                        detail: format!("{filename} has no sidecar (worker would backfill)"),
                    });
                    comparable = false;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let sc: Sidecar = match serde_json::from_slice(&bytes) {
                Ok(sc) => sc,
                Err(e) => {
                    // The worker omits the file from the index rather than
                    // fabricate metadata; expected views do the same.
                    divs.push(Divergence {
                        kind: "corrupt-sidecar",
                        package: pkg.to_string(),
                        detail: format!("{filename}: {e}"),
                    });
                    continue;
                }
            };
            let core_metadata = names.contains(format!("{filename}{METADATA_SUFFIX}").as_str());
            let provenance = names.contains(format!("{filename}{PROVENANCE_SUFFIX}").as_str());
            files.push(FileMetadata::from_sidecar(
                filename,
                sc,
                core_metadata,
                provenance,
            ));
        }
    }

    let has_artifacts = !artifacts.is_empty();
    let base = format!("{SIMPLE_PREFIX}{pkg}/");
    if has_artifacts && comparable {
        // Render exactly as the worker does (worker::write_pkg_indexes): same
        // per-project status, same quarantine link-omission — otherwise every
        // status-bearing package would read as a spurious stale-view.
        let status = crate::status::read_status(storage, pkg).await?;
        let render_files: &[FileMetadata] = if status.status.blocks_downloads() {
            &[]
        } else {
            &files
        };
        for (suffix, expected) in [
            (
                "index.html",
                pep503_package_html(pkg, render_files, &status),
            ),
            (
                "index.json",
                pep691_package_json(pkg, render_files, &status),
            ),
        ] {
            match storage.get_bytes(&format!("{base}{suffix}")).await {
                Ok(actual) if actual == expected.as_bytes() => {}
                Ok(_) => divs.push(Divergence {
                    kind: "stale-view",
                    package: pkg.to_string(),
                    detail: format!("{suffix} differs from what truth renders to"),
                }),
                Err(e) if is_not_found(&e) => divs.push(Divergence {
                    kind: "missing-view",
                    package: pkg.to_string(),
                    detail: format!("{suffix} is not materialized"),
                }),
                Err(e) => return Err(e),
            }
        }
    }
    if !has_artifacts {
        for suffix in ["index.html", "index.json"] {
            if storage.head_exists(&format!("{base}{suffix}")).await? {
                divs.push(Divergence {
                    kind: "orphan-view",
                    package: pkg.to_string(),
                    detail: format!("{suffix} exists but the package has no artifacts"),
                });
            }
        }
    }

    Ok((pkg.to_string(), has_artifacts, divs))
}

/// The global index must list exactly the packages that have artifacts.
async fn check_global(storage: &dyn Storage, live: &[String], divs: &mut Vec<Divergence>) {
    let live_owned: Vec<String> = live.to_vec();
    for (suffix, expected) in [
        ("index.html", pep503_global_html(&live_owned)),
        ("index.json", pep691_global_json(&live_owned)),
    ] {
        match storage.get_bytes(&format!("{SIMPLE_PREFIX}{suffix}")).await {
            Ok(actual) if actual == expected.as_bytes() => {}
            Ok(_) => divs.push(Divergence {
                kind: "stale-global-index",
                package: String::new(),
                detail: format!(
                    "{suffix} does not match the live package set ({} names)",
                    live.len()
                ),
            }),
            // Never-materialized is only fine when there is nothing to list:
            // a fresh data dir no server has booted yet.
            Err(_) if live.is_empty() => {}
            Err(_) => divs.push(Divergence {
                kind: "missing-global-index",
                package: String::new(),
                detail: suffix.to_string(),
            }),
        }
    }
}
