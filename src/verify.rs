//! `pypiron verify-index`: the read-only oracle. Recompute every materialized view
//! from truth (artifacts + sidecars) and diff against what storage actually
//! serves. Divergence means a healing bug, an interrupted write, or
//! out-of-band storage surgery.
//!
//! Exit codes follow the grep/diff idiom so CI and chaos tests can branch on
//! the three outcomes: **0** converged, **1** diverged (the summary lists what,
//! on stdout — an expected result, not a tool error), **2** the check could not
//! run (storage unreachable, bad config, I/O failure). The diverged path is
//! deliberately kept off the error channel so a found-difference never looks
//! like the tool itself crashed.
//!
//! Strictly read-only: where the worker would backfill a missing sidecar,
//! verify reports it instead.

use std::collections::BTreeMap;

use anyhow::Result;
use clap::Args as ClapArgs;

use crate::app::{DIRTY_PREFIX, PACKAGES_PREFIX, SIMPLE_PREFIX};
use crate::names::normalize_pkg_name;
use crate::origin::OriginState;
use crate::render::{
    pep503_global_html, pep503_project_html, pep691_global_json, pep691_project_json, FileMetadata,
};
use crate::sidecar::{
    is_artifact, Sidecar, FROZEN_SUFFIX, METADATA_SUFFIX, MIRROR_QUARANTINED_SUFFIX,
    PROVENANCE_SUFFIX, SIDECAR_SUFFIX, TOMBSTONE_SUFFIX,
};
use crate::storage::{is_not_found, ObjectMeta, Storage, StorageArgs, SHARD_CHARS};

const SHARD_CONCURRENCY: usize = 8;
const PACKAGE_CONCURRENCY: usize = 16;
const SIDECAR_READ_CONCURRENCY: usize = 64;

#[derive(ClapArgs, Debug)]
pub struct VerifyArgs {
    #[command(flatten)]
    pub storage: StorageArgs,
}

/// One observed divergence, printed as `kind\tpackage\tdetail`.
pub struct Divergence {
    pub kind: &'static str,
    pub package: String,
    pub detail: String,
}

/// What one pass over storage found: the truth it counted and the views that
/// disagreed with it.
pub struct VerifyReport {
    pub packages: usize,
    pub files: usize,
    pub divergences: Vec<Divergence>,
}

/// The pure diff: recompute every view from truth and return what disagrees.
/// Read-only and storage-agnostic, so the deterministic simulator can point it
/// at an in-memory bucket and get the same byte-strict verdict the CLI gives.
/// The truth counts come back with the diff because enumerating `packages/` is
/// the expensive part on a real mirror — a caller that wants totals must not
/// pay for a second listing pass.
pub async fn verify_storage(storage: &dyn Storage) -> Result<VerifyReport> {
    let truth = enumerate_grouped(storage, PACKAGES_PREFIX).await?;
    let views = enumerate_grouped(storage, SIMPLE_PREFIX).await?;
    let package_count = truth.len();
    let file_count = truth.values().map(Vec::len).sum::<usize>();

    let mut divergences: Vec<Divergence> = Vec::new();
    let mut live_packages: Vec<String> = Vec::new();

    let packages: Vec<(&String, &Vec<ObjectMeta>)> = truth.iter().collect();
    for chunk in packages.chunks(PACKAGE_CONCURRENCY) {
        let checks = chunk
            .iter()
            .map(|(pkg, objects)| check_package(storage, pkg, objects));
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
    check_global(storage, &live_packages, &mut divergences).await;
    Ok(VerifyReport {
        packages: package_count,
        files: file_count,
        divergences,
    })
}

/// Run the read-only diff. `Ok(true)` = converged, `Ok(false)` = diverged
/// (rows + summary already printed to stdout), `Err` = the check could not run.
/// The caller maps these to exit codes 0 / 1 / 2.
pub async fn run_verify(args: VerifyArgs) -> Result<bool> {
    let storage = args.storage.build().await?;

    let pending = storage.list_dir_entries(DIRTY_PREFIX).await?;
    if !pending.is_empty() {
        eprintln!(
            "warning: {} dirty marker(s) pending — in-flight packages may report stale views",
            pending.len()
        );
    }

    // One pass: the diff counts the truth it already enumerated, so the summary
    // totals cost nothing extra and no second copy of the truth map (a full
    // mirror is ~10^6 objects) is ever alive.
    let report = verify_storage(storage.as_ref()).await?;

    for d in &report.divergences {
        println!("{}\t{}\t{}", d.kind, d.package, d.detail);
    }
    println!(
        "verify: {} packages, {} files, {} divergence(s)",
        report.packages,
        report.files,
        report.divergences.len()
    );
    // Diverged is an expected, scriptable outcome — return it as data (the rows
    // and summary are already on stdout) rather than routing it through the
    // error channel, which is reserved for "could not run".
    Ok(report.divergences.is_empty())
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

/// The renderer's mirror omission rules, as a pure predicate so the oracle and
/// the thing it audits cannot drift ([`crate::worker`]'s `load_file_metadata`).
/// A mirror record never renders under a private claim — that is the
/// dependency-confusion boundary — and a quarantined mirror body only renders
/// once a private upload has actually superseded it.
fn suppressed_mirror(
    pkg_origin: Option<OriginState>,
    sc: &Sidecar,
    mirror_quarantined: bool,
) -> bool {
    let origin = sc.origin.as_deref();
    (pkg_origin == Some(OriginState::Private) && origin == Some(crate::origin::MIRROR))
        || (mirror_quarantined && origin != Some(crate::origin::PRIVATE))
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
    // Tombstoned filenames are excluded from indexes,
    // so the oracle must exclude them too — otherwise a crashed delete that left
    // an orphan artifact beside its tombstone would read as a stale-view forever.
    let tombstoned: std::collections::HashSet<&str> = names
        .iter()
        .filter_map(|f| f.strip_suffix(TOMBSTONE_SUFFIX))
        .collect();
    let frozen: std::collections::HashSet<&str> = names
        .iter()
        .filter_map(|f| f.strip_suffix(FROZEN_SUFFIX))
        .collect();
    // A mirror body preserved after its package became private is truth the
    // renderer deliberately omits; so is a mirror record that finished after the
    // claim went private. Both are ordinary states on any fleet that ever
    // demoted a claim, so the oracle has to model them or it reports a
    // correctly-rendered view as stale forever.
    let mirror_quarantined: std::collections::HashSet<&str> = names
        .iter()
        .filter_map(|f| f.strip_suffix(MIRROR_QUARANTINED_SUFFIX))
        .collect();
    let pkg_origin = crate::origin::read_origin_claim(storage, pkg).await?;
    let artifacts: Vec<(&ObjectMeta, &str)> = objects
        .iter()
        .filter_map(|o| {
            let filename = o.key.strip_prefix(&prefix)?;
            // Omission rule 1: quarantined with no sidecar at all. Decided from
            // the listing, like the worker does — it never reads that sidecar.
            let untyped_quarantine = mirror_quarantined.contains(filename)
                && !names.contains(format!("{filename}{SIDECAR_SUFFIX}").as_str());
            (!filename.contains('/')
                && is_artifact(filename)
                && !tombstoned.contains(filename)
                && !frozen.contains(filename)
                && !untyped_quarantine)
                .then_some((o, filename))
        })
        .collect();

    // Assemble expected index entries exactly as the worker does
    // (worker::load_file_metadata), minus the backfill write.
    let mut files: Vec<FileMetadata> = Vec::with_capacity(artifacts.len());
    let mut comparable = true;
    // Artifacts the renderer omits for one of the mirror rules. They are not a
    // divergence and they do not keep the package alive: `still_live` (and so
    // global-index membership) follows the *renderable* set.
    let mut suppressed = 0usize;
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
            if suppressed_mirror(pkg_origin, &sc, mirror_quarantined.contains(filename)) {
                suppressed += 1;
                continue;
            }
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

    let has_artifacts = artifacts.len() > suppressed;
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
                pep503_project_html(pkg, render_files, &status),
            ),
            (
                "index.json",
                pep691_project_json(pkg, render_files, &status),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{SimClock, SimStorage};
    use std::sync::Arc;

    fn sidecar(origin: &str) -> Sidecar {
        Sidecar {
            sha256: "0".repeat(64),
            size: 1,
            version: "1.0".to_string(),
            upload_time: "2026-01-01T00:00:00Z".to_string(),
            upload_epoch_ms: None,
            requires_python: None,
            yanked: crate::sidecar::Yanked::Flag(false),
            origin: Some(origin.to_string()),
            yank_epoch: 0,
            snapshot: false,
        }
    }

    /// The three shapes `worker::load_file_metadata` omits, and the two it
    /// keeps. An oracle that renders any of the three reports a
    /// correctly-rendered view as stale on every fleet that demoted a claim.
    #[test]
    fn the_oracle_omits_exactly_what_the_renderer_omits() {
        let private = Some(OriginState::Private);
        let mirror = Some(OriginState::Mirror);
        // Mirror bytes under a private claim: never rendered.
        assert!(suppressed_mirror(private, &sidecar("mirror"), false));
        // Quarantined and still non-private: not rendered until superseded.
        assert!(suppressed_mirror(mirror, &sidecar("mirror"), true));
        assert!(suppressed_mirror(None, &sidecar("mirror"), true));
        // Private truth under a private claim, and an ordinary mirror record
        // under a mirror claim, both render.
        assert!(!suppressed_mirror(private, &sidecar("private"), false));
        assert!(!suppressed_mirror(mirror, &sidecar("mirror"), false));
        // A quarantine marker over a record a private upload already superseded
        // renders again — that is what the supersede was for.
        assert!(!suppressed_mirror(private, &sidecar("private"), true));
    }

    fn write(storage: &SimStorage, key: &str, bytes: Vec<u8>) {
        storage.insert(key, bytes);
    }

    /// End to end over an in-memory bucket: a demoted package whose mirror
    /// bodies the worker suppressed must read as converged, not as a stale view.
    #[tokio::test]
    async fn a_demoted_package_verifies_clean() {
        let storage = SimStorage::new(SimClock::new(
            time::OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap(),
        ));
        write(
            &storage,
            "packages/demo/.origin",
            format!(r#"{{"origin":"private","nonce":"{}"}}"#, "a".repeat(32)).into_bytes(),
        );
        for (file, origin, quarantined) in [
            ("demo-1.0-py3-none-any.whl", "private", false),
            ("demo-2.0-py3-none-any.whl", "mirror", false),
            ("demo-3.0-py3-none-any.whl", "mirror", true),
        ] {
            let akey = format!("packages/demo/{file}");
            write(&storage, &akey, b"body".to_vec());
            write(
                &storage,
                &format!("{akey}{SIDECAR_SUFFIX}"),
                serde_json::to_vec(&sidecar(origin)).unwrap(),
            );
            if quarantined {
                write(
                    &storage,
                    &format!("{akey}{MIRROR_QUARANTINED_SUFFIX}"),
                    b"{}".to_vec(),
                );
            }
        }
        // Render the views the way the server does, then ask the oracle.
        let bucket: Arc<dyn Storage> = storage.clone();
        let state = crate::sim::single_bucket_state(bucket.clone());
        crate::worker::rebuild_package_excluding(&state, bucket.as_ref(), "demo", None)
            .await
            .unwrap();
        let report = verify_storage(bucket.as_ref()).await.unwrap();
        let view_divergences: Vec<&str> = report
            .divergences
            .iter()
            .filter(|d| d.kind != "missing-global-index")
            .map(|d| d.kind)
            .collect();
        assert!(
            view_divergences.is_empty(),
            "a correctly rendered demoted package read as diverged: {view_divergences:?}"
        );
        // ...and the one private file is what got rendered.
        let index = bucket.get_bytes("simple/demo/index.json").await.unwrap();
        let index = String::from_utf8(index).unwrap();
        assert!(index.contains("demo-1.0-py3-none-any.whl"), "{index}");
        assert!(!index.contains("demo-2.0-py3-none-any.whl"), "{index}");
        assert!(!index.contains("demo-3.0-py3-none-any.whl"), "{index}");
    }

    /// Every artifact suppressed means the package renders nothing, so a
    /// materialized view left behind is an orphan — the same verdict the worker
    /// reaches when it deletes those views.
    #[tokio::test]
    async fn a_fully_suppressed_package_has_no_live_view() {
        let storage = SimStorage::new(SimClock::new(
            time::OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap(),
        ));
        write(
            &storage,
            "packages/ghost/.origin",
            format!(r#"{{"origin":"private","nonce":"{}"}}"#, "b".repeat(32)).into_bytes(),
        );
        let akey = "packages/ghost/ghost-1.0-py3-none-any.whl";
        write(&storage, akey, b"body".to_vec());
        write(
            &storage,
            &format!("{akey}{SIDECAR_SUFFIX}"),
            serde_json::to_vec(&sidecar("mirror")).unwrap(),
        );
        write(&storage, "simple/ghost/index.json", b"{}".to_vec());
        let bucket: Arc<dyn Storage> = storage.clone();
        let report = verify_storage(bucket.as_ref()).await.unwrap();
        assert!(
            report
                .divergences
                .iter()
                .any(|d| d.kind == "orphan-view" && d.package == "ghost"),
            "a view over a package with nothing renderable must read as an orphan"
        );
    }
}
