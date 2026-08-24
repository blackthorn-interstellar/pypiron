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
//!
//! Bodies are the one thing the default pass does not read: it is an O(objects)
//! check, not an O(bytes) one, so it stays runnable on a mirror with a million
//! files. Two claims about the bytes are still checked, at two very different
//! prices. The object's **length** against the sidecar's published `size` is
//! free — the listing already returned it — and always on. Its **sha256** costs
//! a full read of the corpus and is behind `--deep`.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use futures::StreamExt as _;
use sha2::{Digest, Sha256};

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

/// `--deep` hashes bodies as they stream, so a count is a safe bound again:
/// resident memory is a read buffer per artifact in flight, not an artifact.
/// Without streaming this fan-out would multiply by object size — 16 packages
/// × 16 files × a 300 MB wheel is 76 GB — which is why the hasher never sees
/// a whole body ([`stored_sha256`]).
const DEEP_CONCURRENCY: usize = 16;

#[derive(ClapArgs, Debug)]
pub struct VerifyArgs {
    #[command(flatten)]
    pub storage: StorageArgs,

    /// Also re-hash every stored artifact and compare it against the sha256 its
    /// own sidecar publishes — the hash clients check their downloads with.
    /// Reads the whole corpus once, so budget one full pass over your bytes;
    /// without it verify reads no artifact bodies at all.
    #[arg(long, env = "PYPIRON_VERIFY_DEEP")]
    pub deep: bool,
}

/// One observed divergence, printed as `kind\tpackage\tdetail`.
#[derive(Debug)]
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
pub async fn verify_storage(storage: &dyn Storage, deep: bool) -> Result<VerifyReport> {
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
            .map(|(pkg, objects)| check_package(storage, pkg, objects, deep));
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
    let report = verify_storage(storage.as_ref(), args.deep).await?;

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
    deep: bool,
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
    // What `--deep` will re-hash: (filename, the sha256 the sidecar publishes,
    // the length storage and that sidecar agree on — a short stream must read
    // as a truncated transfer, not as a body that contradicts its hash).
    // Collected here because the sidecar read has already happened — a second
    // pass would read every sidecar twice.
    let mut attested: Vec<(&str, String, u64)> = Vec::new();
    for chunk in artifacts.chunks(SIDECAR_READ_CONCURRENCY) {
        let reads = chunk.iter().map(|(_, filename)| {
            let key = format!("{prefix}{filename}{SIDECAR_SUFFIX}");
            async move { storage.get_bytes(&key).await }
        });
        let loaded = futures::future::join_all(reads).await;
        for ((meta, filename), bytes) in chunk.iter().zip(loaded) {
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
            // Truth's own self-check, and the only half of it that costs
            // nothing: the listing already carried this object's real length
            // and the sidecar already publishes what that length should be. A
            // sidecar is a bucket's assertion that a filename IS truth with
            // that sha256 and nothing downstream re-derives it
            // (tests/conformance_publish_ordering.rs), so a body swapped for
            // one of a different length used to be invisible to every reader
            // pypiron has. Checked before the mirror-omission rules, because a
            // record the renderer omits is still stored bytes somebody may one
            // day promote.
            if meta.size != sc.size {
                divs.push(Divergence {
                    kind: "size-mismatch",
                    package: pkg.to_string(),
                    detail: format!(
                        "{filename}: storage holds {} bytes, its sidecar publishes {} — \
                         the body and the hash clients check it against cannot both be right",
                        meta.size, sc.size
                    ),
                });
            } else if deep {
                attested.push((*filename, sc.sha256.clone(), sc.size));
            }
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

    divs.append(&mut rehash_bodies(storage, pkg, &prefix, &attested).await?);

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

/// `--deep`: read each body and compare its sha256 against the one its own
/// sidecar publishes. Empty (and free) unless `--deep` was asked for.
///
/// This is the check every other reader assumes somebody else did. The renderer
/// copies the sidecar's sha into `simple/`, the cross-bucket merge compares
/// sidecar shas to pick a verdict, and the audit re-renders views from
/// sidecars — so a body that stops matching its sidecar is a lie the whole
/// system agrees with. It is deliberately opt-in: it reads every byte in the
/// store, which on a full-PyPI mirror is the corpus and on a private index is
/// seconds.
async fn rehash_bodies(
    storage: &dyn Storage,
    pkg: &str,
    prefix: &str,
    attested: &[(&str, String, u64)],
) -> Result<Vec<Divergence>> {
    let mut divs = Vec::new();
    for batch in attested.chunks(DEEP_CONCURRENCY) {
        let hashes = batch.iter().map(|(filename, _, size)| {
            let key = format!("{prefix}{filename}");
            async move { stored_sha256(storage, &key, *size).await }
        });
        let hashed = futures::future::join_all(hashes).await;
        for ((filename, published, _), stored) in batch.iter().zip(hashed) {
            match stored {
                Ok(stored) => {
                    if &stored != published {
                        divs.push(Divergence {
                            kind: "body-mismatch",
                            package: pkg.to_string(),
                            detail: format!(
                                "{filename}: stored bytes hash to {stored}, its sidecar \
                                 publishes {published} — a client checking this download \
                                 against the index would reject it"
                            ),
                        });
                    }
                }
                // The listing found this key a moment ago. Gone now means a
                // concurrent delete, or a sidecar standing over an empty key —
                // the second is exactly the shape a rolled-back write leaves.
                Err(e) if is_not_found(&e) => divs.push(Divergence {
                    kind: "missing-body",
                    package: pkg.to_string(),
                    detail: format!("{filename}: sidecar published, bytes not there"),
                }),
                Err(e) => return Err(e),
            }
        }
    }
    Ok(divs)
}

/// The sha256 of what `key` actually holds, hashed as the bytes arrive.
///
/// [`Storage::serve_artifact`] is the trait's streaming read — a seek-and-read
/// on disk, the GET response body on the object stores — so the digest costs a
/// read buffer regardless of the object's size. Loading the body first would
/// make an admin command's memory ceiling the largest object in the store times
/// the fan-out, which on a mirror of other people's wheels is not a number
/// pypiron gets to choose. A missing object comes back as [`is_not_found`],
/// which the caller reports rather than raises.
///
/// `expected` is what both storage and the sidecar say this object weighs (the
/// caller only re-hashes the two after they agree). A stream that ends anywhere
/// short of it is a truncated *transfer*, not a divergence, and the difference
/// matters: the digest of a partial body never matches, so hashing whatever
/// arrived would accuse the store of serving bytes that contradict their own
/// sidecar on the strength of a dropped connection. That is an error (exit 2),
/// which says "the check could not run" — the honest verdict.
async fn stored_sha256(storage: &dyn Storage, key: &str, expected: u64) -> Result<String> {
    // This error travels with its type intact: the caller tells a missing body
    // from a real I/O failure by downcasting to NotFound, which survives a
    // context layer but not a rebuild into a fresh error (`anyhow!("{e}")`).
    let mut body = storage
        .serve_artifact(key, None)
        .await?
        .into_body()
        .into_data_stream();
    let mut hasher = Sha256::new();
    let mut read = 0u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.with_context(|| format!("read {key}"))?;
        read += chunk.len() as u64;
        hasher.update(&chunk);
    }
    if read != expected {
        anyhow::bail!("read {key}: body ended after {read} of {expected} bytes");
    }
    Ok(format!("{:x}", hasher.finalize()))
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

    const BODY: &[u8] = b"body";

    /// Describes [`BODY`] truthfully: the size cross-check is always on, so a
    /// fixture whose sidecar lies about its own body is a diverged store.
    fn sidecar(origin: &str) -> Sidecar {
        Sidecar {
            sha256: crate::hash::sha256_hex(BODY),
            size: BODY.len() as u64,
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
            write(&storage, &akey, BODY.to_vec());
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
        let report = verify_storage(bucket.as_ref(), true).await.unwrap();
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
        write(&storage, akey, BODY.to_vec());
        write(
            &storage,
            &format!("{akey}{SIDECAR_SUFFIX}"),
            serde_json::to_vec(&sidecar("mirror")).unwrap(),
        );
        write(&storage, "simple/ghost/index.json", b"{}".to_vec());
        let bucket: Arc<dyn Storage> = storage.clone();
        let report = verify_storage(bucket.as_ref(), true).await.unwrap();
        assert!(
            report
                .divergences
                .iter()
                .any(|d| d.kind == "orphan-view" && d.package == "ghost"),
            "a view over a package with nothing renderable must read as an orphan"
        );
    }

    /// The two claims about the bytes, at their two prices. A record the
    /// renderer *omits* is the subject on purpose: suppressed truth is still
    /// stored bytes somebody may one day promote, so the integrity check must
    /// not skip it the way the view diff does.
    #[tokio::test]
    async fn a_body_that_contradicts_its_own_sidecar_is_a_divergence() {
        let kinds = |report: &VerifyReport| -> Vec<&'static str> {
            report.divergences.iter().map(|d| d.kind).collect()
        };
        let store = |body: &'static [u8]| {
            let storage = SimStorage::new(SimClock::new(
                time::OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap(),
            ));
            let akey = "packages/crossed/crossed-1.0-py3-none-any.whl";
            write(&storage, akey, body.to_vec());
            write(
                &storage,
                &format!("{akey}{SIDECAR_SUFFIX}"),
                serde_json::to_vec(&sidecar("private")).unwrap(),
            );
            storage
        };

        // Same length, different bytes: only the re-hash can see it, which is
        // the whole argument for `--deep` existing at all.
        let swapped: Arc<dyn Storage> = store(b"BODY");
        let shallow = verify_storage(swapped.as_ref(), false).await.unwrap();
        assert!(
            !kinds(&shallow).contains(&"body-mismatch"),
            "the default pass must not read bodies: {:?}",
            kinds(&shallow)
        );
        let deep = verify_storage(swapped.as_ref(), true).await.unwrap();
        assert!(
            kinds(&deep).contains(&"body-mismatch"),
            "--deep missed a same-length body swap: {:?}",
            kinds(&deep)
        );

        // Different length: the listing already told us, so no read is needed
        // and no flag either.
        let truncated: Arc<dyn Storage> = store(b"bod");
        let report = verify_storage(truncated.as_ref(), false).await.unwrap();
        assert!(
            kinds(&report).contains(&"size-mismatch"),
            "a length the sidecar contradicts is free to catch: {:?}",
            kinds(&report)
        );
        // ...and having caught it, `--deep` does not also spend a read to say
        // the same thing twice.
        let report = verify_storage(truncated.as_ref(), true).await.unwrap();
        assert!(
            !kinds(&report).contains(&"body-mismatch"),
            "a proven size mismatch should not also be re-hashed: {:?}",
            kinds(&report)
        );
    }

    /// The two ways a `--deep` read comes back wrong without the store lying
    /// about its own bytes, and the two different verdicts they earn. Driven
    /// through [`rehash_bodies`] directly because both need a key that the
    /// listing produced and the read cannot satisfy — a state no store reaches
    /// by standing still: delete an artifact out of band and it simply leaves
    /// truth, taking its index entry's `orphan-view` with it.
    #[tokio::test]
    async fn a_body_that_never_arrives_is_not_a_body_that_disagrees() {
        let storage = SimStorage::new(SimClock::new(
            time::OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap(),
        ));
        write(
            &storage,
            "packages/raced/raced-1.0-py3-none-any.whl",
            BODY.to_vec(),
        );
        let bucket: Arc<dyn Storage> = storage.clone();
        let sha = crate::hash::sha256_hex(BODY);
        let size = BODY.len() as u64;

        // Listed a moment ago, gone by the time the hasher asked for it. That
        // is a row the operator reads, not a failed run — and it stays one only
        // while the store's NotFound reaches the caller as a NotFound. Rebuild
        // it as a fresh error anywhere in `stored_sha256` and this goes red.
        let vanished = [("raced-2.0-py3-none-any.whl", sha.clone(), size)];
        let divs = rehash_bodies(bucket.as_ref(), "raced", "packages/raced/", &vanished)
            .await
            .expect("a missing body is a reported divergence, not a failed run");
        let kinds: Vec<&str> = divs.iter().map(|d| d.kind).collect();
        assert_eq!(kinds, ["missing-body"], "{kinds:?}");

        // A body that ends short of the length storage and its sidecar already
        // agreed on: the digest of what arrived cannot match, but blaming the
        // bytes for a truncated transfer is a false accusation. The run fails
        // (exit 2) instead of reporting a divergence it cannot stand behind.
        let short = [("raced-1.0-py3-none-any.whl", sha, size + 1)];
        let err = rehash_bodies(bucket.as_ref(), "raced", "packages/raced/", &short)
            .await
            .expect_err("a body that ends short must fail the run, not accuse the store");
        assert!(err.to_string().contains("body ended after"), "{err}");
    }
}
