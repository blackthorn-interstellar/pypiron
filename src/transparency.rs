//! Tamper-evident checkpoints: an append-only, hash-chained transparency log the
//! leader audit writes to `_transparency/chain/<seq>.json`, and `verify-chain`,
//! the read-only replay that catches out-of-band artifact rewrites.
//!
//! The threat is an attacker who holds your storage credentials and rewrites an
//! artifact's bytes *and* its `.meta.json` sha256 consistently — a change every
//! internal check (which trusts the sidecar) would wave through. Each audit
//! commits the sha256 of every changed package's files into a link whose
//! `prev-sha256` binds it to the previous link's exact bytes. History is
//! therefore append-only: an attacker cannot rewrite a past commitment without
//! breaking the chain. `verify-chain` replays the chain to the expected
//! `(package, filename, sha256)` state and diffs it against what storage holds
//! now; a sidecar whose sha no longer matches its commitment is a caught tamper.
//!
//! The write is deltas-only (churn-sized, like the fingerprint shards): each
//! link carries just the packages whose audit fingerprint moved this pass,
//! mapped to their *complete current* file→sha256 map. Replay applies deltas in
//! seq order, last-write-wins, an empty map removing the package. Serialization
//! is byte-deterministic (`BTreeMap`s throughout, `crate::clock::now_utc()` for
//! the timestamp, no randomness) so the deterministic simulator (`examples/vopr`)
//! can exercise the write without diverging.
//!
//! Exit codes mirror `verify-index`: **0** chain valid and storage matches,
//! **1** a violation (rows on stdout, an expected scriptable outcome), **2** the
//! check could not run. A chain that lags truth (files in storage not yet
//! committed) is fine — verify never faults uncommitted files.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

use crate::app::PACKAGES_PREFIX;
use crate::hash::sha256_hex;
use crate::sidecar::{Sidecar, SIDECAR_SUFFIX, TOMBSTONE_SUFFIX};
use crate::storage::{is_not_found, Storage, StorageArgs};

/// Namespace for the chain. `replicate.rs` only ever touches `packages/`, so the
/// chain is excluded from replication for free — no exclusion code needed.
pub(crate) const CHAIN_PREFIX: &str = "_transparency/chain/";

/// Fixed-width zero-padded seq so a lexicographic listing is numeric order.
const SEQ_WIDTH: usize = 16;

const DIFF_CONCURRENCY: usize = 64;

/// One package's committed file→sha256 map.
pub type FileShas = BTreeMap<String, String>;

/// A checkpoint delta: changed packages → their complete current file map.
pub type Delta = BTreeMap<String, FileShas>;

/// Storage key for the link at `seq`.
pub fn chain_key(seq: u64) -> String {
    format!("{CHAIN_PREFIX}{seq:0width$}.json", width = SEQ_WIDTH)
}

/// Parse the seq out of a chain-link key, or `None` if it is not one.
pub(crate) fn seq_from_key(key: &str) -> Option<u64> {
    key.strip_prefix(CHAIN_PREFIX)?
        .strip_suffix(".json")?
        .parse()
        .ok()
}

/// One hash-chained checkpoint. Field order is the on-disk byte order; keep it
/// stable, and keep every map a `BTreeMap`, or the sha chain stops being
/// reproducible.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ChainLink {
    pub seq: u64,
    /// Hex sha256 of the previous link's exact bytes; genesis uses `""`.
    #[serde(rename = "prev-sha256")]
    pub prev_sha256: String,
    /// RFC 3339 timestamp from `crate::clock::now_utc()`.
    pub created: String,
    /// Delta: changed packages → complete current file→sha256 map.
    pub packages: Delta,
}

/// Deterministic serialization of a link — the exact bytes the next link's
/// `prev-sha256` commits to.
pub fn link_bytes(link: &ChainLink) -> Result<Vec<u8>> {
    serde_json::to_vec(link).context("serializing chain link")
}

/// Read the highest-seq link (the chain head) as `(seq, exact bytes)`, or `None`
/// for an empty chain.
pub async fn read_head(storage: &dyn Storage) -> Result<Option<(u64, Vec<u8>)>> {
    let entries = storage.list_all(CHAIN_PREFIX).await?;
    let Some(max) = entries.iter().filter_map(|o| seq_from_key(&o.key)).max() else {
        return Ok(None);
    };
    let bytes = storage.get_bytes(&chain_key(max)).await?;
    Ok(Some((max, bytes)))
}

/// Apply deltas in order to reconstruct the expected `(package → file→sha)`
/// state. Last-write-wins; an empty map removes the package.
pub fn replay<'a>(links: impl IntoIterator<Item = &'a ChainLink>) -> Delta {
    let mut state: Delta = BTreeMap::new();
    for link in links {
        for (pkg, files) in &link.packages {
            if files.is_empty() {
                state.remove(pkg);
            } else {
                state.insert(pkg.clone(), files.clone());
            }
        }
    }
    state
}

/// One observed problem, printed as `kind\tpackage\tdetail`.
pub(crate) struct Violation {
    pub kind: &'static str,
    pub package: String,
    pub detail: String,
}

/// Verify the chain is gapless from its lowest present seq and each link commits
/// to the previous link's exact bytes. `links` must be sorted by seq ascending;
/// each tuple carries the on-disk bytes the sha is taken over and the link
/// parsed from them. An empty vec means the chain is intact.
pub(crate) fn check_integrity(links: &[(u64, Vec<u8>, ChainLink)]) -> Vec<Violation> {
    let mut violations = Vec::new();
    for (i, (seq, _, link)) in links.iter().enumerate() {
        if link.seq != *seq {
            violations.push(Violation {
                kind: "seq-mismatch",
                package: String::new(),
                detail: format!("link file seq {seq} but its body says {}", link.seq),
            });
        }
        if i == 0 {
            if !link.prev_sha256.is_empty() {
                violations.push(Violation {
                    kind: "bad-genesis",
                    package: String::new(),
                    detail: format!("lowest link seq {seq} must have an empty prev-sha256"),
                });
            }
        } else {
            let (prev_seq, prev_bytes, _) = &links[i - 1];
            if *seq != prev_seq + 1 {
                violations.push(Violation {
                    kind: "gap",
                    package: String::new(),
                    detail: format!("seq jumps from {prev_seq} to {seq}"),
                });
            }
            let expected = sha256_hex(prev_bytes);
            if link.prev_sha256 != expected {
                violations.push(Violation {
                    kind: "broken-link",
                    package: String::new(),
                    detail: format!("seq {seq} prev-sha256 does not match seq {prev_seq}'s bytes"),
                });
            }
        }
    }
    violations
}

#[derive(ClapArgs, Debug)]
pub struct VerifyChainArgs {
    #[command(flatten)]
    pub storage: StorageArgs,
}

/// Run the read-only chain verification. `Ok(true)` = chain valid and storage
/// matches, `Ok(false)` = a violation (rows + summary already on stdout), `Err`
/// = the check could not run. The caller maps these to exit 0 / 1 / 2.
pub async fn run_verify_chain(args: VerifyChainArgs) -> Result<bool> {
    let storage = args.storage.build().await?;

    // 1. Load every link, sorted by seq. A key that does not parse as a seq is
    // not a chain link and is dropped in the same pass.
    let mut entries: Vec<(u64, String)> = storage
        .list_all(CHAIN_PREFIX)
        .await?
        .into_iter()
        .filter_map(|o| seq_from_key(&o.key).map(|seq| (seq, o.key)))
        .collect();
    entries.sort_by_key(|(seq, _)| *seq);
    if entries.is_empty() {
        println!("no chain");
        return Ok(true);
    }
    let mut links: Vec<(u64, Vec<u8>, ChainLink)> = Vec::with_capacity(entries.len());
    for (seq, key) in &entries {
        let bytes = storage.get_bytes(key).await?;
        let link: ChainLink =
            serde_json::from_slice(&bytes).with_context(|| format!("parsing chain link {key}"))?;
        links.push((*seq, bytes, link));
    }

    // 2. Chain integrity. A broken chain makes replay meaningless, so report and
    // stop before the storage diff.
    let integrity = check_integrity(&links);
    if !integrity.is_empty() {
        for v in &integrity {
            println!("{}\t{}\t{}", v.kind, v.package, v.detail);
        }
        println!(
            "verify-chain: {} link(s), chain integrity broken ({} violation(s))",
            links.len(),
            integrity.len()
        );
        return Ok(false);
    }

    // 3. Warn (not fault) on an in-place sha change of an already-committed
    // filename across the chain — legitimate only for the rare mirror→private
    // demotion. A running replay-so-far gives the "already committed" view.
    let mut so_far: Delta = BTreeMap::new();
    for (_, _, link) in &links {
        for (pkg, files) in &link.packages {
            if let Some(prev) = so_far.get(pkg) {
                for (filename, sha) in files {
                    if prev.get(filename).is_some_and(|old| old != sha) {
                        let old = &prev[filename];
                        eprintln!(
                            "WARNING: {pkg}/{filename} sha changed in-chain {old} -> {sha} \
                             (legitimate only for a mirror->private demotion)"
                        );
                    }
                }
            }
            if files.is_empty() {
                so_far.remove(pkg);
            } else {
                so_far.insert(pkg.clone(), files.clone());
            }
        }
    }

    // Expected state is the full replay of the (now-verified) chain.
    let expected = replay(links.iter().map(|(_, _, link)| link));

    // 4. Diff the expected state against storage, batched like verify-index.
    let checks: Vec<(&String, &String, &String)> = expected
        .iter()
        .flat_map(|(pkg, files)| files.iter().map(move |(f, sha)| (pkg, f, sha)))
        .collect();
    let mut violations: Vec<Violation> = Vec::new();
    for chunk in checks.chunks(DIFF_CONCURRENCY) {
        let diffs = chunk
            .iter()
            .map(|(pkg, filename, sha)| diff_one(storage.as_ref(), pkg, filename, sha));
        for result in futures::future::join_all(diffs).await {
            if let Some(v) = result? {
                violations.push(v);
            }
        }
    }

    for v in &violations {
        println!("{}\t{}\t{}", v.kind, v.package, v.detail);
    }
    println!(
        "verify-chain: {} link(s), {} committed file(s), {} violation(s)",
        links.len(),
        checks.len(),
        violations.len()
    );
    Ok(violations.is_empty())
}

/// Diff one committed `(package, filename, sha)` against storage. The sidecar is
/// the sha of record: present-but-different is a tamper; gone-with-no-tombstone
/// is a vanish; gone-with-tombstone is a legitimate delete the chain hasn't
/// caught up to yet.
async fn diff_one(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    expected_sha: &str,
) -> Result<Option<Violation>> {
    let base = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    match storage.get_bytes(&format!("{base}{SIDECAR_SUFFIX}")).await {
        Ok(bytes) => {
            let sc: Sidecar = match serde_json::from_slice(&bytes) {
                Ok(sc) => sc,
                Err(e) => {
                    return Ok(Some(Violation {
                        kind: "corrupt-sidecar",
                        package: pkg.to_string(),
                        detail: format!("{filename}: {e}"),
                    }))
                }
            };
            if sc.sha256 != expected_sha {
                return Ok(Some(Violation {
                    kind: "hash-changed",
                    package: pkg.to_string(),
                    detail: format!(
                        "{filename}: committed {expected_sha} but the sidecar now holds {}",
                        sc.sha256
                    ),
                }));
            }
            Ok(None)
        }
        Err(e) if is_not_found(&e) => {
            let tombstone = format!("{base}{TOMBSTONE_SUFFIX}");
            if storage.head_exists(&tombstone).await? {
                Ok(None)
            } else {
                Ok(Some(Violation {
                    kind: "vanished",
                    package: pkg.to_string(),
                    detail: format!("{filename}: committed sidecar is gone with no tombstone"),
                }))
            }
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(seq: u64, prev: &str, packages: Delta) -> ChainLink {
        ChainLink {
            seq,
            prev_sha256: prev.to_string(),
            created: "2026-07-17T00:00:00Z".to_string(),
            packages,
        }
    }

    fn pkg(files: &[(&str, &str)]) -> FileShas {
        files
            .iter()
            .map(|(f, s)| (f.to_string(), s.to_string()))
            .collect()
    }

    #[test]
    fn link_bytes_are_deterministic_and_hash_round_trips() {
        let mut packages: Delta = BTreeMap::new();
        packages.insert(
            "six".to_string(),
            pkg(&[("six-1.whl", "aa"), ("six-2.whl", "bb")]),
        );
        let l = link(3, "deadbeef", packages);

        let b1 = link_bytes(&l).unwrap();
        let b2 = link_bytes(&l).unwrap();
        assert_eq!(b1, b2, "serialization must be byte-stable");

        // Reparse: sha256 of the bytes is what the next link commits to.
        let parsed: ChainLink = serde_json::from_slice(&b1).unwrap();
        assert_eq!(parsed, l);
        assert_eq!(sha256_hex(&b1), sha256_hex(&link_bytes(&parsed).unwrap()));
    }

    #[test]
    fn replay_applies_last_write_wins_and_empty_map_removes() {
        let l0 = link(0, "", {
            let mut d = Delta::new();
            d.insert("six".to_string(), pkg(&[("six-1.whl", "aa")]));
            d.insert("flask".to_string(), pkg(&[("flask-1.whl", "cc")]));
            d
        });
        // six gains a file (last-write-wins replaces its whole map); flask vanishes.
        let l1 = link(1, "x", {
            let mut d = Delta::new();
            d.insert(
                "six".to_string(),
                pkg(&[("six-1.whl", "aa"), ("six-2.whl", "bb")]),
            );
            d.insert("flask".to_string(), FileShas::new());
            d
        });

        let state = replay([&l0, &l1]);
        assert_eq!(state.len(), 1, "flask removed by its empty map");
        assert_eq!(
            state["six"],
            pkg(&[("six-1.whl", "aa"), ("six-2.whl", "bb")])
        );
        assert!(!state.contains_key("flask"));
    }

    #[test]
    fn integrity_catches_a_tampered_middle_link() {
        // Build three correctly-chained links.
        let l0 = link(0, "", {
            let mut d = Delta::new();
            d.insert("a".to_string(), pkg(&[("a-1.whl", "11")]));
            d
        });
        let b0 = link_bytes(&l0).unwrap();
        let l1 = link(1, &sha256_hex(&b0), {
            let mut d = Delta::new();
            d.insert("b".to_string(), pkg(&[("b-1.whl", "22")]));
            d
        });
        let b1 = link_bytes(&l1).unwrap();
        let l2 = link(2, &sha256_hex(&b1), {
            let mut d = Delta::new();
            d.insert("c".to_string(), pkg(&[("c-1.whl", "33")]));
            d
        });
        let b2 = link_bytes(&l2).unwrap();

        // Intact chain: no violations.
        let intact = vec![
            (0, b0.clone(), l0.clone()),
            (1, b1.clone(), l1.clone()),
            (2, b2.clone(), l2.clone()),
        ];
        assert!(check_integrity(&intact).is_empty());

        // Tamper the middle link's stored bytes (attacker edits seq 1 in place).
        // seq 2 still commits to the ORIGINAL seq 1 bytes, so the link breaks.
        let mut tampered = l1.clone();
        tampered
            .packages
            .insert("b".to_string(), pkg(&[("b-1.whl", "ff")]));
        let tampered_bytes = link_bytes(&tampered).unwrap();
        let broken = vec![(0, b0, l0), (1, tampered_bytes, tampered), (2, b2, l2)];
        let violations = check_integrity(&broken);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].kind, "broken-link");
    }

    #[test]
    fn integrity_catches_a_seq_gap() {
        let l0 = link(0, "", Delta::new());
        let b0 = link_bytes(&l0).unwrap();
        let l2 = link(2, &sha256_hex(&b0), Delta::new());
        let b2 = link_bytes(&l2).unwrap();
        let gapped = vec![(0, b0, l0), (2, b2, l2)];
        let kinds: Vec<&str> = check_integrity(&gapped).iter().map(|v| v.kind).collect();
        assert!(
            kinds.contains(&"gap"),
            "a missing seq must be caught: {kinds:?}"
        );
    }
}
