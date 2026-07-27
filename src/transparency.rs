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

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::app::PACKAGES_PREFIX;
use crate::hash::sha256_hex;
use crate::sidecar::{Sidecar, MIRROR_QUARANTINED_SUFFIX, SIDECAR_SUFFIX, TOMBSTONE_SUFFIX};
use crate::storage::{is_not_found, Storage, StorageArgs};

/// Namespace for the chain. Classified [`SingletonReplicated`] in the storage
/// layout manifest (`crate::layout`): each immutable, leader-authored link is
/// written through to every healthy bucket after the audit commits it (see
/// [`reseed_chain_to_peers`]), so a failover leader reads the latest seq from its
/// own bucket and *continues* the chain instead of restarting at a fresh genesis
/// that would launder the tamper history. The truth fan-out (`replicate.rs`) only
/// touches `packages/`; the chain rides this dedicated create-if-absent
/// write-through instead — links are immutable per seq, so a peer that already
/// holds a seq is never overwritten.
///
/// [`SingletonReplicated`]: crate::layout::Class::SingletonReplicated
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

/// Mirror the primary's current chain head to every healthy peer that lags it,
/// so a failover to any bucket continues this chain rather than starting a fresh
/// genesis that would launder tamper history. Each immutable link is written
/// create-if-absent (`put_if_none_match`): a peer already holding a seq is left
/// untouched, and a peer holding a *divergent* seq (a forked chain) is never
/// overwritten — `verify-chain` adjudicates that divergence rather than laundering
/// it here.
///
/// This is both the write-through (the leader just appended a link) and the
/// backstop (a peer that missed writes, or a freshly added bucket, is backfilled).
/// It runs every audit regardless of churn, so an idle fleet still converges.
/// Best-effort and a no-op in single-bucket mode (empty `replicas`) or on an empty
/// chain.
pub async fn reseed_chain_to_peers(
    primary: &dyn Storage,
    replicas: &[crate::layout::ReplicaTarget<'_>],
) {
    if replicas.is_empty() {
        return;
    }
    let (seq, bytes) = match read_head(primary).await {
        Ok(Some(head)) => head,
        Ok(None) => return, // no chain yet — nothing to mirror
        Err(e) => {
            warn!(error = ?e, "transparency: reading chain head for peer reseed failed; retries next audit");
            return;
        }
    };
    for replica in replicas {
        if let Err(e) = catch_up_replica(primary, replica, seq, &bytes).await {
            warn!(
                bucket = %replica.name,
                seq,
                error = ?e,
                "transparency: chain reseed to a peer failed; retries next audit"
            );
        }
    }
}

/// Copy every chain link a single peer is missing, from its head up to `seq`
/// (whose bytes are `head_bytes`), oldest first so the peer's chain stays a
/// gapless prefix. A peer already current with (or ahead of) `seq` is a no-op;
/// an empty peer (a freshly added bucket) is seeded the whole chain.
async fn catch_up_replica(
    primary: &dyn Storage,
    replica: &crate::layout::ReplicaTarget<'_>,
    seq: u64,
    head_bytes: &[u8],
) -> Result<()> {
    let start = match read_head(replica.storage).await? {
        // Current or ahead: a longer/equal peer chain is either identical (done)
        // or a divergence for verify-chain to catch — never overwrite it.
        Some((peer_head, _)) if peer_head >= seq => return Ok(()),
        Some((peer_head, _)) => peer_head + 1,
        None => 0,
    };
    for s in start..=seq {
        let bytes = if s == seq {
            head_bytes.to_vec()
        } else {
            primary.get_bytes(&chain_key(s)).await?
        };
        replica
            .storage
            .put_if_none_match(&chain_key(s), bytes)
            .await
            .with_context(|| format!("writing chain link {s} to peer {}", replica.name))?;
    }
    Ok(())
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

/// One bucket's loaded chain, for the cross-bucket verification.
struct BucketChain<'a> {
    name: &'a str,
    storage: &'a dyn Storage,
    /// Links sorted ascending by seq: `(seq, on-disk bytes, parsed link)`.
    links: Vec<(u64, Vec<u8>, ChainLink)>,
}

impl BucketChain<'_> {
    /// The highest committed seq, or `None` for an empty chain.
    fn head(&self) -> Option<u64> {
        self.links.last().map(|(s, _, _)| *s)
    }
}

/// Load one bucket's chain, sorted by seq. A key that does not parse as a seq is
/// not a chain link and is dropped in the same pass.
async fn load_chain(storage: &dyn Storage) -> Result<Vec<(u64, Vec<u8>, ChainLink)>> {
    let mut entries: Vec<(u64, String)> = storage
        .list_all(CHAIN_PREFIX)
        .await?
        .into_iter()
        .filter_map(|o| seq_from_key(&o.key).map(|seq| (seq, o.key)))
        .collect();
    entries.sort_by_key(|(seq, _)| *seq);
    let mut links = Vec::with_capacity(entries.len());
    for (seq, key) in &entries {
        let bytes = storage.get_bytes(key).await?;
        let link: ChainLink =
            serde_json::from_slice(&bytes).with_context(|| format!("parsing chain link {key}"))?;
        links.push((*seq, bytes, link));
    }
    Ok(links)
}

/// Warn (not fault) on an in-place sha change of an already-committed filename
/// across a chain — legitimate only for the rare mirror→private demotion. A
/// running replay-so-far gives the "already committed" view.
fn warn_in_chain_sha_changes(links: &[(u64, Vec<u8>, ChainLink)]) {
    let mut so_far: Delta = BTreeMap::new();
    for (_, _, link) in links {
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
}

/// Run the read-only chain verification across every configured bucket. `Ok(true)`
/// = every bucket's chain is valid and its storage matches, `Ok(false)` = a
/// violation (rows + summary already on stdout), `Err` = the check could not run.
/// The caller maps these to exit 0 / 1 / 2.
///
/// Multi-bucket semantics: (a) each bucket's chain must hash-link internally;
/// (b) every bucket's chain must be a prefix of the longest valid chain — a
/// shorter one is merely *lagging* (reported, not a fault), a divergent or
/// restarted one is a violation; (c) the longest valid chain is replayed and its
/// expected `(package, filename, sha256)` state is diffed against every bucket's
/// sidecars. A present sidecar whose sha contradicts the commitment is caught on
/// **any** single bucket (`hash-changed`), so a silent byte-rewrite anywhere
/// faults. A committed file gone with no tombstone is a `vanished` fault only when
/// it is absent from **every** bucket — a truth still held on any peer is intact,
/// and one bucket's missing sidecar is ordinary replication lag, never faulted
/// (which also denies an attacker a per-bucket vanish exemption via chain
/// truncation). The walk is metadata-only — chain links plus sidecar reads, never
/// artifact bytes.
pub async fn run_verify_chain(args: VerifyChainArgs) -> Result<bool> {
    let storages = args.storage.build_all().await?;
    let names = args.storage.bucket_names();

    // Load every bucket's chain. A storage error is could-not-run (exit 2): a
    // bucket we cannot read cannot be verified, and a silent skip could hide a
    // tamper.
    let mut chains: Vec<BucketChain> = Vec::with_capacity(storages.len());
    for (storage, name) in storages.iter().zip(names.iter()) {
        let links = load_chain(storage.as_ref())
            .await
            .with_context(|| format!("loading chain from bucket {name}"))?;
        chains.push(BucketChain {
            name,
            storage: storage.as_ref(),
            links,
        });
    }

    // Bucket-tagged violations (exit 1) and lagging reports (informational).
    let mut violations: Vec<(String, Violation)> = Vec::new();

    // 1. Per-bucket integrity. A broken chain can neither be the reference nor be
    // prefix-checked, so exclude it from reference selection; its rows still
    // report and fault.
    let mut integrity_broken: HashSet<usize> = HashSet::new();
    for (i, chain) in chains.iter().enumerate() {
        let iv = check_integrity(&chain.links);
        if !iv.is_empty() {
            integrity_broken.insert(i);
            for v in iv {
                violations.push((chain.name.to_string(), v));
            }
        }
    }

    // 2. Reference = the longest integrity-clean, non-empty chain. A tie keeps the
    // first in bucket order; a same-length divergent peer then fails the prefix
    // check in step 3.
    let reference = chains
        .iter()
        .enumerate()
        .filter(|(i, c)| !integrity_broken.contains(i) && !c.links.is_empty())
        .max_by_key(|(_, c)| c.head().unwrap_or(0))
        .map(|(i, _)| i);

    let Some(ref_idx) = reference else {
        // No usable reference: either every bucket is empty (no chain anywhere)
        // or every present chain is integrity-broken.
        if violations.is_empty() {
            println!("no chain");
            return Ok(true);
        }
        print_violations(&violations);
        println!(
            "verify-chain: {} bucket(s), no valid chain to replay ({} violation(s))",
            chains.len(),
            violations.len()
        );
        return Ok(false);
    };

    let ref_chain = &chains[ref_idx];
    let ref_head = ref_chain.head().unwrap_or(0);
    // seq → sha of the reference links, for the prefix comparison.
    let ref_shas: BTreeMap<u64, String> = ref_chain
        .links
        .iter()
        .map(|(s, b, _)| (*s, sha256_hex(b)))
        .collect();

    // 3. Cross-bucket prefix check. Each other integrity-clean chain must match
    // the reference on every seq it holds. A mismatch is a divergence (a restarted
    // or tampered chain) → violation; a clean but shorter chain is lagging →
    // reported, not a fault.
    let mut lagging: Vec<(String, Option<u64>)> = Vec::new();
    for (i, chain) in chains.iter().enumerate() {
        if i == ref_idx || integrity_broken.contains(&i) {
            continue;
        }
        let mut diverged = false;
        for (s, b, _) in &chain.links {
            let matches = ref_shas.get(s).is_some_and(|rs| rs == &sha256_hex(b));
            if !matches {
                diverged = true;
                violations.push((
                    chain.name.to_string(),
                    Violation {
                        kind: "chain-diverged",
                        package: String::new(),
                        detail: format!(
                            "link seq {s} differs from the longest chain (bucket {}); \
                             a restarted or tampered chain, not a prefix",
                            ref_chain.name
                        ),
                    },
                ));
            }
        }
        if !diverged && chain.head() != Some(ref_head) {
            lagging.push((chain.name.to_string(), chain.head()));
        }
    }

    // 4. Warn on a legitimate-looking in-place sha change across the reference.
    warn_in_chain_sha_changes(&ref_chain.links);

    // 5. Replay the reference and diff its expected state against every bucket's
    // sidecars. Two independent verdicts per committed file:
    //   - hash-changed: a present sidecar whose sha contradicts the commitment is a
    //     tamper on *that* bucket, always faulted — the core silent-rewrite signal.
    //   - vanished: a committed file gone with no tombstone. This is a *fleet*
    //     property — it faults only when the truth is absent from EVERY bucket,
    //     never for one bucket's missing sidecar. The chain (write-through, whole
    //     chain in a single audit — `reseed_chain_to_peers`) and sidecars (fan-out
    //     at upload + reconcile backstop) replicate on independently paced paths, so
    //     a bucket is routinely chain-current while its sidecars still trickle in;
    //     faulting per-bucket absence would fire a tamper alarm during exactly the
    //     onboarding/recovery events this tool audits. Keying vanish on the fleet
    //     also refuses the fail-open where an attacker truncates a peer's newest
    //     chain links to earn a lag exemption and then deletes its sidecars: there
    //     is no per-bucket exemption to earn, and a truth still present on any peer
    //     is intact regardless of chain lag (reconcile re-copies the lost replica).
    let expected = replay(ref_chain.links.iter().map(|(_, _, l)| l));
    let checks: Vec<(&String, &String, &String)> = expected
        .iter()
        .flat_map(|(pkg, files)| files.iter().map(move |(f, sha)| (pkg, f, sha)))
        .collect();

    // Per committed file: is the truth held (matching/present sidecar or tombstone)
    // on any bucket, and which buckets are cleanly absent?
    let mut covered = vec![false; checks.len()];
    let mut absent_in: Vec<Vec<usize>> = vec![Vec::new(); checks.len()];
    for (bi, chain) in chains.iter().enumerate() {
        for (ci, chunk) in checks.chunks(DIFF_CONCURRENCY).enumerate() {
            let probes = chunk
                .iter()
                .map(|(pkg, filename, sha)| probe_presence(chain.storage, pkg, filename, sha));
            for (j, presence) in futures::future::join_all(probes)
                .await
                .into_iter()
                .enumerate()
            {
                let fi = ci * DIFF_CONCURRENCY + j;
                let (pkg, filename, expected_sha) = checks[fi];
                match presence? {
                    // Present truth, or a disappearance the operator authorized.
                    Presence::Match | Presence::Covered => covered[fi] = true,
                    // Present but unparseable: still not a vanish, but flag it.
                    Presence::Corrupt(e) => {
                        covered[fi] = true;
                        violations.push((
                            chain.name.to_string(),
                            Violation {
                                kind: "corrupt-sidecar",
                                package: pkg.clone(),
                                detail: format!("{filename}: {e}"),
                            },
                        ));
                    }
                    // Present, contradicting sha: a tamper on this bucket, always.
                    Presence::WrongSha(actual) => violations.push((
                        chain.name.to_string(),
                        Violation {
                            kind: "hash-changed",
                            package: pkg.clone(),
                            detail: format!(
                                "{filename}: committed {expected_sha} but the sidecar now holds {actual}"
                            ),
                        },
                    )),
                    Presence::Absent => absent_in[fi].push(bi),
                }
            }
        }
    }

    // Fleet-wide vanish: a committed file cleanly absent somewhere and held nowhere.
    for fi in 0..checks.len() {
        if covered[fi] || absent_in[fi].is_empty() {
            continue;
        }
        let (pkg, filename, _) = checks[fi];
        let buckets: Vec<&str> = absent_in[fi].iter().map(|&bi| chains[bi].name).collect();
        violations.push((
            buckets.join(","),
            Violation {
                kind: "vanished",
                package: pkg.clone(),
                detail: format!("{filename}: committed sidecar is gone with no tombstone"),
            },
        ));
    }

    for (name, head) in &lagging {
        match head {
            Some(h) => println!(
                "chain-lagging\t{name}\thead seq {h} of {ref_head} — a prefix of the longest chain (OK)"
            ),
            None => println!(
                "chain-lagging\t{name}\tno chain yet; longest is seq {ref_head} (OK)"
            ),
        }
    }
    print_violations(&violations);
    println!(
        "verify-chain: {} bucket(s), reference bucket {} ({} link(s), {} committed file(s)), \
         {} lagging, {} violation(s)",
        chains.len(),
        ref_chain.name,
        ref_chain.links.len(),
        checks.len(),
        lagging.len(),
        violations.len()
    );
    Ok(violations.is_empty())
}

/// Print bucket-tagged violation rows as `bucket\tkind\tpackage\tdetail`.
fn print_violations(violations: &[(String, Violation)]) {
    for (bucket, v) in violations {
        println!("{bucket}\t{}\t{}\t{}", v.kind, v.package, v.detail);
    }
}

/// One bucket's view of a committed `(package, filename, sha)`, folded into the
/// fleet-wide verdict by `run_verify_chain`.
enum Presence {
    /// Sidecar present, sha matches the commitment.
    Match,
    /// Sidecar present but its sha contradicts the commitment — a tamper.
    WrongSha(String),
    /// Sidecar present but unparseable.
    Corrupt(String),
    /// No sidecar, but a marker authorizes the disappearance: a tombstone (a
    /// legitimate delete) or `.mirror-quarantined` (the operator's own
    /// mirror→private supersede, which drops the demoted record and keeps only
    /// its fence). Either way the chain simply has not caught up yet.
    Covered,
    /// No sidecar and no marker authorizing its absence.
    Absent,
}

/// Classify one committed `(package, filename, sha)` against a single bucket's
/// storage. The sidecar is the sha of record; the caller decides what each
/// presence means for the fleet.
async fn probe_presence(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    expected_sha: &str,
) -> Result<Presence> {
    let base = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    match storage.get_bytes(&format!("{base}{SIDECAR_SUFFIX}")).await {
        Ok(bytes) => match serde_json::from_slice::<Sidecar>(&bytes) {
            Ok(sc) if sc.sha256 == expected_sha => Ok(Presence::Match),
            Ok(sc) => Ok(Presence::WrongSha(sc.sha256)),
            Err(e) => Ok(Presence::Corrupt(e.to_string())),
        },
        Err(e) if is_not_found(&e) => {
            let (tombstoned, demoted) = futures::future::try_join(
                storage.head_exists(&format!("{base}{TOMBSTONE_SUFFIX}")),
                storage.head_exists(&format!("{base}{MIRROR_QUARANTINED_SUFFIX}")),
            )
            .await?;
            if tombstoned || demoted {
                Ok(Presence::Covered)
            } else {
                Ok(Presence::Absent)
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
