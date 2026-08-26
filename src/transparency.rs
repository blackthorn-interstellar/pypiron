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
//! Across buckets the chain is one chain, not one per bucket. Every audit
//! reconciles the fleet *before* it appends ([`catch_up_fleet`]): links are copied
//! in whichever direction lacks them, so the write pin's head is the fleet head by
//! the time a link is written and a leader that just failed over cannot re-use a
//! seq a peer already spent. When a pass cannot establish that — a chain it could
//! not list, or a copy onto the pin that did not finish — it appends nothing and
//! carries the delta to the next audit: a checkpoint that waits is repairable, and
//! a fork written on a head nobody could vouch for is not.
//!
//! The append itself is a create-if-absent CAS on a
//! deterministic arbiter (the first reachable bucket in config order), and a
//! leader that loses that CAS adopts the winner and appends again at the next seq
//! rather than dropping its delta — a dropped delta leaves the chain committing an
//! old sha over new bytes, which `verify-chain` then reports as a tamper that
//! never clears. A delta that no bucket would accept is held in the leader's
//! memory and merged into the next pass; a crash there loses it, and that is the
//! accepted residual (the next audit that sees the package change re-commits it).
//!
//! Exit codes mirror `verify-index`: **0** chain valid and storage matches,
//! **1** a violation (rows on stdout, an expected scriptable outcome), **2** the
//! check could not run. A chain that lags truth (files in storage not yet
//! committed) is fine — verify never faults uncommitted files. A fork — two
//! buckets holding different links at the same seq — exits 1 and names both
//! branches; nothing here resolves it automatically.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::app::PACKAGES_PREFIX;
use crate::hash::sha256_hex;
use crate::sidecar::{Sidecar, MIRROR_QUARANTINED_SUFFIX, SIDECAR_SUFFIX, TOMBSTONE_SUFFIX};
use crate::storage::{is_not_found, Storage, StorageArgs};

/// Namespace for the chain. Classified [`SingletonReplicated`] in the storage
/// layout manifest (`crate::layout`): each immutable, leader-authored link is
/// mirrored to every healthy bucket, and the fleet is reconciled *before* the
/// audit appends (see [`catch_up_fleet`]), so a failover leader continues the one
/// chain instead of restarting at a fresh genesis — or spending a seq a peer
/// already used — either of which would launder the tamper history. The truth
/// fan-out (`replicate.rs`) only touches `packages/`; the chain rides this
/// dedicated create-if-absent write-through instead — links are immutable per seq,
/// so a peer that already holds a seq is never overwritten.
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

/// Every seq a bucket currently holds a link for.
async fn chain_seqs(storage: &dyn Storage) -> Result<BTreeSet<u64>> {
    Ok(storage
        .list_all(CHAIN_PREFIX)
        .await?
        .iter()
        .filter_map(|o| seq_from_key(&o.key))
        .collect())
}

/// Where the fleet's chain stands after [`catch_up_fleet`].
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FleetChain {
    /// Positions in the fleet, config order, of every bucket whose chain this pass
    /// could read and found consistent with the write pin's — the buckets the next
    /// link may be created on. The first is the append arbiter.
    ///
    /// Empty means *append nothing this pass*. That is the verdict whenever this
    /// pass could not establish that the pin's head is the fleet head: a chain it
    /// could not list (the pin's or any peer's), or a pull onto the pin that did
    /// not finish. A head nobody vouched for is exactly how a second link gets
    /// written at a seq a peer already spent, and that fork is permanent —
    /// deferring the checkpoint is not.
    pub in_sync: Vec<usize>,
}

/// Reconcile the chain across the fleet, and report which buckets a new link may
/// be created on.
///
/// This runs *before* the leader appends, and that ordering is the point: once
/// every reachable bucket agrees, the write pin's head is the fleet head, so a
/// leader that just failed over onto a lagging bucket continues the chain instead
/// of spending a seq a peer already used under different bytes. (That fork needed
/// no attacker: a reseed is best-effort, so an ordinary transient error leaves a
/// peer behind, and the next failover forks the chain there.)
///
/// `buckets` is the eligible fleet in config order and `primary` the write pin's
/// position in it. Each peer is compared against the pin and the links either side
/// lacks are copied to it — the pin pushes what a lagging peer never received, and
/// *pulls* what a peer holding a longer chain has. Missing links are a genuine set
/// difference, so a hole below a peer's head is backfilled rather than skipped
/// forever. Every copy is create-if-absent (`put_if_none_match`) and no link is
/// ever renumbered, rewritten, or deleted: `_transparency/` stays append-only under
/// an S3 Object Lock.
///
/// A bucket holding different bytes at a seq the pin also holds is a **fork**.
/// Nothing is written to it in either direction and it can never arbitrate the
/// append: splicing across a divergence would graft one branch's suffix onto the
/// other's prefix and turn a recoverable split into a permanently broken chain.
/// `verify-chain` reports the fork and an operator decides which branch stands —
/// a tamper witness that picked a winner and re-chained the loser would be
/// laundering exactly what it exists to catch.
///
/// Best-effort in one direction only, and the asymmetry is the whole point. A
/// *push* that fails leaves the peer behind, which the append already tolerates
/// — it receives the new link and the hole backfills next pass — so the peer is
/// warned about and kept an arbiter candidate, and one transient write error
/// cannot move the arbiter out from under the fleet. Everything else fails
/// closed, reporting no in-sync buckets at all: a peer whose chain cannot be
/// listed (its head is then unknown, so the pin's cannot be called the fleet
/// head), and a *pull* that did not finish (this bucket is then knowingly short
/// of links a peer already spent). Both used to be waved through, and both wrote
/// the next link at a spent seq — a permanent `chain-diverged` that no later
/// pass repairs, traded for a checkpoint that merely waits.
///
/// "The fleet" is the *eligible* fleet and no more: `buckets` is what
/// [`AppState::singleton_replicas`] handed over, which already drops buckets the
/// health tracker evicted and is empty on a topology-fenced node. So the fail-closed
/// rule binds only to buckets this node still counts — an evicted one is not
/// waited for, and the append proceeds without it. That bounds the stall (a
/// bucket that stays broken stops blocking once it is evicted) and it is also
/// the hole: the chain such a bucket holds is not consulted, so a leader here can
/// spend a seq it already used. See the transparency entry in `private/ROADMAP.md`.
///
/// It runs every audit regardless of churn, so an idle fleet still converges,
/// and in single-bucket mode it is one listing.
///
/// [`AppState::singleton_replicas`]: crate::app::AppState::singleton_replicas
pub async fn catch_up_fleet(
    buckets: &[crate::layout::ReplicaTarget<'_>],
    primary: usize,
) -> FleetChain {
    let Some(pin) = buckets.get(primary) else {
        return FleetChain::default();
    };
    let mut ours = match chain_seqs(pin.storage).await {
        Ok(seqs) => seqs,
        Err(e) => {
            warn!(bucket = %pin.name, error = ?e, "transparency: listing the chain failed; checkpoint deferred this pass");
            return FleetChain::default();
        }
    };
    let mut in_sync = vec![primary];
    // Deferring the append is decided at the end, not on the spot: reconciling
    // the rest of the fleet is still worth doing (every copy is create-if-absent
    // and none of them can fork), and a peer that only this node can reach must
    // not be starved of links because an earlier one in config order went quiet.
    let mut defer = false;
    for (position, peer) in buckets.iter().enumerate() {
        if position == primary {
            continue;
        }
        // A peer we cannot list is a peer whose head we do not know, and this
        // pass's one claim — that the pin's head is the fleet head — is exactly
        // what an unknown head denies. Skipping it here (it arbitrates nothing,
        // it is copied nothing) still left the append free to spend a seq the
        // silent peer had already used under different bytes, discovered only
        // when it came back. Defer instead.
        let theirs = match chain_seqs(peer.storage).await {
            Ok(seqs) => seqs,
            Err(e) => {
                warn!(bucket = %peer.name, error = ?e, "transparency: listing a peer's chain failed; its head is unknown, so the checkpoint is deferred this pass");
                defer = true;
                continue;
            }
        };
        match catch_up_replica(pin, peer, &mut ours, &theirs).await {
            Ok(CatchUp::Synced) => in_sync.push(position),
            // Forked: warned inside, written to by nobody, arbitrates nothing.
            Ok(CatchUp::Forked) => {}
            // Only a failure that leaves THIS bucket short reaches here (a copy
            // toward the peer is swallowed inside). Its head is therefore not
            // the fleet head, and appending on it would fork the chain.
            Err(e) => {
                warn!(bucket = %peer.name, error = ?e, "transparency: this bucket's own chain catch-up did not complete; checkpoint deferred this pass");
                defer = true;
            }
        }
    }
    if defer {
        return FleetChain::default();
    }
    in_sync.sort_unstable();
    FleetChain { in_sync }
}

/// One peer's outcome in [`catch_up_fleet`].
#[derive(Debug, PartialEq, Eq)]
enum CatchUp {
    /// The peer holds this chain (after any copying) — a valid append target.
    /// Also the verdict for a peer left *behind* by a copy that failed: it holds
    /// a prefix of this chain and nothing else, which the append tolerates.
    Synced,
    /// The peer holds a different link at a seq the primary also holds.
    Forked,
}

/// Reconcile one peer with the primary's chain: refuse a fork, then copy each
/// side's missing links to the other, oldest first so neither chain grows a hole
/// it did not already have. `ours` is the primary's seq set and gains whatever is
/// pulled, so a later peer in the same pass is compared against the full chain.
///
/// `Err` is reserved for a failure that leaves the *primary* unable to claim the
/// fleet head — an unfinished pull, or a fork probe that could not be read — and
/// the caller turns it into "append nothing this pass". A failure copying toward
/// the peer is not that: it is warned about here and reported `Synced`, leaving
/// the peer an arbiter candidate exactly as before.
async fn catch_up_replica(
    primary: &crate::layout::ReplicaTarget<'_>,
    replica: &crate::layout::ReplicaTarget<'_>,
    ours: &mut BTreeSet<u64>,
    theirs: &BTreeSet<u64>,
) -> Result<CatchUp> {
    if let Some(seq) = first_contradiction(primary, replica, ours, theirs).await? {
        warn!(
            seq,
            branch = %primary.name,
            branch_head = ?ours.iter().next_back(),
            other = %replica.name,
            other_head = ?theirs.iter().next_back(),
            "transparency: chain fork — two buckets hold different links at the same seq; \
             nothing copied either way. verify-chain reports it; resolving it is an \
             out-of-band operator decision"
        );
        return Ok(CatchUp::Forked);
    }
    // A genuine set difference, not `peer_head + 1`: a peer whose chain has a hole
    // below its head gets that hole backfilled instead of faulting forever.
    let pull: Vec<u64> = theirs.difference(ours).copied().collect();
    let push: Vec<u64> = ours.difference(theirs).copied().collect();
    // Pull first: a peer that ran ahead (it led while this bucket was the lagging
    // one) is the half of catch-up that was missing, and the reason a failover
    // leader could re-use a spent seq.
    for seq in pull {
        let bytes = replica
            .storage
            .get_bytes(&chain_key(seq))
            .await
            .with_context(|| format!("reading chain link {seq} from bucket {}", replica.name))?;
        // A divergence here is this bucket's own chain contradicting the link we
        // just pulled — a rival leader wrote our missing seq while we worked.
        // Counting it as ours (what a swallowed fork did) would claim a head we
        // do not hold, so the whole pass defers rather than append on it.
        if copy_link(primary, seq, &bytes).await? == Copied::Diverged {
            bail!(
                "chain link {seq} from bucket {} contradicts the link bucket {} already holds \
                 at that seq",
                replica.name,
                primary.name
            );
        }
        ours.insert(seq);
    }
    // Pushing is the direction that can fail without costing this pass anything:
    // the peer stays on a prefix of our chain, which is a state the append
    // already handles (it takes the new link and the hole backfills next pass).
    // Stopping at the first failure keeps the copies oldest-first — the rest of
    // the run would only widen the gap it already left.
    for seq in push {
        let bytes = match primary.storage.get_bytes(&chain_key(seq)).await {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(seq, bucket = %primary.name, error = ?e, "transparency: could not read our own chain link to copy it out; the peer stays behind and retries next audit");
                break;
            }
        };
        match copy_link(replica, seq, &bytes).await {
            Ok(Copied::Landed) => {}
            // The peer gained a rival link at this seq since we listed it. It is
            // a branch, not a lagging copy: nothing more goes to it, and it must
            // not arbitrate — an append there would splice our suffix onto its
            // prefix, which is the one thing catch-up may never do.
            Ok(Copied::Diverged) => return Ok(CatchUp::Forked),
            Err(e) => {
                warn!(seq, bucket = %replica.name, error = ?e, "transparency: copying a chain link to a peer failed; the peer stays behind and retries next audit");
                break;
            }
        }
    }
    Ok(CatchUp::Synced)
}

/// The lowest and highest seq both buckets hold, compared byte for byte; `None`
/// when they agree everywhere compared. Two probes rather than the whole overlap:
/// each link commits the previous link's exact bytes, so agreement at one seq
/// carries down the prefix of any chain that passes its own integrity check — and
/// auditing that is [`check_integrity`]'s job, not this write path's.
async fn first_contradiction(
    primary: &crate::layout::ReplicaTarget<'_>,
    replica: &crate::layout::ReplicaTarget<'_>,
    ours: &BTreeSet<u64>,
    theirs: &BTreeSet<u64>,
) -> Result<Option<u64>> {
    let shared: Vec<u64> = ours.intersection(theirs).copied().collect();
    let probes: BTreeSet<u64> = shared
        .first()
        .into_iter()
        .chain(shared.last())
        .copied()
        .collect();
    for seq in probes {
        let (mine, theirs) = futures::future::try_join(
            primary.storage.get_bytes(&chain_key(seq)),
            replica.storage.get_bytes(&chain_key(seq)),
        )
        .await
        .with_context(|| {
            format!(
                "comparing chain link {seq} across buckets {} and {}",
                primary.name, replica.name
            )
        })?;
        if mine != theirs {
            return Ok(Some(seq));
        }
    }
    Ok(None)
}

/// Create-if-absent copy of one link into `dst`. A seq already taken is left
/// standing — links are immutable — but the CAS result is not thrown away: bytes
/// that differ from ours at that seq are a fork someone else created while we
/// worked, and saying so is free here.
pub(crate) async fn copy_link(
    dst: &crate::layout::ReplicaTarget<'_>,
    seq: u64,
    bytes: &[u8],
) -> Result<Copied> {
    let key = chain_key(seq);
    let taken = dst
        .storage
        .put_if_none_match(&key, bytes.to_vec())
        .await
        .with_context(|| format!("writing chain link {seq} to bucket {}", dst.name))?
        .is_none();
    if taken {
        let held = dst
            .storage
            .get_bytes(&key)
            .await
            .with_context(|| format!("reading back chain link {seq} on bucket {}", dst.name))?;
        if held != bytes {
            warn!(
                seq,
                bucket = %dst.name,
                "transparency: chain fork — this bucket already holds different bytes at that \
                 seq; left as it stands. verify-chain reports it; resolving it is an \
                 out-of-band operator decision"
            );
            return Ok(Copied::Diverged);
        }
    }
    Ok(Copied::Landed)
}

/// What a create-if-absent copy found at the destination's seq.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Copied {
    /// The link is on the destination — written by this copy, or already there
    /// byte for byte.
    Landed,
    /// The destination already holds *different* bytes at that seq: two branches
    /// of one history, and nothing was written. Reporting it is the point — a
    /// swallowed divergence let the peer stay an append target (so the next link
    /// grafted this branch's suffix onto that one's prefix) and let a pull count
    /// a seq as ours that we hold under other bytes.
    Diverged,
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
        // One row per bucket, at the first seq that contradicts the reference:
        // every later seq on a forked branch differs too, and printing each one
        // buries the fact that matters — where the two histories parted.
        let diverged = chain
            .links
            .iter()
            .find(|(s, b, _)| ref_shas.get(s).is_none_or(|rs| rs != &sha256_hex(b)));
        if let Some((s, _, _)) = diverged {
            violations.push((
                chain.name.to_string(),
                Violation {
                    kind: "chain-diverged",
                    package: String::new(),
                    detail: format!(
                        "histories part at link seq {s}: bucket {} holds one branch (head seq \
                         {}), bucket {} another (head seq {ref_head}). No file is implicated. \
                         Decide out of band which branch is your history — nothing merges them \
                         automatically",
                        chain.name,
                        chain.head().unwrap_or(0),
                        ref_chain.name,
                    ),
                },
            ));
        }
        if diverged.is_none() && chain.head() != Some(ref_head) {
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
    //     never for one bucket's missing sidecar. The chain (reconciled whole in a
    //     single audit — `catch_up_fleet`) and sidecars (fan-out
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
    ///
    /// Faulted unconditionally, including over the one legitimate in-place byte
    /// change: a mirror→private supersede. Deliberate, and the bar to soften it
    /// is not met. Nothing durable in storage tells the two apart — the demotion
    /// fence is deleted the moment private truth stands under it
    /// (`replicate::clear_spent_demotion_fence`), and every other candidate
    /// witness (that fence, the `_quarantine/` copy of the committed bytes) is an
    /// object the attacker this whole module exists for — one holding storage
    /// credentials — writes as cheaply as the rewrite itself. Honouring one would
    /// sell a two-line bypass of the only signal in the system that catches a
    /// silent byte rewrite. Absence is a different question, which is why
    /// [`Presence::Covered`] exists: a marker authorizes a *disappearance*, and a
    /// disappearance serves nobody the attacker's bytes.
    ///
    /// Both rows a real supersede can produce are worth printing. Before the next
    /// audit re-commits the package the row names the operator's own change, and
    /// it clears on that audit (which reports the change once more, as an
    /// in-chain sha move — [`warn_in_chain_sha_changes`]). After it, a row means
    /// some bucket is still serving the *withdrawn* body — the exact fail-open
    /// the demotion exists to close, and never noise.
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
    use crate::sidecar::{mirror_quarantined_key, sidecar_key, Yanked};
    use crate::storage::test_support::InMemStorage;

    const PKG: &str = "six";
    const FILE: &str = "six-1.16.0-py2.py3-none-any.whl";
    /// The sha the chain committed for `FILE`.
    const COMMITTED: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn akey() -> String {
        format!("{PACKAGES_PREFIX}{PKG}/{FILE}")
    }

    fn sidecar_bytes(sha: &str, origin: &str) -> Vec<u8> {
        serde_json::to_vec(&Sidecar {
            sha256: sha.to_string(),
            size: 42,
            version: "1.16.0".to_string(),
            upload_time: "2026-07-17T00:00:00Z".to_string(),
            upload_epoch_ms: None,
            requires_python: None,
            yanked: Yanked::default(),
            origin: Some(origin.to_string()),
            yank_epoch: 0,
            snapshot: false,
            store_checksum: None,
        })
        .expect("serializing a test sidecar")
    }

    async fn probe(storage: &InMemStorage) -> Presence {
        probe_presence(storage, PKG, FILE, COMMITTED)
            .await
            .expect("probing an in-memory bucket cannot fail")
    }

    /// The demotion fence authorizes the disappearance it caused. A settled
    /// mirror→private supersede drops the demoted record and leaves only
    /// `.mirror-quarantined` standing (`replicate::settle_mirror_quarantine`),
    /// while the chain goes on committing the withdrawn filename until the next
    /// audit. Reading that gap as `vanished` turns every operator supersede into
    /// a fleet-wide tamper alarm.
    #[tokio::test]
    async fn a_demotion_fence_authorizes_the_committed_record_it_withdrew() {
        let storage = InMemStorage::default();
        storage.insert(&mirror_quarantined_key(&akey()), b"{}".to_vec());
        assert!(
            matches!(probe(&storage).await, Presence::Covered),
            "a demoted record must read as covered, not vanished"
        );
    }

    /// The other direction: nothing authorizing the absence is still a vanish.
    /// Without this the arm above could be widened to "absent is fine" and the
    /// out-of-band-delete signal would go quiet.
    #[tokio::test]
    async fn a_committed_record_gone_with_nothing_authorizing_it_is_absent() {
        let storage = InMemStorage::default();
        assert!(
            matches!(probe(&storage).await, Presence::Absent),
            "an unauthorized disappearance must stay a vanish"
        );
    }

    /// A completed supersede is NOT excused. The private body that replaced the
    /// committed one reports `hash-changed` even with the demotion fence still
    /// beside it — see [`Presence::WrongSha`]. Every witness a suppression could
    /// key on is an object the credential-holding attacker writes just as easily,
    /// and this is the only check that catches a silent byte rewrite.
    #[tokio::test]
    async fn a_superseded_body_still_faults_under_its_own_demotion_fence() {
        let storage = InMemStorage::default();
        let replacement = "2".repeat(64);
        storage.insert(
            &sidecar_key(&akey()),
            sidecar_bytes(&replacement, crate::origin::PRIVATE),
        );
        storage.insert(&mirror_quarantined_key(&akey()), b"{}".to_vec());
        match probe(&storage).await {
            Presence::WrongSha(actual) => assert_eq!(actual, replacement),
            _ => panic!("a rewritten body must fault regardless of the markers beside it"),
        }
    }

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

    /// A correctly chained run of `count` links whose deltas name `tag`, so two
    /// runs built under different tags differ byte for byte at every seq.
    fn chain_of(count: u64, tag: &str) -> Vec<Vec<u8>> {
        let mut prev = String::new();
        let mut out = Vec::new();
        for seq in 0..count {
            let mut packages = Delta::new();
            packages.insert(tag.to_string(), pkg(&[("a-1.whl", &format!("{tag}{seq}"))]));
            let bytes = link_bytes(&link(seq, &prev, packages)).expect("serializing a test link");
            prev = sha256_hex(&bytes);
            out.push(bytes);
        }
        out
    }

    /// A branch sharing `base`'s links below `seq` and holding a different — but
    /// internally valid — link at `seq`. This is what a failover leader produced:
    /// same history, then two futures.
    fn fork_of(base: &[Vec<u8>], seq: u64, tag: &str) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = base[..seq as usize].to_vec();
        let prev = out.last().map(|b| sha256_hex(b)).unwrap_or_default();
        let mut packages = Delta::new();
        packages.insert(tag.to_string(), pkg(&[("a-1.whl", tag)]));
        out.push(link_bytes(&link(seq, &prev, packages)).expect("serializing a test link"));
        out
    }

    fn seed_chain(storage: &InMemStorage, links: &[Vec<u8>], seqs: &[u64]) {
        for &seq in seqs {
            storage.insert(&chain_key(seq), links[seq as usize].clone());
        }
    }

    async fn held_seqs(storage: &InMemStorage) -> Vec<u64> {
        chain_seqs(storage)
            .await
            .expect("listing an in-memory chain")
            .into_iter()
            .collect()
    }

    /// Drive one peer's catch-up exactly as [`catch_up_fleet`] does.
    async fn catch_up(primary: &InMemStorage, peer: &InMemStorage) -> CatchUp {
        let mut ours = chain_seqs(primary)
            .await
            .expect("listing the primary chain");
        let theirs = chain_seqs(peer).await.expect("listing the peer chain");
        let pin = crate::layout::ReplicaTarget {
            storage: primary,
            name: "bucket-a",
        };
        let replica = crate::layout::ReplicaTarget {
            storage: peer,
            name: "bucket-b",
        };
        catch_up_replica(&pin, &replica, &mut ours, &theirs)
            .await
            .expect("catch-up across in-memory buckets")
    }

    /// The missing half of catch-up, and the reported bug: a peer holding links
    /// this bucket lacks used to be waved through as "identical or a divergence
    /// for verify-chain", so the pin stayed behind forever and the next append
    /// re-used a seq the peer had already spent. Pull them instead.
    #[tokio::test]
    async fn a_peer_holding_more_of_the_same_chain_is_pulled_back() {
        let links = chain_of(4, "same");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &links, &[0, 1]);
        seed_chain(&peer, &links, &[0, 1, 2, 3]);

        assert_eq!(catch_up(&primary, &peer).await, CatchUp::Synced);
        assert_eq!(
            held_seqs(&primary).await,
            vec![0, 1, 2, 3],
            "the peer's extra links must be pulled onto the pin"
        );
        assert_eq!(held_seqs(&peer).await, vec![0, 1, 2, 3]);
    }

    /// Two branches of the same history must not be spliced together in either
    /// direction. Grafting one branch's suffix onto the other's prefix turns a
    /// recoverable split into a permanently broken chain; picking a winner would
    /// launder the tamper evidence this module exists to keep.
    #[tokio::test]
    async fn a_fork_is_never_spliced_in_either_direction() {
        let ours = chain_of(3, "left");
        let theirs = fork_of(&ours, 1, "right");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &ours, &[0, 1, 2]);
        seed_chain(&peer, &theirs, &[0, 1]);

        assert_eq!(catch_up(&primary, &peer).await, CatchUp::Forked);
        assert_eq!(held_seqs(&primary).await, vec![0, 1, 2]);
        assert_eq!(held_seqs(&peer).await, vec![0, 1], "no link may be added");
        for (storage, links) in [(&primary, &ours), (&peer, &theirs)] {
            for (seq, bytes) in links.iter().enumerate().take(2) {
                assert_eq!(
                    storage.get_bytes(&chain_key(seq as u64)).await.unwrap(),
                    *bytes,
                    "seq {seq} must be left exactly as its own branch wrote it"
                );
            }
        }
    }

    /// Catch-up is a set difference, not `head + 1`: a peer that lost a link below
    /// its head used to be judged current and kept its hole forever, faulting
    /// `verify-chain` on a gap nobody could repair.
    #[tokio::test]
    async fn a_hole_below_the_peers_head_is_backfilled() {
        let links = chain_of(3, "same");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &links, &[0, 1, 2]);
        seed_chain(&peer, &links, &[0, 2]);

        assert_eq!(catch_up(&primary, &peer).await, CatchUp::Synced);
        assert_eq!(
            held_seqs(&peer).await,
            vec![0, 1, 2],
            "the hole below the peer's head must be backfilled"
        );
    }

    /// [`catch_up_replica`] driven with the seq sets a pass *listed*, which is
    /// not always what the buckets hold by the time it copies — that window is
    /// the whole reason the copies are create-if-absent.
    async fn catch_up_as_listed(
        primary: &InMemStorage,
        peer: &InMemStorage,
        ours: &[u64],
        theirs: &[u64],
    ) -> Result<CatchUp> {
        let mut ours: BTreeSet<u64> = ours.iter().copied().collect();
        let theirs: BTreeSet<u64> = theirs.iter().copied().collect();
        let pin = crate::layout::ReplicaTarget {
            storage: primary,
            name: "bucket-a",
        };
        let replica = crate::layout::ReplicaTarget {
            storage: peer,
            name: "bucket-b",
        };
        catch_up_replica(&pin, &replica, &mut ours, &theirs).await
    }

    /// A peer that gained a rival link at a seq we were about to push is a
    /// branch, not a lagging copy. The copy itself always refused to overwrite
    /// it — but reporting that refusal as "synced" left the peer an append
    /// target, and the next link grafted this branch's suffix onto that one's
    /// prefix: a fork built by the very path that exists to refuse splicing.
    #[tokio::test]
    async fn a_peer_that_gained_a_rival_link_since_the_listing_is_forked_not_synced() {
        let ours = chain_of(3, "left");
        let theirs = fork_of(&ours, 2, "right");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &ours, &[0, 1, 2]);
        seed_chain(&peer, &theirs, &[0, 1, 2]);

        // What this pass listed: the peer had not written its own seq 2 yet.
        let outcome = catch_up_as_listed(&primary, &peer, &[0, 1, 2], &[0, 1])
            .await
            .expect("a refused copy is a verdict, not a failure");

        assert_eq!(
            outcome,
            CatchUp::Forked,
            "a rival link at a seq we tried to push must disqualify the peer, not pass as synced"
        );
        assert_eq!(
            peer.get_bytes(&chain_key(2)).await.unwrap(),
            theirs[2],
            "the peer's own link must stand exactly as its branch wrote it"
        );
    }

    /// The mirror image, and the worse one: a link pulled from a peer lands on a
    /// seq this bucket already holds under other bytes. Counting it as ours (the
    /// old swallow) claimed a head we do not hold, and the append numbered off
    /// it. There is no head to vouch for here, so the pass defers.
    #[tokio::test]
    async fn a_pull_onto_a_seq_we_hold_under_other_bytes_defers_the_checkpoint() {
        let ours = chain_of(3, "left");
        let theirs = fork_of(&ours, 2, "right");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &ours, &[0, 1, 2]);
        seed_chain(&peer, &theirs, &[0, 1, 2]);

        // What this pass listed: our own seq 2 had not landed yet.
        let outcome = catch_up_as_listed(&primary, &peer, &[0, 1], &[0, 1, 2]).await;

        assert!(
            outcome.is_err(),
            "a pulled link contradicting our own must defer the pass, not be counted as ours"
        );
        assert_eq!(
            primary.get_bytes(&chain_key(2)).await.unwrap(),
            ours[2],
            "our own link must stand exactly as this branch wrote it"
        );
    }

    /// Two buckets in config order, the first being the write pin.
    fn fleet<'a>(
        primary: &'a InMemStorage,
        peer: &'a InMemStorage,
    ) -> Vec<crate::layout::ReplicaTarget<'a>> {
        vec![
            crate::layout::ReplicaTarget {
                storage: primary,
                name: "bucket-a",
            },
            crate::layout::ReplicaTarget {
                storage: peer,
                name: "bucket-b",
            },
        ]
    }

    /// The reported bug, at the level that produced it: one failed read while
    /// pulling an ahead peer's links left this bucket short, and the peer was
    /// reported in-sync anyway. The append then numbered off this bucket's stale
    /// head and spent a seq the peer already held under different bytes — a
    /// `chain-diverged` that every later pass refuses to touch. Nothing to
    /// append on is the only safe answer.
    #[tokio::test]
    async fn a_pull_that_could_not_finish_defers_the_checkpoint() {
        let links = chain_of(4, "same");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &links, &[0, 1]);
        seed_chain(&peer, &links, &[0, 1, 2, 3]);
        peer.fail_reads_of(&chain_key(2));

        let synced = catch_up_fleet(&fleet(&primary, &peer), 0).await;

        assert!(
            synced.in_sync.is_empty(),
            "a bucket that knows it is short of a peer's links must arbitrate nothing: {:?}",
            synced.in_sync
        );
        assert_eq!(
            held_seqs(&primary).await,
            vec![0, 1],
            "the failed pull must not have half-applied"
        );
    }

    /// A peer whose chain cannot be listed has an unknown head, and this pass's
    /// only claim is that the pin's head IS the fleet head. Skipping the peer
    /// used to leave the append free to spend a seq the silent bucket had
    /// already used — a fork discovered when it came back, and never repaired.
    #[tokio::test]
    async fn a_peer_whose_chain_cannot_be_listed_defers_the_checkpoint() {
        let links = chain_of(2, "same");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &links, &[0, 1]);
        seed_chain(&peer, &links, &[0, 1]);
        peer.fail_lists_of(CHAIN_PREFIX);

        let synced = catch_up_fleet(&fleet(&primary, &peer), 0).await;

        assert!(
            synced.in_sync.is_empty(),
            "an unknown peer head must defer the append, not license one on the pin's own head: {:?}",
            synced.in_sync
        );
    }

    /// The half that must NOT fail closed, and the reason the two directions are
    /// split at all: a copy toward the peer that fails leaves the peer on a
    /// prefix of this chain — a state the append already handles — so it stays
    /// an arbiter candidate. Failing closed here would let one transient write
    /// error move the arbiter out from under the fleet every pass.
    #[tokio::test]
    async fn a_push_that_failed_still_leaves_the_peer_an_append_target() {
        let links = chain_of(3, "same");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &links, &[0, 1, 2]);
        seed_chain(&peer, &links, &[0, 1]);
        peer.fail_writes_of(&chain_key(2));

        let synced = catch_up_fleet(&fleet(&primary, &peer), 0).await;

        assert_eq!(
            synced.in_sync,
            vec![0, 1],
            "a peer left behind by a failed copy is still a valid append target"
        );
        assert_eq!(
            held_seqs(&peer).await,
            vec![0, 1],
            "and it is left exactly where the failed copy left it"
        );
    }

    /// The ordinary reseed still works: an empty (freshly added) bucket is seeded
    /// the whole chain, oldest first.
    #[tokio::test]
    async fn an_empty_peer_is_seeded_the_whole_chain() {
        let links = chain_of(3, "same");
        let primary = InMemStorage::default();
        let peer = InMemStorage::default();
        seed_chain(&primary, &links, &[0, 1, 2]);

        assert_eq!(catch_up(&primary, &peer).await, CatchUp::Synced);
        assert_eq!(held_seqs(&peer).await, vec![0, 1, 2]);
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
