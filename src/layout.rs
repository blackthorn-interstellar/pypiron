//! The storage-layout replication-class manifest — one registry naming every
//! top-level key prefix, how it relates to a multi-bucket fleet, and a one-line
//! why. A new prefix that ships without a declared class trips the unit tests
//! below, so the "does this replicate?" decision is never left implicit (the
//! motivating exhibit: the transparency chain's non-replication used to be a
//! silent accident of prefix scoping — now it is a stated class).
//!
//! This module also owns the mechanism for the [`Class::SingletonReplicated`]
//! files: [`write_singleton`] writes a leader-authored control record to the
//! selected bucket (authoritative) and best-effort to every healthy peer, so a
//! failover to any bucket still finds it.

use anyhow::{Context, Result};
use tracing::warn;

use crate::storage::Storage;

/// How a storage key prefix relates to the multi-bucket fleet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Package records — the operator's truth. Replicated to every bucket via
    /// the three-tier fan-out → `_repl/` notes → reconcile machinery, so any
    /// single bucket can serve the whole corpus.
    TruthReplicated,
    /// Leader-authored control files that are not per-package truth but must
    /// exist on every bucket so a failover to any of them stays armed. Written
    /// write-through to all healthy buckets ([`write_singleton`]) and healed by
    /// reseed-if-absent.
    SingletonReplicated,
    /// Regenerable per-bucket views (indexes, audit report, sync cursors,
    /// fingerprint state). Never copied — each bucket rebuilds or re-derives
    /// its own from the truth it holds.
    DerivedPerBucket,
    /// Per-bucket coordination state (leases, `_repl/` notes, topology stamps,
    /// staging, dirty worklist). Bucket-local by design; replicating it would be
    /// wrong, not merely wasteful.
    CoordinationPerBucket,
    /// Leader-authored rolled-up data files — immutable once written — that must
    /// exist on every bucket so a failover finds the whole history, not a hole.
    /// Written write-through / healed by copy-if-absent reseed (the same
    /// mechanism as [`Class::SingletonReplicated`], but many keys rather than one
    /// fixed key). The counter day-rollups are the exhibit: losing them on a
    /// failover would zero the audit's download ranking during the incident it
    /// exists for. See [`crate::counters::Counters::compact`], whose pass both
    /// freezes each bucket's rollups and converges every bucket on their union.
    ReplicatedRollup,
    /// A narrow, explicitly-bounded acceptable-loss window — the totality
    /// exemption for state that is neither derived (not re-computable) nor
    /// coordination scratch. Not replicated by design; the `why` states the loss
    /// bound. Two exhibits, per the DESIGN.md totality principle: the counter
    /// live tallies (the current day's un-rolled-up segments, at most one day),
    /// and `_quarantine/` (byte-sets a freeze or demotion resolved *on that
    /// bucket* — never a byte the fleet serves, never the winner, and always
    /// announced by a fence that does replicate).
    DeclaredLoss,
}

impl Class {
    /// A stable machine label for the class, for tests and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Class::TruthReplicated => "truth-replicated",
            Class::SingletonReplicated => "singleton-replicated",
            Class::DerivedPerBucket => "derived-per-bucket",
            Class::CoordinationPerBucket => "coordination-per-bucket",
            Class::ReplicatedRollup => "replicated-rollup",
            Class::DeclaredLoss => "declared-loss",
        }
    }
}

/// One registry row: a storage key prefix (or a full key, for the singleton
/// control files that share a `_advisories/` neighborhood with a derived one),
/// its replication class, and the reason.
pub struct PrefixClass {
    pub prefix: &'static str,
    pub class: Class,
    pub why: &'static str,
}

/// The single source of truth for storage-key classification. Every prefix
/// references the const that defines it, so the registry cannot drift from the
/// code that writes the key. Ordered longest-first is NOT required — [`classify`]
/// resolves by longest match — but keep related prefixes grouped for readers.
pub const MANIFEST: &[PrefixClass] = &[
    PrefixClass {
        prefix: crate::app::PACKAGES_PREFIX,
        class: Class::TruthReplicated,
        why: "package artifacts + sidecars — private truth, `sync --to` snapshots, AND proxy-cache fills — all replicated to every bucket; private/snapshot via pre-ack fan-out, cache via an async post-serve `_repl/` note, so any bucket serves the whole corpus",
    },
    PrefixClass {
        prefix: crate::app::SIMPLE_PREFIX,
        class: Class::DerivedPerBucket,
        why: "PEP 503 index: a rebuildable view of packages/, each bucket keeps its own",
    },
    PrefixClass {
        prefix: crate::advisories::FEED_KEY,
        class: Class::SingletonReplicated,
        why: "OSV malware feed snapshot: leader-authored, must arm every bucket on failover",
    },
    PrefixClass {
        prefix: crate::advisories::QUARANTINED_KEY,
        class: Class::SingletonReplicated,
        why: "PEP 792 quarantined-project set: a byte-gate input DERIVED from the per-package status sidecars (the audit sweep rebuilds it from truth), but every bucket needs it on failover, so it is written through like a control singleton rather than re-derived per bucket. Epoch-CAS'd on the selected bucket; peers get the same epoch-bearing body best-effort and no reader ever moves backwards",
    },
    PrefixClass {
        prefix: crate::advisories::REPORT_KEY,
        class: Class::DerivedPerBucket,
        why: "org audit report: re-materialized from the walk each sweep, per bucket",
    },
    PrefixClass {
        prefix: crate::app::DIRTY_PREFIX,
        class: Class::CoordinationPerBucket,
        why: "dirty-package worklist for the local index rebuild; bucket-local",
    },
    PrefixClass {
        prefix: crate::counters::DAY_PREFIX,
        class: Class::ReplicatedRollup,
        why: "download-counter day-rollups (frozen shard totals + summaries): leader-authored immutable truth, reseeded to every bucket so a failover keeps the /audit ranking and /stats history",
    },
    PrefixClass {
        prefix: crate::counters::SEG_PREFIX,
        class: Class::DeclaredLoss,
        why: "download-counter live tallies (current day's un-rolled-up segments): declared acceptable loss ≤ current day, by design, per DESIGN.md totality principle",
    },
    PrefixClass {
        prefix: crate::lease::LEASE_KEY,
        class: Class::CoordinationPerBucket,
        why: "per-bucket leader lease; a failover starts a fresh one, never copied",
    },
    PrefixClass {
        prefix: crate::replicate::QUARANTINE_PREFIX,
        class: Class::DeclaredLoss,
        why: "losing byte-sets of freezes and demotions resolved on this bucket — never a byte the fleet serves, never the winner; the fence that announces each (`.frozen` + tombstone, or `.mirror-quarantined`) does replicate",
    },
    PrefixClass {
        prefix: crate::replicate::REPL_PREFIX,
        class: Class::CoordinationPerBucket,
        why: "per-destination replication repair notes; consumed on heal, never copied",
    },
    PrefixClass {
        prefix: crate::storage::STAGING_PREFIX,
        class: Class::CoordinationPerBucket,
        why: "in-progress large-upload staging, promoted then cleaned; bucket-local",
    },
    PrefixClass {
        prefix: crate::worker::STATE_PREFIX,
        class: Class::DerivedPerBucket,
        why: "per-bucket derived state: fingerprint shards and the enforced-denylist stamp — views of config/truth, regenerable",
    },
    PrefixClass {
        prefix: crate::sync::CURSORS_KEY,
        class: Class::DerivedPerBucket,
        why: "sync relay cursors: a bucket-local resume hint, re-derivable from the mirror",
    },
    PrefixClass {
        prefix: crate::buckets::TOPOLOGY_STAMP_KEY,
        class: Class::CoordinationPerBucket,
        why: "per-bucket topology fence stamp; identity of the local bucket, never copied",
    },
    PrefixClass {
        prefix: crate::transparency::CHAIN_PREFIX,
        class: Class::SingletonReplicated,
        why: "tamper-evidence checkpoint chain: immutable leader-authored links written through to every bucket so a failover continues the chain, not a fresh genesis",
    },
];

/// The replication class of a storage key, resolved by longest matching prefix,
/// or `None` when no manifest entry claims it — the CI tripwire for an
/// unclassified new prefix.
pub fn classify(key: &str) -> Option<Class> {
    MANIFEST
        .iter()
        .filter(|entry| key.starts_with(entry.prefix))
        .max_by_key(|entry| entry.prefix.len())
        .map(|entry| entry.class)
}

/// One peer bucket a control singleton writes through to and reseeds onto — its
/// storage handle and identity (for log lines). Built by the caller from the
/// live health view; an eligible, non-selected bucket in a multi-bucket fleet.
pub struct ReplicaTarget<'a> {
    pub storage: &'a dyn Storage,
    pub name: &'a str,
}

/// Write a leader-authored control singleton to `primary` (authoritative — its
/// error propagates and its stored etag drives the local reload) and best-effort
/// to every peer in `replicas`. A peer failure is logged, not fatal: the
/// reseed-if-absent backstop heals a bucket that missed the write. In
/// single-bucket mode `replicas` is empty and this is one `put_bytes`.
pub async fn write_singleton(
    primary: &dyn Storage,
    replicas: &[ReplicaTarget<'_>],
    key: &str,
    bytes: Vec<u8>,
    content_type: Option<&str>,
) -> Result<()> {
    primary
        .put_bytes(key, bytes.clone(), content_type)
        .await
        .with_context(|| format!("writing control singleton {key}"))?;
    write_through(replicas, key, &bytes, content_type).await;
    Ok(())
}

/// Best-effort write-through of an already-authored singleton to peer buckets.
/// Used after the primary write, and standalone by the poll refresh which has
/// already persisted the primary copy.
pub async fn write_through(
    replicas: &[ReplicaTarget<'_>],
    key: &str,
    bytes: &[u8],
    content_type: Option<&str>,
) {
    for replica in replicas {
        if let Err(error) = replica
            .storage
            .put_bytes(key, bytes.to_vec(), content_type)
            .await
        {
            warn!(bucket = %replica.name, key, error = ?error, "control singleton write-through failed; reseed will heal");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn no_manifest_prefix_is_empty_or_duplicated() {
        let mut seen = HashSet::new();
        for entry in MANIFEST {
            assert!(!entry.prefix.is_empty(), "a manifest prefix is empty");
            assert!(!entry.why.is_empty(), "{} has no why", entry.prefix);
            assert!(
                seen.insert(entry.prefix),
                "duplicate manifest prefix {}",
                entry.prefix
            );
        }
    }

    #[test]
    fn no_prefix_ambiguously_overlaps_another() {
        // A entry may be a strict prefix of another only when they classify the
        // same — otherwise `classify` would resolve by length and a reader could
        // not tell which class a key gets. We forbid overlap entirely except the
        // deliberate `_advisories/` singleton-vs-derived split, which never
        // nests (three distinct full keys under one dir, none a prefix of another).
        for a in MANIFEST {
            for b in MANIFEST {
                if std::ptr::eq(a, b) {
                    continue;
                }
                assert!(
                    !a.prefix.starts_with(b.prefix),
                    "manifest prefix {} nests inside {}; classification is ambiguous",
                    a.prefix,
                    b.prefix
                );
            }
        }
    }

    #[test]
    fn every_class_is_represented() {
        for class in [
            Class::TruthReplicated,
            Class::SingletonReplicated,
            Class::DerivedPerBucket,
            Class::CoordinationPerBucket,
            Class::ReplicatedRollup,
            Class::DeclaredLoss,
        ] {
            assert!(
                MANIFEST.iter().any(|entry| entry.class == class),
                "class {} is undocumented in the manifest",
                class.as_str()
            );
        }
    }

    #[test]
    fn representative_keys_classify_to_their_class() {
        assert_eq!(
            classify("packages/requests/requests-2.0.0.tar.gz"),
            Some(Class::TruthReplicated)
        );
        assert_eq!(
            classify("simple/requests/index.html"),
            Some(Class::DerivedPerBucket)
        );
        // Counter day-rollups replicate; the current day's live segments do not.
        // A rollup names the bucket whose segments it summed, in the filename —
        // the path arity, and so this classification, is unchanged by that.
        assert_eq!(
            classify("_counters/day/downloads/2026-01-01/r@s3---east.json"),
            Some(Class::ReplicatedRollup)
        );
        assert_eq!(
            classify("_counters/day/downloads/2026-01-01/_summary@s3---east.json"),
            Some(Class::ReplicatedRollup)
        );
        assert_eq!(
            classify("_counters/seg/downloads/2026-01-01/r/inc-0.json"),
            Some(Class::DeclaredLoss)
        );
        // A demotion/freeze loser is preserved evidence, not a derived view:
        // nothing recomputes it, and it is declared lost on a bucket failure.
        assert_eq!(
            classify("_quarantine/requests/requests-2.0.0.tar.gz@0123456789ab"),
            Some(Class::DeclaredLoss)
        );
        assert_eq!(
            classify(crate::advisories::FEED_KEY),
            Some(Class::SingletonReplicated)
        );
        assert_eq!(
            classify(crate::advisories::QUARANTINED_KEY),
            Some(Class::SingletonReplicated)
        );
        // The derived report shares the `_advisories/` dir with the singletons but
        // classifies on its own exact key, not the singletons'.
        assert_eq!(
            classify(crate::advisories::REPORT_KEY),
            Some(Class::DerivedPerBucket)
        );
        assert_eq!(
            classify("_repl/s3---east/pkg/file!nonce"),
            Some(Class::CoordinationPerBucket)
        );
        assert_eq!(
            classify(crate::buckets::TOPOLOGY_STAMP_KEY),
            Some(Class::CoordinationPerBucket)
        );
        // An unclassified prefix is the CI tripwire.
        assert_eq!(classify("_brand_new_prefix/x"), None);
    }

    #[test]
    fn every_known_prefix_const_is_registered() {
        // Ties the manifest to the consts the rest of the crate writes: a prefix
        // that exists in code but not here (or vice-versa) fails.
        for key in [
            crate::app::PACKAGES_PREFIX,
            crate::app::SIMPLE_PREFIX,
            crate::app::DIRTY_PREFIX,
            crate::advisories::FEED_KEY,
            crate::advisories::QUARANTINED_KEY,
            crate::advisories::REPORT_KEY,
            crate::counters::DAY_PREFIX,
            crate::counters::SEG_PREFIX,
            crate::lease::LEASE_KEY,
            crate::replicate::QUARANTINE_PREFIX,
            crate::replicate::REPL_PREFIX,
            crate::storage::STAGING_PREFIX,
            crate::worker::STATE_PREFIX,
            crate::sync::CURSORS_KEY,
            crate::buckets::TOPOLOGY_STAMP_KEY,
            crate::transparency::CHAIN_PREFIX,
        ] {
            assert!(
                classify(key).is_some(),
                "storage prefix {key} is not classified in the layout manifest"
            );
        }
    }
}
