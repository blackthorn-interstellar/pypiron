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
    /// Regenerable per-bucket views (indexes, audit report, counters, sync
    /// cursors, fingerprint state, frozen-conflict bodies). Never copied — each
    /// bucket rebuilds or re-derives its own from the truth it holds.
    DerivedPerBucket,
    /// Per-bucket coordination state (leases, `_repl/` notes, topology stamps,
    /// staging, dirty worklist). Bucket-local by design; replicating it would be
    /// wrong, not merely wasteful.
    CoordinationPerBucket,
}

impl Class {
    /// A stable machine label for the class, for tests and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Class::TruthReplicated => "truth-replicated",
            Class::SingletonReplicated => "singleton-replicated",
            Class::DerivedPerBucket => "derived-per-bucket",
            Class::CoordinationPerBucket => "coordination-per-bucket",
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
        why: "package artifacts + sidecars: the operator's truth, replicated to every bucket",
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
        why: "PEP 792 quarantined-project set: leader-authored byte-gate input, every bucket",
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
        prefix: crate::counters::PREFIX,
        class: Class::DerivedPerBucket,
        why: "download counters: per-node immutable segments aggregated locally (judgment call — cross-bucket aggregation pending)",
    },
    PrefixClass {
        prefix: crate::lease::LEASE_KEY,
        class: Class::CoordinationPerBucket,
        why: "per-bucket leader lease; a failover starts a fresh one, never copied",
    },
    PrefixClass {
        prefix: crate::replicate::QUARANTINE_PREFIX,
        class: Class::DerivedPerBucket,
        why: "byte-conflict losers preserved as moves; local to the bucket that resolved it",
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
        why: "per-bucket fingerprint shards: views of views, regenerable",
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
        class: Class::DerivedPerBucket,
        why: "per-bucket tamper-evidence checkpoint chain; today bucket-local (gap 9 revisits cross-bucket verify)",
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
            classify("_repl/1/pkg/file!nonce"),
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
            crate::counters::PREFIX,
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
