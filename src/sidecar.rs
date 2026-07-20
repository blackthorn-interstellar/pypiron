//! Metadata sidecars: `<filename>.meta.json` next to each artifact.
//!
//! The sidecar schema is part of the storage contract (dev/DESIGN.md). Everything
//! is captured at write time so rebuilds never hash artifacts or infer names.

use serde::{Deserialize, Serialize};

pub const SIDECAR_SUFFIX: &str = ".meta.json";
pub const METADATA_SUFFIX: &str = ".metadata";
/// PEP 740 provenance object, relayed verbatim from upstream next to the
/// artifact. Like `.metadata`, it is a served companion, not truth we author.
pub const PROVENANCE_SUFFIX: &str = ".provenance";
/// Delete marker: `<filename>.tombstone` next to where the artifact lived. A
/// deleted private filename may never be reused (dev/MULTIBUCKET.md §6.4), and
/// a crashed delete must converge to "gone" rather than resurrect the file.
pub const TOMBSTONE_SUFFIX: &str = ".tombstone";
/// Freeze marker: `<filename>.frozen` beside where the artifact lived. Two
/// buckets that committed different bytes under one filename (a split-brain,
/// dev/MULTIBUCKET.md §6.3) both move their body to `_quarantine/` and drop this
/// marker, which suppresses the filename from index rebuilds until a human
/// resolves it. Unlike a tombstone the name is not permanently barred — the
/// operator republishes a new version.
pub const FROZEN_SUFFIX: &str = ".frozen";
/// A mirror body preserved after its package became private. The live bytes are
/// deliberately left in place (and omitted from indexes) so cleanup never
/// opens an artifact-key ABA window for a concurrent private writer.
pub const MIRROR_QUARANTINED_SUFFIX: &str = ".mirror-quarantined";

/// PEP 592 yank state: `false`, `true`, or a reason string.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Yanked {
    Flag(bool),
    Reason(String),
}

impl Default for Yanked {
    fn default() -> Self {
        Yanked::Flag(false)
    }
}

impl Yanked {
    /// The canonical form the upload/yank endpoints store: a reason is trimmed,
    /// and an empty (or whitespace-only) reason collapses to a bare `true`.
    /// Normalizing inbound upstream yank to this form keeps `sync` reconcile
    /// idempotent — otherwise a `Reason("")` upstream value would never match
    /// the `Flag(true)` the server actually persists, and reconcile would
    /// re-yank every run.
    pub fn normalized(&self) -> Yanked {
        match self {
            Yanked::Reason(r) => {
                let t = r.trim();
                if t.is_empty() {
                    Yanked::Flag(true)
                } else if t.len() == r.len() {
                    Yanked::Reason(r.clone())
                } else {
                    Yanked::Reason(t.to_string())
                }
            }
            Yanked::Flag(b) => Yanked::Flag(*b),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sidecar {
    pub sha256: String,
    pub size: u64,
    pub version: String,
    #[serde(rename = "upload-time")]
    pub upload_time: String,
    /// Server-stamped receive time (epoch milliseconds) used only as the
    /// first-uploaded-wins tiebreak for the rare cross-partition byte conflict
    /// (dev/MULTIBUCKET.md §Conflict resolution). Absent on legacy sidecars
    /// and on mirror artifacts; a conflict with either side missing this field,
    /// or with the two within a small skew window, degrades to quarantine-both
    /// + alarm.
    #[serde(
        rename = "upload-epoch-ms",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub upload_epoch_ms: Option<u64>,
    #[serde(
        rename = "requires-python",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_python: Option<String>,
    #[serde(default)]
    pub yanked: Yanked,
    /// Per-artifact origin (`"private"` | `"mirror"`), captured at write time so
    /// the replicator can decide "replicate private truth only" from bucket
    /// state alone, not from history (dev/MULTIBUCKET.md §4, §6.2). Legacy and
    /// fabricated sidecars omit it; the worker backfills a missing value from
    /// the package-level `.origin` claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    /// Monotonic yank epoch, bumped on every yank/unyank flip (§6.5). The
    /// cross-bucket merge takes the max epoch — no wall clocks, because two
    /// buckets have two clocks and skew makes verdicts non-convergent. Absent
    /// means 0 (never yanked).
    #[serde(rename = "yank-epoch", default, skip_serializing_if = "is_zero_epoch")]
    pub yank_epoch: u64,
}

/// Serde predicate: keep the common (never-yanked) sidecar free of epoch noise
/// while still persisting any non-zero epoch the merge depends on.
fn is_zero_epoch(epoch: &u64) -> bool {
    *epoch == 0
}

/// Storage key of a companion object: the artifact key with its suffix appended.
fn companion_key(artifact_key: &str, suffix: &str) -> String {
    format!("{artifact_key}{suffix}")
}

/// Storage key of the sidecar for an artifact key.
pub fn sidecar_key(artifact_key: &str) -> String {
    companion_key(artifact_key, SIDECAR_SUFFIX)
}

/// Storage key of the PEP 658 metadata file for an artifact key.
pub fn metadata_key(artifact_key: &str) -> String {
    companion_key(artifact_key, METADATA_SUFFIX)
}

/// Storage key of the PEP 740 provenance companion for an artifact key.
pub fn provenance_key(artifact_key: &str) -> String {
    companion_key(artifact_key, PROVENANCE_SUFFIX)
}

/// Storage key of the delete tombstone for an artifact key.
pub fn tombstone_key(artifact_key: &str) -> String {
    companion_key(artifact_key, TOMBSTONE_SUFFIX)
}

/// Storage key of the freeze marker for an artifact key.
pub fn frozen_key(artifact_key: &str) -> String {
    companion_key(artifact_key, FROZEN_SUFFIX)
}

pub fn mirror_quarantined_key(artifact_key: &str) -> String {
    companion_key(artifact_key, MIRROR_QUARANTINED_SUFFIX)
}

/// True if `filename` (no directory part) is an artifact, not a sidecar,
/// tombstone, freeze marker, or dotfile.
pub fn is_artifact(filename: &str) -> bool {
    !filename.is_empty()
        && !filename.starts_with('.')
        && !filename.ends_with(SIDECAR_SUFFIX)
        && !filename.ends_with(METADATA_SUFFIX)
        && !filename.ends_with(PROVENANCE_SUFFIX)
        && !filename.ends_with(TOMBSTONE_SUFFIX)
        && !filename.ends_with(FROZEN_SUFFIX)
        && !filename.ends_with(MIRROR_QUARANTINED_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_filter_excludes_sidecars_and_dotfiles() {
        assert!(is_artifact("six-1.16.0-py2.py3-none-any.whl"));
        assert!(!is_artifact("six-1.16.0-py2.py3-none-any.whl.meta.json"));
        assert!(!is_artifact("six-1.16.0-py2.py3-none-any.whl.metadata"));
        assert!(!is_artifact("six-1.16.0-py2.py3-none-any.whl.provenance"));
        assert!(!is_artifact("six-1.16.0-py2.py3-none-any.whl.tombstone"));
        assert!(!is_artifact("six-1.16.0-py2.py3-none-any.whl.frozen"));
        assert!(!is_artifact(
            "six-1.16.0-py2.py3-none-any.whl.mirror-quarantined"
        ));
        assert!(!is_artifact(".origin"));
        assert!(!is_artifact(".project-status.json"));
        assert!(!is_artifact(""));
    }

    #[test]
    fn yanked_normalized_matches_server_storage() {
        // Trim, and collapse an empty/whitespace reason to a bare flag — the
        // same rule the upload/yank endpoints apply, so sync reconcile is
        // idempotent against a sloppy upstream reason.
        assert_eq!(Yanked::Reason("".into()).normalized(), Yanked::Flag(true));
        assert_eq!(
            Yanked::Reason("   ".into()).normalized(),
            Yanked::Flag(true)
        );
        assert_eq!(
            Yanked::Reason("broken ".into()).normalized(),
            Yanked::Reason("broken".into())
        );
        assert_eq!(
            Yanked::Reason("broken".into()).normalized(),
            Yanked::Reason("broken".into())
        );
        assert_eq!(Yanked::Flag(false).normalized(), Yanked::Flag(false));
        assert_eq!(Yanked::Flag(true).normalized(), Yanked::Flag(true));
    }

    #[test]
    fn sidecar_schema_round_trips() {
        let json = r#"{
            "sha256": "abc",
            "size": 123,
            "version": "1.2.3",
            "upload-time": "2026-06-11T00:00:00Z",
            "requires-python": ">=3.9",
            "yanked": false
        }"#;
        let sc: Sidecar = serde_json::from_str(json).unwrap();
        assert_eq!(sc.sha256, "abc");
        assert_eq!(sc.requires_python.as_deref(), Some(">=3.9"));
        assert_eq!(sc.yanked, Yanked::Flag(false));

        let reasoned: Sidecar = serde_json::from_str(
            r#"{"sha256":"a","size":1,"version":"1","upload-time":"t","yanked":"broken"}"#,
        )
        .unwrap();
        assert_eq!(reasoned.yanked, Yanked::Reason("broken".into()));
        let out = serde_json::to_string(&reasoned).unwrap();
        assert!(out.contains(r#""yanked":"broken""#));
    }

    #[test]
    fn origin_and_yank_epoch_default_for_legacy_sidecars() {
        // A pre-migration sidecar carries neither field: both must serde-default
        // (origin None → "backfill from .origin", yank_epoch 0 → never yanked),
        // or every legacy sidecar would fail to parse.
        let legacy: Sidecar =
            serde_json::from_str(r#"{"sha256":"a","size":1,"version":"1","upload-time":"t"}"#)
                .unwrap();
        assert_eq!(legacy.origin, None);
        assert_eq!(legacy.yank_epoch, 0);
        // The common case stays free of epoch/origin noise on the wire.
        let out = serde_json::to_string(&legacy).unwrap();
        assert!(!out.contains("yank-epoch"), "zero epoch must not serialize");
        assert!(!out.contains("origin"), "absent origin must not serialize");

        // A migrated sidecar round-trips both fields.
        let migrated: Sidecar = serde_json::from_str(
            r#"{"sha256":"a","size":1,"version":"1","upload-time":"t",
                "origin":"private","yank-epoch":3}"#,
        )
        .unwrap();
        assert_eq!(migrated.origin.as_deref(), Some("private"));
        assert_eq!(migrated.yank_epoch, 3);
        let out = serde_json::to_string(&migrated).unwrap();
        assert!(out.contains(r#""origin":"private""#));
        assert!(out.contains(r#""yank-epoch":3"#));
    }

    #[test]
    fn upload_epoch_ms_defaults_for_legacy_sidecars_and_round_trips() {
        let legacy: Sidecar =
            serde_json::from_str(r#"{"sha256":"a","size":1,"version":"1","upload-time":"t"}"#)
                .unwrap();
        assert_eq!(legacy.upload_epoch_ms, None);
        let out = serde_json::to_string(&legacy).unwrap();
        assert!(
            !out.contains("upload-epoch-ms"),
            "absent upload epoch must not serialize"
        );

        let stamped: Sidecar = serde_json::from_str(
            r#"{"sha256":"a","size":1,"version":"1","upload-time":"t","upload-epoch-ms":1234}"#,
        )
        .unwrap();
        assert_eq!(stamped.upload_epoch_ms, Some(1234));
        let out = serde_json::to_string(&stamped).unwrap();
        assert!(out.contains(r#""upload-epoch-ms":1234"#));
    }
}
