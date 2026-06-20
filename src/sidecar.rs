//! Metadata sidecars: `<filename>.meta.json` next to each artifact.
//!
//! The sidecar schema is part of the storage contract (DESIGN.md). Everything
//! is captured at write time so rebuilds never hash artifacts or infer names.

use serde::{Deserialize, Serialize};

pub const SIDECAR_SUFFIX: &str = ".meta.json";
pub const METADATA_SUFFIX: &str = ".metadata";
/// PEP 740 provenance object, relayed verbatim from upstream next to the
/// artifact. Like `.metadata`, it is a served companion, not truth we author.
pub const PROVENANCE_SUFFIX: &str = ".provenance";

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
    #[serde(
        rename = "requires-python",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_python: Option<String>,
    #[serde(default)]
    pub yanked: Yanked,
}

/// Storage key of the sidecar for an artifact key.
pub fn sidecar_key(artifact_key: &str) -> String {
    format!("{artifact_key}{SIDECAR_SUFFIX}")
}

/// Storage key of the PEP 658 metadata file for an artifact key.
pub fn metadata_key(artifact_key: &str) -> String {
    format!("{artifact_key}{METADATA_SUFFIX}")
}

/// Storage key of the PEP 740 provenance companion for an artifact key.
pub fn provenance_key(artifact_key: &str) -> String {
    format!("{artifact_key}{PROVENANCE_SUFFIX}")
}

/// True if `filename` (no directory part) is an artifact, not a sidecar or dotfile.
pub fn is_artifact(filename: &str) -> bool {
    !filename.is_empty()
        && !filename.starts_with('.')
        && !filename.ends_with(SIDECAR_SUFFIX)
        && !filename.ends_with(METADATA_SUFFIX)
        && !filename.ends_with(PROVENANCE_SUFFIX)
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
}
