//! Per-project status markers (PEP 792, Simple API 1.4): `active`, `archived`,
//! `quarantined`, `deprecated`. Stored as a per-project sidecar
//! `packages/<pkg>/.project-status.json`; an absent file means `active`.
//!
//! We relay upstream status verbatim through sync and the proxy — like PEP 740
//! provenance, it is metadata we carry, not state we author.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::storage::{is_not_found, Storage};
use crate::PACKAGES_PREFIX;

/// A PEP 792 project status marker. Unknown values are rejected at parse time
/// (no `#[serde(other)]`): a corrupt or typo'd marker must never silently read
/// back as `active` and un-freeze a project (see [`read_status`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProjectStatus {
    Active,
    Archived,
    Quarantined,
    Deprecated,
}

impl ProjectStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ProjectStatus::Active => "active",
            ProjectStatus::Archived => "archived",
            ProjectStatus::Quarantined => "quarantined",
            ProjectStatus::Deprecated => "deprecated",
        }
    }

    /// `active` is the default; per PEP 792 the marker MAY be omitted for it,
    /// and we do — an active project renders byte-identically to no marker.
    pub fn is_active(self) -> bool {
        matches!(self, ProjectStatus::Active)
    }

    /// PEP 792: a quarantined project MUST NOT offer any distribution for
    /// download, so its index is rendered with no file links.
    pub fn blocks_downloads(self) -> bool {
        matches!(self, ProjectStatus::Quarantined)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatusDoc {
    pub status: ProjectStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Default for ProjectStatusDoc {
    fn default() -> Self {
        Self {
            status: ProjectStatus::Active,
            reason: None,
        }
    }
}

pub fn status_key(pkg: &str) -> String {
    format!("{PACKAGES_PREFIX}{pkg}/.project-status.json")
}

/// The package's status, defaulting to `active` when no marker file exists.
/// Both a storage error AND a corrupt/unknown-marker body propagate as `Err` —
/// never swallowed to `active`, or a quarantine could silently un-enforce
/// itself on a flaky read (fail-closed, like [`crate::origin::read_origin`]).
pub async fn read_status(storage: &dyn Storage, pkg: &str) -> Result<ProjectStatusDoc> {
    match storage.get_bytes(&status_key(pkg)).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if is_not_found(&e) => Ok(ProjectStatusDoc::default()),
        Err(e) => Err(e),
    }
}

/// Write the marker file (crash-safe tmp+rename via `put_bytes`). Last write
/// wins — status is a re-settable marker, not a one-shot claim like `.origin`.
pub async fn write_status(storage: &dyn Storage, pkg: &str, doc: &ProjectStatusDoc) -> Result<()> {
    storage
        .put_bytes(
            &status_key(pkg),
            serde_json::to_vec(doc)?,
            Some("application/json"),
        )
        .await
}

/// Remove the marker file, reverting the project to the default `active`.
pub async fn clear_status(storage: &dyn Storage, pkg: &str) -> Result<()> {
    storage.delete_keys(&[status_key(pkg)]).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_round_trips_with_and_without_reason() {
        let with = ProjectStatusDoc {
            status: ProjectStatus::Archived,
            reason: Some("moved to foo".into()),
        };
        let json = serde_json::to_string(&with).unwrap();
        assert_eq!(json, r#"{"status":"archived","reason":"moved to foo"}"#);
        assert_eq!(
            serde_json::from_str::<ProjectStatusDoc>(&json).unwrap(),
            with
        );

        // reason is omitted when absent.
        let bare = ProjectStatusDoc {
            status: ProjectStatus::Deprecated,
            reason: None,
        };
        assert_eq!(
            serde_json::to_string(&bare).unwrap(),
            r#"{"status":"deprecated"}"#
        );
    }

    #[test]
    fn corrupt_body_errors_rather_than_defaulting_to_active() {
        assert!(serde_json::from_slice::<ProjectStatusDoc>(b"{not json").is_err());
    }

    #[test]
    fn unknown_marker_errors_rather_than_defaulting_to_active() {
        // The single most important property: a typo'd/foreign marker must NOT
        // deserialize to active and un-quarantine a project.
        assert!(serde_json::from_str::<ProjectStatusDoc>(r#"{"status":"frozen"}"#).is_err());
    }
}
