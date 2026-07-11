//! Per-project status markers (PEP 792, Simple API 1.4): `active`, `archived`,
//! `quarantined`, `deprecated`. Stored as a per-project sidecar
//! `packages/<pkg>/.project-status.json`; an absent file means `active` at
//! epoch zero. Once a status has changed, even an `active` clear remains as a
//! marker: the monotone epoch is what lets writable buckets converge without
//! resurrecting an older quarantine after a partition.
//!
//! We relay upstream status verbatim through sync and the proxy — like PEP 740
//! provenance, it is metadata we carry, not state we author.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::storage::Storage;
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

/// Stored envelope. The PEP 792 fields remain at the top level; the namespaced
/// epoch is pypiron's clock-free merge token and is ignored by ordinary Simple
/// API consumers. Legacy marker bodies deserialize at epoch zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredProjectStatus {
    #[serde(flatten)]
    doc: ProjectStatusDoc,
    #[serde(rename = "pypiron-epoch", default, skip_serializing_if = "is_zero")]
    epoch: u64,
    /// Which package-origin world authored this event. Old markers omit it;
    /// demotion keeps exact-ETag fallback semantics for those legacy bodies.
    #[serde(
        rename = "pypiron-origin",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    origin: Option<StatusOrigin>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum StatusOrigin {
    Private,
    Mirror,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// A status observation suitable for a conditional merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedProjectStatus {
    pub doc: ProjectStatusDoc,
    pub epoch: u64,
    pub(crate) origin: Option<StatusOrigin>,
    pub etag: String,
}

/// Deterministic, symmetric status merge result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusWinner {
    InSync,
    Left,
    Right,
}

/// The side changed by a successful pairwise reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusConvergence {
    InSync,
    UpdatedLeft,
    UpdatedRight,
}

const STATUS_CAS_ATTEMPTS: usize = 8;

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
        Ok(bytes) => Ok(serde_json::from_slice::<StoredProjectStatus>(&bytes)?.doc),
        Err(error) if crate::storage::is_not_found(&error) => Ok(ProjectStatusDoc::default()),
        Err(error) => Err(error),
    }
}

/// Legacy single-bucket write: one unconditional PUT of the plain PEP 792
/// document. Multi-bucket mode uses [`advance_status`] instead because it needs
/// the epoch/origin envelope for deterministic reconciliation.
pub async fn write_status(storage: &dyn Storage, pkg: &str, doc: &ProjectStatusDoc) -> Result<()> {
    storage
        .put_bytes(
            &status_key(pkg),
            serde_json::to_vec(doc)?,
            Some("application/json"),
        )
        .await
}

/// Legacy single-bucket clear: absence is the active default and preserves the
/// pre-multi-bucket request count and storage layout.
pub async fn clear_status(storage: &dyn Storage, pkg: &str) -> Result<()> {
    storage.delete_keys(&[status_key(pkg)]).await
}

/// Read the stored status plus its logical epoch and conditional-write token.
/// Missing means active at epoch zero; corrupt bodies and storage failures are
/// errors, never an implicit un-quarantine.
pub async fn read_status_versioned(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<VersionedProjectStatus>> {
    match storage.get_with_etag(&status_key(pkg)).await? {
        Some((bytes, etag)) => {
            let stored: StoredProjectStatus = serde_json::from_slice(&bytes)?;
            Ok(Some(VersionedProjectStatus {
                doc: stored.doc,
                epoch: stored.epoch,
                origin: stored.origin,
                etag,
            }))
        }
        None => Ok(None),
    }
}

/// Encode one stored status event. Kept crate-visible for the cross-bucket
/// reconciler, which conditionally adopts the deterministic winner.
pub(crate) fn encode_status(
    doc: &ProjectStatusDoc,
    epoch: u64,
    origin: Option<StatusOrigin>,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&StoredProjectStatus {
        doc: doc.clone(),
        epoch,
        origin,
    })?)
}

fn restriction_rank(status: ProjectStatus) -> u8 {
    match status {
        ProjectStatus::Active => 0,
        ProjectStatus::Deprecated => 1,
        ProjectStatus::Archived => 2,
        ProjectStatus::Quarantined => 3,
    }
}

fn status_digest(status: &VersionedProjectStatus) -> [u8; 32] {
    let bytes = encode_status(&status.doc, status.epoch, status.origin).unwrap_or_default();
    Sha256::digest(bytes).into()
}

/// Merge project status without clocks. Private-origin events outrank tagged
/// mirror events; within one origin world the greater epoch wins. A split can
/// produce two different events at the same epoch; the more restrictive state
/// wins, then the lexicographically smaller canonical-record digest settles
/// differing reasons. No bucket identity participates, so all pair orders agree.
pub fn merge_status(
    left: Option<&VersionedProjectStatus>,
    right: Option<&VersionedProjectStatus>,
) -> StatusWinner {
    match (left, right) {
        (None, None) => StatusWinner::InSync,
        (Some(left), None) => {
            if left.epoch == 0 && left.doc == ProjectStatusDoc::default() {
                StatusWinner::InSync
            } else {
                StatusWinner::Left
            }
        }
        (None, Some(right)) => {
            if right.epoch == 0 && right.doc == ProjectStatusDoc::default() {
                StatusWinner::InSync
            } else {
                StatusWinner::Right
            }
        }
        (Some(left), Some(right))
            if left.origin == Some(StatusOrigin::Private)
                && right.origin == Some(StatusOrigin::Mirror) =>
        {
            StatusWinner::Left
        }
        (Some(left), Some(right))
            if left.origin == Some(StatusOrigin::Mirror)
                && right.origin == Some(StatusOrigin::Private) =>
        {
            StatusWinner::Right
        }
        (Some(left), Some(right)) if left.epoch > right.epoch => StatusWinner::Left,
        (Some(left), Some(right)) if right.epoch > left.epoch => StatusWinner::Right,
        (Some(left), Some(right)) if left.doc == right.doc && left.origin == right.origin => {
            StatusWinner::InSync
        }
        (Some(left), Some(right)) if left.doc == right.doc => match (left.origin, right.origin) {
            (Some(StatusOrigin::Private), _) => StatusWinner::Left,
            (_, Some(StatusOrigin::Private)) => StatusWinner::Right,
            (Some(StatusOrigin::Mirror), None) => StatusWinner::Left,
            (None, Some(StatusOrigin::Mirror)) => StatusWinner::Right,
            _ => StatusWinner::InSync,
        },
        (Some(left), Some(right)) => {
            let left_rank = restriction_rank(left.doc.status);
            let right_rank = restriction_rank(right.doc.status);
            if left_rank > right_rank {
                StatusWinner::Left
            } else if right_rank > left_rank {
                StatusWinner::Right
            } else if status_digest(left) <= status_digest(right) {
                StatusWinner::Left
            } else {
                StatusWinner::Right
            }
        }
    }
}

/// Conditionally install an exact status event. `expected_etag = None` means
/// create-only; otherwise replace-only-if-unchanged. Returns false on a race so
/// the caller can re-read and re-run the merge algebra.
pub(crate) async fn put_status_if_version(
    storage: &dyn Storage,
    pkg: &str,
    expected_etag: Option<&str>,
    doc: &ProjectStatusDoc,
    epoch: u64,
    origin: Option<StatusOrigin>,
) -> Result<bool> {
    let key = status_key(pkg);
    let bytes = encode_status(doc, epoch, origin)?;
    match expected_etag {
        Some(etag) => Ok(storage.put_if_match(&key, etag, bytes).await?.is_some()),
        None => Ok(storage.put_if_none_match(&key, bytes).await?.is_some()),
    }
}

/// Record a local status change by CAS-bumping its logical epoch. Clears write
/// an explicit `active` event instead of deleting the marker, so an older
/// quarantine on another bucket cannot return during reconciliation.
pub async fn advance_status(
    storage: &dyn Storage,
    pkg: &str,
    doc: &ProjectStatusDoc,
    origin: Option<StatusOrigin>,
) -> Result<u64> {
    for _ in 0..STATUS_CAS_ATTEMPTS {
        let current = read_status_versioned(storage, pkg).await?;
        let epoch = current
            .as_ref()
            .map_or(1, |status| status.epoch.saturating_add(1));
        if put_status_if_version(
            storage,
            pkg,
            current.as_ref().map(|status| status.etag.as_str()),
            doc,
            epoch,
            origin,
        )
        .await?
        {
            return Ok(epoch);
        }
    }
    Err(anyhow!(
        "could not update project status for '{pkg}' after {STATUS_CAS_ATTEMPTS} attempts"
    ))
}

/// Converge one package's status across two buckets with conditional writes.
///
/// The winner is recomputed after every lost CAS race. This keeps the merge
/// symmetric while ensuring that reconciliation never blindly overwrites a
/// newer local status event.
pub(crate) async fn reconcile_status_pair(
    left_storage: &dyn Storage,
    right_storage: &dyn Storage,
    pkg: &str,
) -> Result<StatusConvergence> {
    for _ in 0..STATUS_CAS_ATTEMPTS {
        let left = read_status_versioned(left_storage, pkg).await?;
        let right = read_status_versioned(right_storage, pkg).await?;
        match merge_status(left.as_ref(), right.as_ref()) {
            StatusWinner::InSync => return Ok(StatusConvergence::InSync),
            StatusWinner::Left => {
                let winner = left
                    .as_ref()
                    .ok_or_else(|| anyhow!("status merge selected an absent left winner"))?;
                if put_status_if_version(
                    right_storage,
                    pkg,
                    right.as_ref().map(|status| status.etag.as_str()),
                    &winner.doc,
                    winner.epoch,
                    winner.origin,
                )
                .await?
                {
                    return Ok(StatusConvergence::UpdatedRight);
                }
            }
            StatusWinner::Right => {
                let winner = right
                    .as_ref()
                    .ok_or_else(|| anyhow!("status merge selected an absent right winner"))?;
                if put_status_if_version(
                    left_storage,
                    pkg,
                    left.as_ref().map(|status| status.etag.as_str()),
                    &winner.doc,
                    winner.epoch,
                    winner.origin,
                )
                .await?
                {
                    return Ok(StatusConvergence::UpdatedLeft);
                }
            }
        }
    }
    Err(anyhow!(
        "could not reconcile project status for '{pkg}' after {STATUS_CAS_ATTEMPTS} attempts"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;

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
    fn stored_status_keeps_legacy_shape_at_epoch_zero() {
        let doc = ProjectStatusDoc {
            status: ProjectStatus::Quarantined,
            reason: Some("bad release".into()),
        };
        assert_eq!(
            String::from_utf8(encode_status(&doc, 0, None).unwrap()).unwrap(),
            r#"{"status":"quarantined","reason":"bad release"}"#
        );
        assert_eq!(
            String::from_utf8(encode_status(&doc, 7, None).unwrap()).unwrap(),
            r#"{"status":"quarantined","reason":"bad release","pypiron-epoch":7}"#
        );
        assert_eq!(
            String::from_utf8(encode_status(&doc, 7, Some(StatusOrigin::Private)).unwrap())
                .unwrap(),
            r#"{"status":"quarantined","reason":"bad release","pypiron-epoch":7,"pypiron-origin":"private"}"#
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

    #[tokio::test]
    async fn active_clear_is_a_new_event_not_an_absent_marker() {
        let storage = InMemStorage::default();
        let quarantined = ProjectStatusDoc {
            status: ProjectStatus::Quarantined,
            reason: Some("investigating".into()),
        };
        assert_eq!(
            advance_status(&storage, "pkg", &quarantined, None)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            advance_status(&storage, "pkg", &ProjectStatusDoc::default(), None)
                .await
                .unwrap(),
            2
        );

        let stored = read_status_versioned(&storage, "pkg")
            .await
            .unwrap()
            .expect("active clear remains stored");
        assert_eq!(stored.doc, ProjectStatusDoc::default());
        assert_eq!(stored.epoch, 2);
    }

    #[tokio::test]
    async fn conditional_adoption_loses_to_a_concurrent_change() {
        let storage = InMemStorage::default();
        let first = ProjectStatusDoc {
            status: ProjectStatus::Archived,
            reason: None,
        };
        advance_status(&storage, "pkg", &first, None).await.unwrap();
        let observed = read_status_versioned(&storage, "pkg")
            .await
            .unwrap()
            .unwrap();
        advance_status(&storage, "pkg", &ProjectStatusDoc::default(), None)
            .await
            .unwrap();

        assert!(!put_status_if_version(
            &storage,
            "pkg",
            Some(&observed.etag),
            &first,
            observed.epoch + 1,
            None,
        )
        .await
        .unwrap());
        assert_eq!(
            read_status_versioned(&storage, "pkg")
                .await
                .unwrap()
                .unwrap()
                .doc,
            ProjectStatusDoc::default()
        );
    }

    #[tokio::test]
    async fn pair_reconciliation_copies_newer_events_and_active_clears() {
        let left = InMemStorage::default();
        let right = InMemStorage::default();
        let quarantined = ProjectStatusDoc {
            status: ProjectStatus::Quarantined,
            reason: Some("investigating".into()),
        };
        advance_status(&left, "pkg", &quarantined, None)
            .await
            .unwrap();

        assert!(matches!(
            reconcile_status_pair(&left, &right, "pkg").await.unwrap(),
            StatusConvergence::UpdatedRight
        ));
        assert_eq!(read_status(&right, "pkg").await.unwrap(), quarantined);

        advance_status(&right, "pkg", &ProjectStatusDoc::default(), None)
            .await
            .unwrap();
        assert!(matches!(
            reconcile_status_pair(&left, &right, "pkg").await.unwrap(),
            StatusConvergence::UpdatedLeft
        ));
        let stored = read_status_versioned(&left, "pkg").await.unwrap().unwrap();
        assert_eq!(stored.doc, ProjectStatusDoc::default());
        assert_eq!(stored.epoch, 2);
    }

    #[tokio::test]
    async fn pair_reconciliation_uses_restrictive_same_epoch_tie_break() {
        let left = InMemStorage::default();
        let right = InMemStorage::default();
        let active = ProjectStatusDoc::default();
        let quarantined = ProjectStatusDoc {
            status: ProjectStatus::Quarantined,
            reason: None,
        };
        assert!(put_status_if_version(&left, "pkg", None, &active, 4, None)
            .await
            .unwrap());
        assert!(
            put_status_if_version(&right, "pkg", None, &quarantined, 4, None)
                .await
                .unwrap()
        );

        assert!(matches!(
            reconcile_status_pair(&left, &right, "pkg").await.unwrap(),
            StatusConvergence::UpdatedLeft
        ));
        assert_eq!(read_status(&left, "pkg").await.unwrap(), quarantined);
    }

    fn versioned(
        status: ProjectStatus,
        reason: Option<&str>,
        epoch: u64,
    ) -> VersionedProjectStatus {
        VersionedProjectStatus {
            doc: ProjectStatusDoc {
                status,
                reason: reason.map(str::to_string),
            },
            epoch,
            origin: None,
            etag: format!("etag-{epoch}"),
        }
    }

    #[test]
    fn status_merge_is_epoch_first_and_fail_closed_on_a_tie() {
        let old_quarantine = versioned(ProjectStatus::Quarantined, None, 2);
        let new_active = versioned(ProjectStatus::Active, None, 3);
        assert_eq!(
            merge_status(Some(&old_quarantine), Some(&new_active)),
            StatusWinner::Right
        );

        let active = versioned(ProjectStatus::Active, None, 4);
        let quarantine = versioned(ProjectStatus::Quarantined, None, 4);
        assert_eq!(
            merge_status(Some(&active), Some(&quarantine)),
            StatusWinner::Right
        );
        assert_eq!(
            merge_status(Some(&quarantine), Some(&active)),
            StatusWinner::Left
        );

        let mut private = versioned(ProjectStatus::Active, None, 1);
        private.origin = Some(StatusOrigin::Private);
        let mut mirror = versioned(ProjectStatus::Quarantined, None, 99);
        mirror.origin = Some(StatusOrigin::Mirror);
        assert_eq!(
            merge_status(Some(&private), Some(&mirror)),
            StatusWinner::Left,
            "mirror epochs belong to a superseded origin world"
        );
    }

    #[test]
    fn status_merge_reason_tie_break_is_symmetric() {
        let a = versioned(ProjectStatus::Archived, Some("alpha"), 5);
        let b = versioned(ProjectStatus::Archived, Some("beta"), 5);
        let ab = merge_status(Some(&a), Some(&b));
        let ba = merge_status(Some(&b), Some(&a));
        assert!(matches!(ab, StatusWinner::Left | StatusWinner::Right));
        assert_eq!(
            (ab, ba),
            match ab {
                StatusWinner::Left => (StatusWinner::Left, StatusWinner::Right),
                StatusWinner::Right => (StatusWinner::Right, StatusWinner::Left),
                StatusWinner::InSync => unreachable!(),
            }
        );
    }
}
