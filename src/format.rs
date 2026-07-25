//! Storage-format read gate. A tree's on-disk format is a single integer; a
//! fresh tree is format 1 and carries no stamp. A future format bump ships the
//! new number in `_format/stamp.json`, and this gate makes an older binary
//! refuse to start against a newer tree instead of writing format-N-1 shapes
//! into it. Read-only and startup-only: nothing here ever writes the stamp (the
//! stamp-creation and CAS-bump tooling arrives with format 2), and the gate is
//! wired at every write-open boundary — serve startup and the headless
//! maintenance commands — never on a read-only path.
//!
//! See dev/DESIGN.md for the bump policy the format number obeys.

use anyhow::{bail, Context as _, Result};
use tracing::warn;

use crate::buckets::{bounded_topology_io, BucketHandle};
use crate::storage::{is_not_found, Storage};

/// Storage key of the format stamp. Absent means format 1 — a permanent rule,
/// never written while the tree is at format 1.
pub const FORMAT_STAMP_KEY: &str = "_format/stamp.json";

/// The storage format this binary writes. Bumped only by a rename, move, or
/// semantic reinterpretation of the layout — never by an additive field with a
/// safe serde default (see dev/DESIGN.md).
pub const CURRENT_FORMAT: u32 = 1;

/// The stored format stamp. Deliberately tolerant so a future format-N stamp
/// still parses far enough for this binary to compare `format` and refuse:
/// unknown fields are ignored (no `deny_unknown_fields`) and `writer` — the
/// human-readable identity of the pypiron that stamped it — is optional.
#[derive(Debug, serde::Deserialize)]
pub struct FormatStamp {
    pub format: u32,
    #[serde(default)]
    pub writer: Option<String>,
}

/// Fail closed before serving or writing when any reachable bucket is stamped
/// with a storage format newer than this binary writes, or carries a corrupt
/// stamp. An absent stamp is the valid format-1 state and clears the bucket. A
/// GET the classifier calls an availability failure skips that bucket with a
/// warning, so a multi-bucket fleet still starts on its reachable buckets during
/// the outage multi-bucket exists for; every other GET error fails closed.
///
/// The classifier is applied verbatim — the one-second control-I/O bound is NOT
/// folded in here. Serve wraps its classifier with
/// [`topology_error_is_availability`](crate::buckets::topology_error_is_availability)
/// so a bucket too slow to answer that bound is skipped like any other outage.
/// `build_for_write` (rebuild-index) passes a classifier that never skips, so
/// ANY unverifiable bucket — a real GET error or a hang past that bound —
/// refuses rather than being written blind; `build_all_for_write` folds the
/// bound in and skips like serve, because its ops defer an unreachable
/// member's writes under the same bound (see its doc in src/storage.rs).
///
/// If every bucket was skipped and none cleared, refuse: a startup that verified
/// no bucket must not bind, mirroring the topology check's no-reachable-bucket
/// bail (src/buckets.rs).
pub async fn verify_format<F>(handles: &[BucketHandle], is_availability: F) -> Result<()>
where
    F: Fn(usize, &anyhow::Error) -> bool,
{
    let mut cleared = 0usize;
    for (index, handle) in handles.iter().enumerate() {
        let bytes = match read_stamp(handle.storage.as_ref()).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                cleared += 1;
                continue;
            }
            Err(error) if is_availability(index, &error) => {
                warn!(bucket=%handle.name, error=%error, "bucket unavailable during storage-format check; skipping");
                continue;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("read storage format stamp from bucket '{}'", handle.name)
                })
            }
        };
        // A stamp is written atomically on every backend, so a byte that will
        // not parse is foreign interference, never a torn write we may recreate.
        let stamp: FormatStamp = serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "corrupt storage format stamp {FORMAT_STAMP_KEY} in bucket '{}': remove the \
                 object and restart; an absent stamp is valid and means format {CURRENT_FORMAT}",
                handle.name
            )
        })?;
        if stamp.format > CURRENT_FORMAT {
            let by = match &stamp.writer {
                Some(writer) => format!(" (stamped by {writer})"),
                None => String::new(),
            };
            bail!(
                "storage format {}{by} is newer than supported format {CURRENT_FORMAT} in bucket \
                 '{}': deploy a pypiron that supports format {}; do not roll back past a format bump",
                stamp.format,
                handle.name,
                stamp.format
            );
        }
        cleared += 1;
    }
    // A startup that skipped every bucket verified no format at all; single-bucket
    // serve would then bind blind (topology no-ops there). Refuse, mirroring
    // verify_topology_with's no-reachable-bucket bail. Headless writers skip
    // nothing, so this never fires for them.
    if !handles.is_empty() && cleared == 0 {
        bail!("cannot verify storage format: no configured bucket is reachable at startup");
    }
    Ok(())
}

/// One bounded GET of the format stamp: `Some(bytes)` when present, `None` when
/// absent (format 1). Every backend, disk included, reports a missing object as
/// [`is_not_found`], which is the absent case — not an error to propagate.
async fn read_stamp(storage: &dyn Storage) -> Result<Option<Vec<u8>>> {
    match bounded_topology_io(storage.get_bytes(FORMAT_STAMP_KEY)).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;
    use std::sync::Arc;

    fn handle(storage: &Arc<InMemStorage>) -> Vec<BucketHandle> {
        vec![BucketHandle {
            storage: storage.clone(),
            name: "iron".to_string(),
        }]
    }

    async fn seed(storage: &InMemStorage, body: &[u8]) {
        storage
            .put_bytes(FORMAT_STAMP_KEY, body.to_vec(), None)
            .await
            .unwrap();
    }

    async fn gate(handles: &[BucketHandle]) -> Result<()> {
        // The strict classifier rebuild-index uses: any bucket that cannot be
        // verified — a GET error or a control-I/O timeout — refuses.
        verify_format(handles, |_, _| false).await
    }

    #[test]
    fn stamp_parses_current_format() {
        let stamp: FormatStamp = serde_json::from_slice(br#"{"format":1}"#).unwrap();
        assert_eq!(stamp.format, 1);
        assert_eq!(stamp.writer, None);
    }

    #[test]
    fn stamp_ignores_unknown_fields_and_reads_writer() {
        let stamp: FormatStamp = serde_json::from_slice(
            br#"{"format":2,"writer":"pypiron 0.5.0 (abc1234)","extra":true}"#,
        )
        .unwrap();
        assert_eq!(stamp.format, 2);
        assert_eq!(stamp.writer.as_deref(), Some("pypiron 0.5.0 (abc1234)"));
    }

    #[tokio::test]
    async fn absent_stamp_is_ok() {
        let storage = Arc::new(InMemStorage::default());
        gate(&handle(&storage)).await.unwrap();
    }

    #[tokio::test]
    async fn format_one_is_ok() {
        let storage = Arc::new(InMemStorage::default());
        seed(storage.as_ref(), br#"{"format":1}"#).await;
        gate(&handle(&storage)).await.unwrap();
    }

    #[tokio::test]
    async fn unknown_field_in_format_one_is_ok() {
        let storage = Arc::new(InMemStorage::default());
        seed(storage.as_ref(), br#"{"format":1,"future":"ignored"}"#).await;
        gate(&handle(&storage)).await.unwrap();
    }

    #[tokio::test]
    async fn missing_writer_is_ok() {
        let storage = Arc::new(InMemStorage::default());
        seed(storage.as_ref(), br#"{"format":1}"#).await;
        gate(&handle(&storage)).await.unwrap();
    }

    #[tokio::test]
    async fn newer_format_names_both_numbers_and_writer() {
        let storage = Arc::new(InMemStorage::default());
        seed(
            storage.as_ref(),
            br#"{"format":2,"writer":"pypiron 0.5.0 (abc1234)"}"#,
        )
        .await;
        let error = gate(&handle(&storage)).await.unwrap_err().to_string();
        assert!(error.contains("storage format 2"), "{error}");
        assert!(error.contains("supported format 1"), "{error}");
        assert!(error.contains("pypiron 0.5.0 (abc1234)"), "{error}");
    }

    #[tokio::test]
    async fn corrupt_stamp_names_the_key_and_recovery() {
        let storage = Arc::new(InMemStorage::default());
        seed(storage.as_ref(), b"not json at all").await;
        let error = format!("{:#}", gate(&handle(&storage)).await.unwrap_err());
        assert!(error.contains(FORMAT_STAMP_KEY), "{error}");
        assert!(error.contains("remove the object"), "{error}");
    }

    #[tokio::test]
    async fn get_failure_under_strict_classifier_refuses_naming_bucket() {
        // A headless writer's strict classifier never skips: a real GET error
        // fails closed rather than being read as absent==format-1 and written.
        let storage = Arc::new(InMemStorage::default());
        storage.fail_next_get();
        let error = format!("{:#}", gate(&handle(&storage)).await.unwrap_err());
        assert!(
            error.contains("read storage format stamp from bucket 'iron'"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn availability_skip_survives_with_a_reachable_sibling() {
        // The warn-skip arm: a GET failure the classifier calls availability is
        // skipped, and a reachable sibling (absent stamp) clears startup.
        let hung = Arc::new(InMemStorage::default());
        hung.fail_next_get();
        let healthy = Arc::new(InMemStorage::default());
        let handles = vec![
            BucketHandle {
                storage: hung.clone(),
                name: "iron".to_string(),
            },
            BucketHandle {
                storage: healthy.clone(),
                name: "tin".to_string(),
            },
        ];
        verify_format(&handles, |_, _| true).await.unwrap();
    }

    #[tokio::test]
    async fn every_bucket_skipped_refuses_rather_than_bind_blind() {
        // If the classifier skips the only bucket, nothing cleared: refuse so a
        // single-bucket serve never binds having verified no format.
        let storage = Arc::new(InMemStorage::default());
        storage.fail_next_get();
        let error = verify_format(&handle(&storage), |_, _| true)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("no configured bucket is reachable"),
            "{error}"
        );
    }
}
