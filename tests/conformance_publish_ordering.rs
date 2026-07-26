//! Conformance: a sidecar never lands ahead of the bytes it names.
//!
//! A sidecar is a bucket's assertion that a filename IS truth with that
//! sha256, and nothing downstream re-derives it: `replicate::decide` compares
//! sidecar shas to pick a merge verdict, the index renderer copies the sha into
//! `simple/`, and neither `verify-index` nor the audit ever re-hashes a stored
//! body. So a sidecar standing over a key whose body is not (yet) the one it
//! names is a lie with no reader that can catch it.
//!
//! The `packages/` write protocol therefore lands the artifact first and the
//! sidecar last, private and `sync --to` mirror uploads alike. The reverse
//! ordering is not merely a wider window, it is an unhealable one: a bare
//! artifact is hashed and given a real sidecar by `worker::backfill_sidecar`,
//! while a bare sidecar has no repairer at all and simply waits for the next
//! writer to take the still-free immutable key with different bytes.
//!
//! Every writer that owns the artifact key's conditional create is covered:
//! both upload origins (`publish::publish_record`) and the replication copy
//! (`replicate::execute`), driven for real against deterministic in-memory
//! buckets. The server-side copy transport is the one exemption and stays
//! sidecar-first — its copy verb has no create-if-absent, so a pre-check is the
//! only gate it can have.
//!
//! The rule is a property of the write *sequence*, so it is asserted on the
//! sequence (via a recording `Storage`) rather than on state a completed run
//! leaves behind. The consequence is asserted too, using the one abort that
//! needs no fault injection: the artifact's conditional create losing to a body
//! already under the key.

use std::sync::Arc;

use pypiron::hash::sha256_hex;
use pypiron::replicate::{decide, execute, read_record, ArtifactSource};
use pypiron::sidecar::{sidecar_key, Sidecar, Yanked};
use pypiron::sim::{multi_bucket_state, single_bucket_state, SimClock, SimStorage};
use pypiron::storage::Storage;
use pypiron::{publish_record, PublishBody, PublishRequest};

const PKG: &str = "ordering-alpha";
const FILENAME: &str = "ordering_alpha-1.0-py3-none-any.whl";

fn artifact_key() -> String {
    format!("packages/{PKG}/{FILENAME}")
}

fn clock() -> Arc<SimClock> {
    // 2026-01-01T00:00:00Z — fixed so the run is deterministic.
    SimClock::new(time::OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap())
}

fn request(body: Vec<u8>, is_mirror: bool) -> PublishRequest {
    PublishRequest {
        pkg: PKG.to_string(),
        filename: FILENAME.to_string(),
        sha256: sha256_hex(&body),
        size: body.len() as u64,
        version: "1.0".to_string(),
        requires_python: None,
        is_mirror,
        upload_time: "2026-01-01T00:00:00Z".to_string(),
        yanked: Yanked::Flag(false),
        wheel_metadata: None,
        is_wheel: false,
        provenance: None,
        body: PublishBody::Bytes(body),
    }
}

/// Every sidecar in the bucket must name the bytes actually stored under its
/// artifact key. A sidecar over an absent body is the unhealable half of the
/// ordering; a sidecar over *different* bytes is that lie already cashed in.
async fn assert_no_sidecar_outruns_its_bytes(storage: &dyn Storage, context: &str) {
    let key = artifact_key();
    let sc_bytes = match storage.get_bytes(&sidecar_key(&key)).await {
        Ok(bytes) => bytes,
        Err(_) => return, // no sidecar: nothing asserted, nothing to contradict
    };
    let sc: Sidecar = serde_json::from_slice(&sc_bytes).expect("sidecar parses");
    let body = storage.get_bytes(&key).await.unwrap_or_else(|_| {
        panic!("{context}: sidecar names sha {} with no artifact under {key} — an assertion the next writer of this still-free immutable key silently invalidates", sc.sha256)
    });
    assert_eq!(
        sha256_hex(&body),
        sc.sha256,
        "{context}: bucket serves bytes contradicting their own published sha256 under {key}"
    );
}

/// A `sync --to` upload whose artifact create loses the immutable key must not
/// leave its sidecar behind. Losing that create is the ordinary refusal path —
/// the same abort a crash, an origin-fence 409 or a storage error produces
/// between the two writes — and every one of them used to publish the sidecar
/// first.
#[tokio::test]
async fn a_refused_mirror_upload_leaves_no_sidecar_over_foreign_bytes() {
    let storage = SimStorage::new(clock());
    let state = single_bucket_state(storage.clone());

    // A bare artifact: the state `worker::backfill_sidecar` exists to heal (a
    // legacy tree, a `sync --from` fill, an interrupted publish). The immutable
    // key is taken; its sidecar slot is still free.
    let stored = b"the bytes already under the immutable key".to_vec();
    assert!(storage
        .put_if_absent(&artifact_key(), stored.clone(), None)
        .await
        .expect("seed the bare artifact"));

    let incoming = b"different bytes from a sync --to upload".to_vec();
    let pinned = state.pin();
    let (status, _) = publish_record(&state, &pinned, request(incoming, true))
        .await
        .expect_err("the immutable key is taken, so the upload must be refused");
    assert_eq!(status, axum::http::StatusCode::CONFLICT);

    assert_no_sidecar_outruns_its_bytes(storage.as_ref(), "refused mirror upload").await;
    assert_eq!(
        storage.get_bytes(&artifact_key()).await.expect("body kept"),
        stored,
        "a refused upload must not disturb the body that owns the key"
    );
}

/// The same refusal on the private path, which has always written the artifact
/// first — a control that pins the two paths to one ordering.
#[tokio::test]
async fn a_refused_private_upload_leaves_no_sidecar_over_foreign_bytes() {
    let storage = SimStorage::new(clock());
    let state = single_bucket_state(storage.clone());

    let stored = b"the bytes already under the immutable key".to_vec();
    assert!(storage
        .put_if_absent(&artifact_key(), stored.clone(), None)
        .await
        .expect("seed the bare artifact"));

    let incoming = b"different bytes from a private upload".to_vec();
    let pinned = state.pin();
    let (status, _) = publish_record(&state, &pinned, request(incoming, false))
        .await
        .expect_err("the immutable key is taken, so the upload must be refused");
    assert_eq!(status, axum::http::StatusCode::CONFLICT);

    assert_no_sidecar_outruns_its_bytes(storage.as_ref(), "refused private upload").await;
}

/// The accepted path still publishes a complete mirror record — the reorder
/// moved the sidecar, it did not drop it.
#[tokio::test]
async fn an_accepted_mirror_upload_still_publishes_its_own_sidecar() {
    let storage = SimStorage::new(clock());
    let state = single_bucket_state(storage.clone());

    let body = b"a fresh sync --to snapshot".to_vec();
    let expected = sha256_hex(&body);
    let pinned = state.pin();
    publish_record(&state, &pinned, request(body, true))
        .await
        .expect("a free key accepts the upload");

    let sc_bytes = storage
        .get_bytes(&sidecar_key(&artifact_key()))
        .await
        .expect("the mirror sidecar is published");
    let sc: Sidecar = serde_json::from_slice(&sc_bytes).expect("sidecar parses");
    assert_eq!(sc.sha256, expected);
    assert_eq!(sc.origin.as_deref(), Some("mirror"));
    assert_no_sidecar_outruns_its_bytes(storage.as_ref(), "accepted mirror upload").await;
}

/// A second `sync --to` of the same filename with different metadata is still
/// refused rather than merged: the record is immutable, sidecar included.
#[tokio::test]
async fn a_mirror_re_upload_with_different_metadata_is_still_refused() {
    let storage = SimStorage::new(clock());
    let state = single_bucket_state(storage.clone());

    let body = b"a fresh sync --to snapshot".to_vec();
    let pinned = state.pin();
    publish_record(&state, &pinned, request(body.clone(), true))
        .await
        .expect("a free key accepts the upload");

    let mut second = request(body, true);
    second.yanked = Yanked::Reason("upstream pulled it".to_string());
    let (status, _) = publish_record(&state, &pinned, second)
        .await
        .expect_err("the filename is already published");
    assert_eq!(status, axum::http::StatusCode::CONFLICT);
    assert_no_sidecar_outruns_its_bytes(storage.as_ref(), "mirror re-upload").await;
}

/// A [`Storage`] that records the order of the mutations it forwards. The
/// ordering rule is a property of the write *sequence*, not of any state a
/// completed run leaves behind — every path here is crash-safe at rest, and the
/// damage only shows when something lands between the two writes. So assert the
/// sequence directly and no interleaving has to be staged.
struct OrderedStorage {
    inner: Arc<SimStorage>,
    writes: std::sync::Mutex<Vec<String>>,
}

impl OrderedStorage {
    fn new(inner: Arc<SimStorage>) -> Arc<Self> {
        Arc::new(OrderedStorage {
            inner,
            writes: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn note(&self, key: &str) {
        self.writes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(key.to_string());
    }

    /// Position of the first write to `key`, or `None` if it was never written.
    fn first_write(&self, key: &str) -> Option<usize> {
        self.writes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .position(|k| k == key)
    }
}

#[async_trait::async_trait]
impl Storage for OrderedStorage {
    async fn head_exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner.head_exists(key).await
    }
    async fn stored_size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.stored_size(key).await
    }
    async fn serve_artifact(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> anyhow::Result<axum::response::Response> {
        self.inner.serve_artifact(key, range).await
    }
    async fn presign_get(
        &self,
        key: &str,
        expires: std::time::Duration,
    ) -> anyhow::Result<Option<String>> {
        self.inner.presign_get(key, expires).await
    }
    async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        self.note(key);
        self.inner.put_bytes(key, bytes, content_type).await
    }
    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.note(key);
        self.inner.put_if_absent(key, bytes, content_type).await
    }
    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.note(key);
        self.inner.put_file_if_absent(key, path, content_type).await
    }
    async fn get_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.get_bytes(key).await
    }
    async fn list_dir_entries(
        &self,
        dir_prefix: &str,
    ) -> anyhow::Result<Vec<pypiron::storage::FileEntry>> {
        self.inner.list_dir_entries(dir_prefix).await
    }
    async fn list_all(&self, prefix: &str) -> anyhow::Result<Vec<pypiron::storage::ObjectMeta>> {
        self.inner.list_all(prefix).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<pypiron::storage::ObjectMeta>> {
        self.inner.list_page(prefix, after, limit).await
    }
    async fn delete_keys(&self, keys: &[String]) -> anyhow::Result<()> {
        for key in keys {
            self.note(key);
        }
        self.inner.delete_keys(keys).await
    }
    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }
    async fn get_with_etag(&self, key: &str) -> anyhow::Result<Option<(Vec<u8>, String)>> {
        self.inner.get_with_etag(key).await
    }
    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<Option<String>> {
        self.note(key);
        self.inner.put_if_none_match(key, bytes).await
    }
    async fn put_if_match(
        &self,
        key: &str,
        etag: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<Option<String>> {
        self.note(key);
        self.inner.put_if_match(key, etag, bytes).await
    }
}

/// The rule, asserted on the sequence: on every writer that owns a conditional
/// create for the artifact key, the bytes land before the sidecar that names
/// them. Both upload origins and the replication copy are covered.
async fn assert_artifact_precedes_sidecar(recorder: &OrderedStorage, context: &str) {
    let akey = artifact_key();
    let skey = sidecar_key(&akey);
    let artifact = recorder
        .first_write(&akey)
        .unwrap_or_else(|| panic!("{context}: the artifact was never written"));
    let sidecar = recorder
        .first_write(&skey)
        .unwrap_or_else(|| panic!("{context}: the sidecar was never written"));
    assert!(
        artifact < sidecar,
        "{context}: the sidecar was published at write #{sidecar}, ahead of the bytes it names at write #{artifact} — anything landing in that window leaves this bucket asserting a sha256 it does not hold"
    );
}

#[tokio::test]
async fn a_mirror_upload_lands_its_bytes_before_the_sidecar_that_names_them() {
    let recorder = OrderedStorage::new(SimStorage::new(clock()));
    let state = single_bucket_state(recorder.clone());
    let pinned = state.pin();
    publish_record(
        &state,
        &pinned,
        request(b"a fresh sync --to snapshot".to_vec(), true),
    )
    .await
    .expect("a free key accepts the upload");
    assert_artifact_precedes_sidecar(&recorder, "sync --to upload").await;
}

#[tokio::test]
async fn a_private_upload_lands_its_bytes_before_the_sidecar_that_names_them() {
    let recorder = OrderedStorage::new(SimStorage::new(clock()));
    let state = single_bucket_state(recorder.clone());
    let pinned = state.pin();
    publish_record(
        &state,
        &pinned,
        request(b"a private release".to_vec(), false),
    )
    .await
    .expect("a free key accepts the upload");
    assert_artifact_precedes_sidecar(&recorder, "private upload").await;
}

/// The replication copy obeys the same rule on the stream transport, where it
/// already holds sha-verified bytes before it touches the destination. (The
/// server-side copy transport is exempt and stays sidecar-first: its copy verb
/// has no create-if-absent, so a pre-check is the only gate it can have.)
#[tokio::test]
async fn a_mirror_copy_lands_its_bytes_before_the_sidecar_that_names_them() {
    let clk = clock();
    let source = SimStorage::new(clk.clone());

    // Publish the source record before the fleet exists, so the recorder only
    // ever sees the copy's own writes.
    let solo = single_bucket_state(source.clone());
    let pin = solo.pin();
    publish_record(
        &solo,
        &pin,
        request(b"the upstream snapshot the copy carries".to_vec(), true),
    )
    .await
    .expect("the source publishes its mirror record");

    let recorder = OrderedStorage::new(SimStorage::new(clk));
    let state = multi_bucket_state(vec![
        ("source".to_string(), source.clone() as Arc<dyn Storage>),
        ("dest".to_string(), recorder.clone() as Arc<dyn Storage>),
    ]);
    let src_rec = read_record(source.as_ref(), PKG, FILENAME)
        .await
        .expect("read the source record");
    let dst_rec = read_record(recorder.as_ref(), PKG, FILENAME)
        .await
        .expect("read the destination record");
    let verdict = decide(&src_rec, &dst_rec);
    assert!(
        matches!(verdict, pypiron::replicate::Verdict::Copy(_)),
        "an empty destination must take the copy path, got {verdict:?}"
    );
    execute(
        &state,
        (source.as_ref(), recorder.as_ref()),
        PKG,
        FILENAME,
        (&src_rec, &dst_rec),
        verdict,
        ArtifactSource::Bucket,
    )
    .await
    .expect("the copy lands");
    assert_artifact_precedes_sidecar(&recorder, "sync --to replication copy").await;
}
