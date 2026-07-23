//! The write path: uploads (the legacy multipart endpoint and its
//! storage-protocol core), deletes, PEP 592 yank, and PEP 792 project status.
//! Each HTTP handler is a thin wrapper over a core the deterministic simulator
//! drives directly. Split out of `app.rs`; the shared response/error helpers and
//! auth guard stay in `app.rs`/`auth.rs` and are imported here.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::{
    extract::{Multipart, Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::warn;

use crate::app::{internal, AppState, PACKAGES_PREFIX, SIMPLE_PREFIX};
use crate::auth::require_admin;
use crate::names::{
    checked_pkg_name, infer_package_from_filename, infer_version_from_filename, is_normalized,
    normalize_pkg_name,
};
use crate::sidecar::{
    frozen_key, metadata_key, provenance_key, sidecar_key, tombstone_key, Sidecar, Yanked,
};
use crate::storage::Storage;
use crate::{
    buckets, markers, names, origin, replicate, sidecar, status, storage, tombstone, upload, wheel,
    worker,
};

/// --- Upload endpoint ------------------------------------------------------
/// Legacy PyPI upload endpoint compatible with uv/twine.
/// Multipart form with metadata text fields (name, version, sha256_digest,
/// requires_python, ...) and the file in field "content" (or "file").
/// Upper bound for the PEP 740 `provenance`/`attestations` form fields. These
/// JSON objects are KBs in practice; the cap only guards against a pathological
/// part buffering unbounded bytes in RAM.
const PROVENANCE_MAX_FIELD_BYTES: usize = 4 * 1024 * 1024;

/// Bound the non-file metadata parts as a whole. The per-field cap above doesn't
/// stop a flood of uniquely-named 64 KiB fields — ~16k of them fit under the
/// 1 GiB body limit and sit resident in the `fields` map at once, OOMing a small
/// box. Real uploads send a few dozen small fields (plus the two large JSON
/// ones), so these limits are generous headroom, not a functional constraint.
const MAX_METADATA_FIELDS: usize = 256;
const MAX_METADATA_TOTAL_BYTES: usize = 32 * 1024 * 1024;

/// The already-spooled or in-memory body an upload writes. `Spool` carries the
/// temp file (self-deleting on drop) the handler streamed the multipart into;
/// `Bytes` lets a deterministic simulator hand [`publish_record`] a body without
/// a filesystem. Either maps to a [`storage::ArtifactBody`] for the verified
/// store.
pub enum PublishBody {
    Spool(upload::TempPath),
    Bytes(Vec<u8>),
}

/// Everything [`publish_record`] needs once the handler has finished the HTTP
/// concerns (auth, multipart spool, filename/name/digest validation, mirror
/// gating, wheel-metadata extraction). Every field is already validated and
/// normalized; the core never re-reads the request or re-derives them.
pub struct PublishRequest {
    /// PEP 503-normalized package name, already validated as a storage segment.
    pub pkg: String,
    /// Artifact filename, already validated against path/sidecar collisions.
    pub filename: String,
    /// Spooled temp file (handler) or in-memory bytes (simulator).
    pub body: PublishBody,
    /// SHA-256 of `body`, already verified against any client-supplied digest.
    pub sha256: String,
    /// Byte length of `body`.
    pub size: u64,
    /// Version string as the handler derived it (form field or filename).
    pub version: String,
    /// `Requires-Python` for the sidecar, if the client sent one.
    pub requires_python: Option<String>,
    /// True for a mirror (`sync --to`, admin) upload; false for a private one.
    pub is_mirror: bool,
    /// Upload timestamp: mirror-provided (backdated) or `now_rfc3339`.
    pub upload_time: String,
    /// Yank state for the sidecar (mirror uploads can arrive pre-yanked).
    pub yanked: Yanked,
    /// PEP 658 wheel METADATA, pre-extracted off the async runtime.
    pub wheel_metadata: Option<Vec<u8>>,
    /// True when `filename` is a wheel (drives PEP 658 metadata handling).
    pub is_wheel: bool,
    /// PEP 740 provenance JSON relayed by a mirror upload, if present.
    pub provenance: Option<String>,
}

pub(crate) async fn legacy_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Mirror-ness lives in a form field, so whether *admin* is required can't
    // be decided until the body is parsed. But every upload needs at least
    // uploader rights, so reject that up front — preserving "never read the
    // body of an unauthorized request".
    let is_admin = state.is_admin(&headers);
    if !is_admin && !state.is_uploader(&headers) {
        return Err(if state.uploads_disabled() {
            (
                StatusCode::FORBIDDEN,
                "Uploads are disabled (no upload credential configured)".into(),
            )
        } else {
            (StatusCode::UNAUTHORIZED, "Unauthorized".into())
        });
    }

    let mut filename_opt: Option<String> = None;
    let mut spooled: Option<upload::FinishedSpool> = None;
    let mut fields: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    // Cumulative bytes across non-file parts — bounds the metadata map's RAM.
    let mut metadata_total_bytes: usize = 0;

    while let Some(mut field) = multipart.next_field().await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Invalid multipart form data".into(),
        )
    })? {
        let field_name = field.name().unwrap_or("").to_string();
        let part_filename = field.file_name().map(|s| s.to_string());

        match field_name.as_str() {
            "content" | "file" => {
                // Stream to a temp file, hashing as we go — memory stays
                // chunk-sized no matter how big the wheel is (see upload.rs).
                let mut spool = upload::UploadSpool::new(&state.spool_dir)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Could not open upload spool: {e}"),
                        )
                    })?;
                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => spool.write_chunk(&chunk).await.map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("Could not spool uploaded file: {e}"),
                            )
                        })?,
                        Ok(None) => break,
                        Err(_) => {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Could not read uploaded file".into(),
                            ))
                        }
                    }
                }
                spooled = Some(spool.finish().await.map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Could not finish upload spool: {e}"),
                    )
                })?);
                if filename_opt.is_none() {
                    filename_opt = part_filename;
                }
            }
            _ => {
                // Metadata fields are tiny (version, sha256_digest, ...). The
                // artifact is streamed to a disk spool; a non-content part must
                // not be the hole that buffers ~1 GiB in RAM and OOMs the box.
                // The PEP 740 provenance/attestations objects are larger JSON —
                // bounded higher, but still bounded.
                let max_field_bytes = match field_name.as_str() {
                    "provenance" | "attestations" => PROVENANCE_MAX_FIELD_BYTES,
                    _ => 64 * 1024,
                };
                let mut buf = Vec::new();
                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => {
                            if buf.len() + chunk.len() > max_field_bytes {
                                return Err((
                                    StatusCode::BAD_REQUEST,
                                    format!("Form field '{field_name}' is too large"),
                                ));
                            }
                            buf.extend_from_slice(&chunk);
                        }
                        Ok(None) => break,
                        Err(_) => {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Invalid multipart form data".into(),
                            ))
                        }
                    }
                }
                if let Ok(text) = String::from_utf8(buf) {
                    if !text.is_empty() {
                        metadata_total_bytes += text.len();
                        if metadata_total_bytes > MAX_METADATA_TOTAL_BYTES {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Metadata fields too large".into(),
                            ));
                        }
                        if !fields.contains_key(&field_name) && fields.len() >= MAX_METADATA_FIELDS
                        {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Too many metadata fields".into(),
                            ));
                        }
                        fields.insert(field_name, text);
                    }
                }
            }
        }
    }

    let filename = filename_opt
        .or_else(|| fields.get("filename").cloned())
        .ok_or((StatusCode::BAD_REQUEST, "Missing filename".to_string()))?;
    let spooled = spooled.ok_or((StatusCode::BAD_REQUEST, "Missing file content".to_string()))?;

    // No path separators, dotfiles, or names colliding with sidecar suffixes.
    if !valid_artifact_filename(&filename) {
        return Err((StatusCode::BAD_REQUEST, "Invalid filename".into()));
    }

    let pkg_norm = match fields.get("name") {
        Some(name) => normalize_pkg_name(name),
        None => infer_package_from_filename(&filename),
    };
    // Normalized names are storage path segments; anything else is hostile.
    if !is_normalized(&pkg_norm) {
        return Err((StatusCode::BAD_REQUEST, "Invalid package name".into()));
    }

    // The hash was computed incrementally during spooling. Zip extraction
    // reads the central directory + one entry from the spool file — it is
    // I/O + CPU bound, so off the async runtime.
    let is_wheel = filename.ends_with(".whl");
    let sha256 = spooled.sha256.clone();
    let wheel_metadata = if is_wheel {
        let path = spooled.path.path().to_path_buf();
        tokio::task::spawn_blocking(move || wheel::extract_metadata_from_file(&path))
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Metadata extraction task failed".to_string(),
                )
            })?
    } else {
        None
    };

    // Verify the client-supplied digest, and capture the hash for the sidecar.
    if let Some(claimed) = fields.get("sha256_digest") {
        if !claimed.eq_ignore_ascii_case(&sha256) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("sha256_digest mismatch: form says {claimed}, file is {sha256}"),
            ));
        }
    }

    // A claimed version must correspond to the filename (PEP 427/625 — every
    // standard build tool derives the name *from* the metadata, so a mismatch is
    // hand-crafted). Enforcing it here makes the filename authoritative by
    // construction, which the project page's cheap version check and the
    // advisory byte gate already rule on. Mirror uploads pass trivially: sync
    // has no version source but the filename (the Simple API carries none), so
    // it always sends the inferred value — and must keep doing so if it ever
    // learns true versions from the JSON API. Legacy binary formats infer no
    // version, so those still take the field's word for it.
    if let (Some(claimed), Some(from_name)) = (
        fields.get("version"),
        infer_version_from_filename(&filename),
    ) {
        if names::fold_version(claimed) != names::fold_version(&from_name) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("version '{claimed}' does not match filename '{filename}'"),
            ));
        }
    }
    let version = fields
        .get("version")
        .cloned()
        .or_else(|| infer_version_from_filename(&filename))
        .unwrap_or_default();

    // Mirror mode: `sync --to` sends mirror=true plus PyPI's historical
    // metadata. Backdating is an admin privilege — never reachable with plain
    // uploader rights, and never reinterpreted as a normal upload.
    let is_mirror = fields.get("mirror").map(String::as_str) == Some("true");
    if is_mirror {
        if !is_admin {
            // Distinguish "admin disabled here" from "you're not admin".
            return Err(if state.admin_credential().is_none() {
                (
                    StatusCode::FORBIDDEN,
                    "Mirror uploads are disabled (no admin credential configured)".into(),
                )
            } else {
                (
                    StatusCode::UNAUTHORIZED,
                    "Mirror uploads require the admin credential".into(),
                )
            });
        }
    } else if fields.contains_key("upload_time")
        || fields.contains_key("yanked")
        || fields.contains_key("yanked_reason")
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "upload_time/yanked fields require a mirror upload (mirror=true, admin credential)"
                .into(),
        ));
    }

    // PEP 740: pypiron relays PyPI's already-verified provenance through the
    // proxy/sync mirror paths, but is not itself a verifying authority and
    // cannot synthesize a valid provenance object from a bare `attestations`
    // array (it has no Trusted Publisher identity). Refuse first-party
    // attestations fail-closed rather than store something no verifier trusts.
    if !is_mirror && fields.contains_key("attestations") {
        return Err((
            StatusCode::BAD_REQUEST,
            "pypiron relays mirrored provenance (via the proxy and sync) but does not verify \
             first-party attestations; re-run the upload without --attestations"
                .into(),
        ));
    }

    let upload_time = match fields.get("upload_time") {
        Some(ts) => {
            if OffsetDateTime::parse(ts, &Rfc3339).is_err() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("upload_time is not RFC 3339: {ts}"),
                ));
            }
            ts.clone()
        }
        None => now_rfc3339(),
    };
    let yanked = if is_mirror {
        match (fields.get("yanked_reason"), fields.get("yanked")) {
            (Some(reason), _) if !reason.trim().is_empty() => {
                Yanked::Reason(reason.trim().to_string())
            }
            (_, Some(flag)) => Yanked::Flag(flag == "true"),
            _ => Yanked::Flag(false),
        }
    } else {
        Yanked::Flag(false)
    };

    // Hand off to the storage-protocol core. The handler has finished every HTTP
    // concern (auth, multipart spool, validation, digest, mirror gating); the
    // origin/claim/fence/commit machine below is what a deterministic simulator
    // drives without axum.
    let req = PublishRequest {
        pkg: pkg_norm,
        filename,
        body: PublishBody::Spool(spooled.path),
        sha256,
        size: spooled.size,
        version,
        requires_python: fields.get("requires_python").cloned(),
        is_mirror,
        upload_time,
        yanked,
        wheel_metadata,
        is_wheel,
        provenance: fields.get("provenance").cloned(),
    };
    // Pin the storage context once for the whole upload (design §3): the origin
    // claim, artifact/sidecar writes, and commit marker all land on this handle.
    let pinned = state.pin();
    publish_record(&state, &pinned, req).await
}

/// The storage-protocol core of an upload: origin observation → private-prefix
/// and cross-origin rejects → intent marker → origin claim (with the early
/// package-level fan-out) → mirror sidecar create → write fence → verified
/// artifact store → mirror post-publish claim re-check → tombstone/frozen
/// filename fence → PEP 658 metadata, PEP 740 provenance, sidecar → commit
/// marker → replication fan-out → read-your-writes index wait → ack. Split out
/// of [`legacy_upload`] so a deterministic simulator can exercise this state
/// machine directly; the handler owns every HTTP concern above it.
pub async fn publish_record(
    state: &AppState,
    pinned: &buckets::Pinned,
    req: PublishRequest,
) -> Result<(StatusCode, &'static str), (StatusCode, String)> {
    let PublishRequest {
        pkg: pkg_norm,
        filename,
        body,
        sha256,
        size,
        version,
        requires_python,
        is_mirror,
        upload_time,
        yanked,
        wheel_metadata,
        is_wheel,
        provenance,
    } = req;
    let key = format!("{PACKAGES_PREFIX}{pkg_norm}/{filename}");

    // Origin exclusivity: each package belongs to exactly one world. A
    // mismatch is a hard error, never a merge — the dependency-confusion
    // defense. Storage errors are outages (503), never "unclaimed".
    let desired_origin = if is_mirror {
        origin::MIRROR
    } else {
        origin::PRIVATE
    };
    let storage = pinned.storage.as_ref();
    let observed_origin = origin::read_origin_observation(storage, &pkg_norm)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error reading origin: {e}"),
            )
        })?;
    let mut write_fence = observed_origin.as_ref().cloned();
    // The private namespace is off-limits to mirrors regardless of claim
    // state — checked here, not only at first write, so adopting a prefix
    // after a name was mirror-claimed still shuts the door.
    if is_mirror {
        if let Some(prefix) = &state.private_prefix {
            if names::matches_prefix(&pkg_norm, prefix) {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("'{pkg_norm}' is inside the private namespace '{prefix}'; mirrors may not touch it"),
                ));
            }
        }
    }
    if let Some(owner) = observed_origin.as_ref().map(|observed| observed.state) {
        if matches!(
            owner,
            origin::OriginState::Mirror | origin::OriginState::Private
        ) && owner.as_str() != desired_origin
        {
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "Package '{pkg_norm}' is {}-owned; {desired_origin} uploads are rejected",
                    owner.as_str()
                ),
            ));
        }
    }

    // The crash-recovery marker is correctness-critical in every mode: it is the
    // only durable signal that carries a global-index membership change (a new
    // name appearing) to the worker. Dropping it before touching truth — and
    // refusing the write if it fails — keeps the audit a safety net for external
    // change, never a substitute for pypiron's own bookkeeping.
    let intent_nonce = Some(
        markers::mark_intent(storage, &pkg_norm)
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("failed to reserve package write: {e}"),
                )
            })?,
    );
    match observed_origin.as_ref().map(|observed| observed.state) {
        Some(origin::OriginState::Mirror) if desired_origin == origin::MIRROR => {}
        Some(origin::OriginState::Private) if desired_origin == origin::PRIVATE => {}
        Some(owner @ (origin::OriginState::Mirror | origin::OriginState::Private)) => {
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "Package '{pkg_norm}' is {}-owned; {desired_origin} uploads are rejected",
                    owner.as_str()
                ),
            ));
        }
        None | Some(origin::OriginState::Unclaimed) => {
            // A new private name must be inside the prefix; existing private
            // packages outside a newly-adopted prefix are grandfathered (only
            // first claims are gated, so adopting a prefix never bricks them).
            if let Some(prefix) = &state.private_prefix {
                if !is_mirror && !names::matches_prefix(&pkg_norm, prefix) {
                    return Err((
                        StatusCode::FORBIDDEN,
                        format!(
                            "Package '{pkg_norm}' does not match the private prefix '{prefix}'"
                        ),
                    ));
                }
            }
            // First write claims the package — atomically, so racing private
            // and mirror first-writes can't merge origins.
            let claim = origin::claim_origin(
                storage,
                &pkg_norm,
                origin::ClaimRequest::new(
                    desired_origin,
                    observed_origin
                        .as_ref()
                        .filter(|observed| observed.state == origin::OriginState::Unclaimed),
                ),
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to claim origin: {e}"),
                )
            })?;
            if claim.owner != desired_origin {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!(
                        "Package '{pkg_norm}' is {}-owned; {desired_origin} uploads are rejected",
                        claim.owner
                    ),
                ));
            }
            // A claim can survive even when the uploader dies before writing an
            // artifact. Fan the package-level claim out to every healthy bucket
            // before the artifact even lands locally, so the private name is
            // reserved fleet-wide ahead of its bytes (the dependency-confusion
            // boundary); the later artifact fan-out re-claims idempotently.
            if !is_mirror && claim.etag.is_some() && state.buckets.is_multi() {
                replicate::fanout_sync(state, pinned, &pkg_norm, replicate::ORIGIN_MARKER).await;
            }
            write_fence = Some(match claim.etag {
                Some(etag) => origin::OriginObservation {
                    state: if is_mirror {
                        origin::OriginState::Mirror
                    } else {
                        origin::OriginState::Private
                    },
                    etag,
                },
                None => origin::read_origin_observation(storage, &pkg_norm)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            format!("storage error re-reading origin claim: {e}"),
                        )
                    })?
                    .filter(|observed| observed.state.as_str() == desired_origin)
                    .ok_or_else(|| {
                        (
                            StatusCode::CONFLICT,
                            format!("Package '{pkg_norm}' changed origin while claiming"),
                        )
                    })?,
            });
        }
    }

    let sc = Sidecar {
        sha256,
        size,
        version,
        upload_time,
        requires_python,
        yanked,
        // Per-artifact origin (§4/§6.2): the replicator decides "private only"
        // from state, never from history.
        origin: Some(desired_origin.to_string()),
        upload_epoch_ms: (!is_mirror).then(now_epoch_millis),
        yank_epoch: 0,
    };
    let sc_bytes = serde_json::to_vec(&sc).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode sidecar".to_string(),
        )
    })?;
    if is_mirror {
        let sc_key = sidecar_key(&key);
        let created = storage
            .put_if_absent(&sc_key, sc_bytes.clone(), Some("application/json"))
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("Failed to store mirror sidecar: {e}"),
                )
            })?;
        if !created {
            let existing = storage.get_bytes(&sc_key).await.map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("Failed to verify mirror sidecar: {e}"),
                )
            })?;
            if existing != sc_bytes {
                let _ = markers::commit_marker(state, storage, &pkg_norm, intent_nonce).await;
                return Err((
                    StatusCode::CONFLICT,
                    format!("File metadata already exists: {filename}"),
                ));
            }
        }
    }

    // Every multi-bucket writer consumes the exact origin observation it began
    // under, closing concurrent origin changes around its artifact write.
    if let Some(ref expected) = write_fence {
        match origin::read_origin_observation(storage, &pkg_norm).await {
            Ok(Some(current)) if current == *expected => {}
            Ok(_) => {
                if let Err(e) =
                    markers::commit_marker(state, storage, &pkg_norm, intent_nonce).await
                {
                    warn!(error=?e, "legacy: failed to close abandoned intent marker");
                }
                return Err((
                    StatusCode::CONFLICT,
                    format!("Package '{pkg_norm}' changed origin during upload"),
                ));
            }
            Err(e) => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error re-checking origin claim: {e}"),
                ));
            }
        }
    }

    // Ordering invariant: artifact, then sidecars, then index job.
    // The conditional create IS the immutability rule (pypi.org's): a plain
    // HEAD-then-PUT is a TOCTOU hole that lets concurrent uploads swap bytes.
    // The write is verified (D1) and bounded (D3): a 200 that landed zero bytes
    // never acks, and a wedged connection fails fast instead of parking on the
    // one-hour transport ceiling. Immutability is preserved — an existing body
    // is still a 409; only this writer's own corrupt debris is cleared so a
    // retry starts from a clean key.
    let artifact_body = match &body {
        PublishBody::Spool(temp) => storage::ArtifactBody::Spool(temp.path()),
        PublishBody::Bytes(bytes) => storage::ArtifactBody::Bytes(bytes.clone()),
    };
    match storage::store_artifact_verified(
        storage,
        &key,
        artifact_body,
        size,
        Some("application/octet-stream"),
        storage::Existing::Reject,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Err((
                StatusCode::CONFLICT,
                format!("File already exists: {filename}"),
            ));
        }
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Failed to store file: {e}"),
            ));
        }
    }

    // The mirror sidecar already exists. In multi-bucket mode, if demotion won
    // after the final fence, leave the typed loser in place for private-precedence
    // quarantine; deleting here would race a newer private body under the same
    // immutable key. Single-bucket mode cannot demote behind this writer, so the
    // shared helper returns without another storage read.
    if is_mirror {
        let Some(expected) = write_fence.as_ref() else {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "mirror upload lost its origin fence".to_string(),
            ));
        };
        match post_publish_mirror_claim_is_current(state, storage, &pkg_norm, expected).await {
            Ok(true) => {}
            Ok(false) => {
                if let Err(e) =
                    markers::commit_marker(state, storage, &pkg_norm, intent_nonce).await
                {
                    warn!(error=?e, "legacy: failed to close post-publish origin race");
                }
                return Err((
                    StatusCode::CONFLICT,
                    format!("Package '{pkg_norm}' changed origin during mirror upload"),
                ));
            }
            Err(e) => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error re-checking mirror claim after publish: {e}"),
                ));
            }
        }
    }

    // One post-create tombstone HEAD preserves the single-bucket write path.
    // Multi-bucket mode also checks `.frozen`, whose first-write ordering makes
    // every interrupted freeze a durable filename fence. A fenced multi-bucket
    // loser stays occupied and inert: deleting by key here could erase a private
    // replacement that landed after this writer's cross-object read.
    let filename_fenced = if state.buckets.is_multi() {
        futures::future::try_join(
            storage.head_exists(&tombstone_key(&key)),
            storage.head_exists(&frozen_key(&key)),
        )
        .await
        .map(|(tombstoned, frozen)| tombstoned || frozen)
    } else {
        storage.head_exists(&tombstone_key(&key)).await
    };
    match filename_fenced {
        Ok(false) => {}
        Ok(true) => {
            if !state.buckets.is_multi() {
                let _ = storage.delete_keys(std::slice::from_ref(&key)).await;
            }
            if let Err(e) = markers::commit_marker(state, storage, &pkg_norm, intent_nonce).await {
                warn!(error=?e, "legacy: failed to close fenced upload intent");
            }
            return Err((
                StatusCode::CONFLICT,
                format!("File '{filename}' is frozen or deleted and cannot be reused"),
            ));
        }
        Err(e) => {
            if !state.buckets.is_multi() {
                let _ = storage.delete_keys(std::slice::from_ref(&key)).await;
            }
            if let Err(commit_error) =
                markers::commit_marker(state, storage, &pkg_norm, intent_nonce).await
            {
                warn!(error=?commit_error, "legacy: failed to close filename-fence upload intent");
            }
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error checking filename fence: {e}"),
            ));
        }
    }

    // PEP 658: capture the wheel's METADATA as a static file next to it.
    if is_wheel {
        match wheel_metadata {
            Some(md) => {
                let write = if is_mirror {
                    storage
                        .put_if_absent(&metadata_key(&key), md, Some("text/plain; charset=utf-8"))
                        .await
                        .map(|_| ())
                } else {
                    storage
                        .put_bytes(&metadata_key(&key), md, Some("text/plain; charset=utf-8"))
                        .await
                };
                if let Err(e) = write {
                    warn!(error=?e, %filename, "failed to store PEP 658 metadata");
                }
            }
            None => warn!(%filename, "wheel has no extractable METADATA"),
        }
    }

    // PEP 740: store the relayed provenance object next to the artifact. Only
    // mirror uploads carry it (`sync --to` forwards PyPI's provenance verbatim);
    // first-party attestations were refused above. Best-effort, like metadata:
    // a missing companion only drops the supply-chain signal.
    if is_mirror {
        if let Some(prov) = provenance.as_ref() {
            if let Err(e) = storage
                .put_if_absent(
                    &provenance_key(&key),
                    prov.clone().into_bytes(),
                    Some("application/json"),
                )
                .await
            {
                warn!(error=?e, %filename, "failed to store PEP 740 provenance");
            }
        }
    }

    if !is_mirror {
        storage
            .put_bytes(&sidecar_key(&key), sc_bytes, Some("application/json"))
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to store sidecar: {e}"),
                )
            })?;
    }

    // Commit marker: truth changed, rebuild now. Pairs with the intent above
    // so the worker consumes both; if this write fails the intent still goes
    // stale and heals the package.
    if let Err(e) = markers::commit_marker(state, storage, &pkg_norm, intent_nonce).await {
        warn!(error=?e, "legacy: failed to write commit marker");
    }

    // Stream the record to every other healthy bucket before the ack; any
    // bucket that misses gets a durable `_repl/` note for the sweep. Mirror
    // cache content is intentionally local and pays none of this cost.
    if !is_mirror {
        replicate::fanout_sync(state, pinned, &pkg_norm, &filename).await;
    }

    // Read-your-writes by waiting: poll our own index until the file shows
    // up, so publish-then-install pipelines never see a missing version.
    if state.wait_on_upload {
        wait_for_index_visibility(state, storage, &pkg_norm, &filename).await;
    }

    // Return a simple OK text body compatible with legacy clients.
    Ok((StatusCode::OK, "OK"))
}

/// Bounded wait for a freshly uploaded file to appear in the package index.
/// A timeout still returns success upstream — the artifact is durable and the
/// index will catch up; failing the upload would only provoke a client retry
/// into the 409 from immutability.
async fn wait_for_index_visibility(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
) {
    let key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
    let deadline = std::time::Instant::now() + state.wait_on_upload_timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(bytes) = storage.get_bytes(&key).await {
            #[derive(serde::Deserialize)]
            struct Index {
                files: Vec<File>,
            }
            #[derive(serde::Deserialize)]
            struct File {
                filename: String,
            }
            if let Ok(idx) = serde_json::from_slice::<Index>(&bytes) {
                if idx.files.iter().any(|f| f.filename == filename) {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    warn!(%pkg, %filename, "wait-on-upload: index visibility wait timed out");
}

/// Current time as RFC 3339 at whole-second precision.
fn now_rfc3339() -> String {
    crate::clock::now_utc()
        .replace_nanosecond(0)
        .unwrap_or_else(|_| crate::clock::now_utc())
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Current Unix epoch time in milliseconds for the private-upload conflict
/// tiebreak. A pre-epoch or unrepresentable system clock degrades to a value
/// that conflict reconciliation will quarantine rather than trusting blindly.
fn now_epoch_millis() -> u64 {
    crate::clock::now_epoch_millis()
}

/// Re-check a mirror writer's exact claim after its artifact becomes visible.
/// Only multi-bucket replication can demote that claim concurrently. Keeping the
/// single-bucket branch I/O-free preserves the original serving-path cost.
pub(crate) async fn post_publish_mirror_claim_is_current(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    expected: &origin::OriginObservation,
) -> Result<bool> {
    if !state.buckets.is_multi() {
        return Ok(true);
    }
    Ok(origin::read_origin_observation(storage, pkg)
        .await?
        .as_ref()
        == Some(expected))
}

// --- Deletion + yank (PEP 592) ----------------------------------------------

/// Delete an artifact. Ordering invariant: the file leaves the index first,
/// then the artifact goes, then its sidecars — a listed-but-missing file is
/// the only harmful state, and this order never produces one.
pub(crate) async fn files_delete(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    // Artifacts only: .origin, sidecars, and metadata companions are managed
    // by the server, not deletable handles.
    let Some(pkg) = checked_pkg_name(&package).filter(|_| valid_artifact_filename(&filename))
    else {
        return Err((StatusCode::NOT_FOUND, "No such file".into()));
    };
    // Pin once (design §3): the existence check, index rewrite, and artifact +
    // sidecar deletes all run against this handle.
    let pinned = state.pin();
    delete_record(&state, &pinned, &pkg, &filename).await
}

/// The storage-protocol core of an artifact delete: existence check → intent
/// marker → origin checks (refuse a delete without a live claim; multi-bucket
/// refuses mirror eviction) → index rewrite dropping the file → origin re-check
/// → tombstone-before-delete for private → artifact delete → presign-cache
/// invalidation → companion/sidecar deletes (`.origin` retained) → commit marker
/// → replication fan-out → 204. Split out of [`files_delete`] so a deterministic
/// simulator can drive it without axum.
pub async fn delete_record(
    state: &AppState,
    pinned: &buckets::Pinned,
    pkg: &str,
    filename: &str,
) -> Result<StatusCode, (StatusCode, String)> {
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let storage = pinned.storage.as_ref();
    match storage.head_exists(&key).await {
        Ok(true) => {}
        Ok(false) => return Err((StatusCode::NOT_FOUND, "No such file".into())),
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error: {e}"),
            ));
        }
    }
    // Correctness-critical in every mode: deleting a package's last file prunes
    // it from the global index, and the intent marker is the only durable signal
    // that carries that removal to the worker. Fail the delete before touching
    // truth if the marker can't be written, rather than mutate truth with no
    // breadcrumb and leave the prune to the external-change audit.
    let intent_nonce = Some(markers::mark_intent(storage, pkg).await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("failed to reserve delete: {e}"),
        )
    })?);

    // A mirror cache eviction cannot be made atomic with a concurrent
    // mirror->private package demotion: the claim and artifact are separate S3
    // objects. Manufacturing a private tombstone after any uncertain claim
    // movement would propagate a cache eviction over real private truth.
    // Refuse this unnecessary admin operation instead of pretending a
    // cross-object transaction exists. Broad lifecycle expiry is not a safe
    // substitute because private and mirror records share `packages/`.
    let origin_before = match origin::read_origin_observation(storage, pkg).await {
        Ok(Some(observed))
            if matches!(
                observed.state,
                origin::OriginState::Private | origin::OriginState::Mirror
            ) =>
        {
            observed
        }
        Ok(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("artifact '{filename}' has no live origin claim; refusing delete"),
            ));
        }
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("storage error reading origin: {e}"),
            ));
        }
    };
    if state.buckets.is_multi() && origin_before.state == origin::OriginState::Mirror {
        let _ = markers::commit_marker(state, storage, pkg, intent_nonce).await;
        return Err((
            StatusCode::CONFLICT,
            "Mirror cache eviction is disabled with multiple buckets".into(),
        ));
    }

    worker::rebuild_package_excluding(state, storage, pkg, Some(filename))
        .await
        .map_err(|e| internal("index rewrite failed", e))?;

    if state.buckets.is_multi() {
        let current = origin::read_origin_observation(storage, pkg)
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error re-checking origin: {e}"),
                )
            })?;
        if current.as_ref() != Some(&origin_before) {
            let _ = markers::commit_marker(state, storage, pkg, intent_nonce).await;
            return Err((
                StatusCode::CONFLICT,
                format!("Package '{pkg}' changed origin during delete"),
            ));
        }
    }

    // Tombstone a private delete BEFORE the artifact goes:
    // the filename is barred from reuse, and a crash between here and the
    // artifact delete converges to "gone" (the index rebuild already drops
    // tombstoned files) instead of resurrecting it. Mirror deletes are local
    // cache management — a cached upstream file stays re-fillable forever — so
    // they are never tombstoned. A read outage fails the delete rather than risk
    // a silent-reuse gap.
    let replicate_delete = origin_before.state == origin::OriginState::Private;
    if replicate_delete {
        tombstone::write(storage, &key, filename)
            .await
            .map_err(|e| internal("tombstone write failed", e))?;
    }

    storage
        .delete_keys(std::slice::from_ref(&key))
        .await
        .map_err(|e| internal("artifact delete failed", e))?;

    // Stop handing out the dead URL immediately (same node; peers age out).
    state.presign_cache.invalidate(&key);
    // Same for the proxy's warm-hit presence proof: without this, a re-request
    // inside PRESENCE_TTL would hit the stale "present" and serve a local 404
    // instead of re-mirroring the file from upstream (peers age out via the TTL).
    if let Some(proxy) = &state.proxy {
        proxy.invalidate_presence(&key);
    }
    // The `.origin` claim is durable on purpose: deleting every artifact must
    // not release the name for the *opposite* world to re-claim. Otherwise a
    // credentialed client could empty a mirror-owned public name and re-upload
    // it as a private package (the dependency-confusion direction). Re-purposing
    // a name from private to mirror is an operator action gated on storage
    // access — `pypiron origin release <package>` performs a checked CAS.
    let _ = storage
        .delete_keys(&[
            sidecar_key(&key),
            sidecar::metadata_key(&key),
            sidecar::provenance_key(&key),
        ])
        .await;

    // Worker confirms from truth and prunes global membership if needed.
    if let Err(e) = markers::commit_marker(state, storage, pkg, intent_nonce).await {
        warn!(error=?e, "delete: failed to write commit marker");
    }
    // A private delete carries a tombstone. Fan it out to every healthy bucket
    // before the ack; mirror cache eviction remains local and unreplicated.
    if replicate_delete {
        replicate::fanout_sync(state, pinned, pkg, filename).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Yank a file (PEP 592). The request body, if any, is the reason.
pub(crate) async fn yank_set(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let reason = body.trim().to_string();
    let yanked = if reason.is_empty() {
        Yanked::Flag(true)
    } else {
        Yanked::Reason(reason)
    };
    yank_handler(&state, &headers, &package, &filename, yanked).await
}

/// Un-yank a file.
pub(crate) async fn yank_clear(
    State(state): State<Arc<AppState>>,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    yank_handler(&state, &headers, &package, &filename, Yanked::Flag(false)).await
}

/// Yank state lives in the sidecar — it is truth, so the system can heal.
async fn yank_handler(
    state: &AppState,
    headers: &HeaderMap,
    package: &str,
    filename: &str,
    yanked: Yanked,
) -> Result<StatusCode, (StatusCode, String)> {
    require_admin(state, headers)?;
    let Some(pkg) = checked_pkg_name(package).filter(|_| valid_artifact_filename(filename)) else {
        return Err((StatusCode::NOT_FOUND, "No such file".to_string()));
    };
    let pinned = state.pin();
    set_yank(state, &pinned, &pkg, filename, yanked).await
}

/// The storage-protocol core of a yank/unyank (PEP 592): the sidecar is truth,
/// so the flip is a bounded compare-and-set loop that bumps the yank epoch on
/// every real change, pairs an intent/commit marker so the derived index heals,
/// and fans a private flip out to every healthy bucket. Split out of
/// [`yank_handler`] so a deterministic simulator can drive it without axum.
pub(crate) async fn set_yank(
    state: &AppState,
    pinned: &buckets::Pinned,
    pkg: &str,
    filename: &str,
    yanked: Yanked,
) -> Result<StatusCode, (StatusCode, String)> {
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let sc_key = sidecar_key(&key);
    let storage = pinned.storage.as_ref();

    let desired = yanked.normalized();
    let mut intent_nonce = if state.buckets.is_multi() {
        Some(markers::mark_intent(storage, pkg).await.map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to reserve yank: {e}"),
            )
        })?)
    } else {
        None
    };
    let mut wrote = false;
    let mut record_origin = None;
    for _ in 0..8 {
        let Some((bytes, etag)) = storage.get_with_etag(&sc_key).await.map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("sidecar read failed: {e}"),
            )
        })?
        else {
            return Err((StatusCode::NOT_FOUND, "No such file".to_string()));
        };
        let mut sc: Sidecar = serde_json::from_slice(&bytes).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("bad sidecar: {e}"),
            )
        })?;
        if sc.yanked.normalized() == desired {
            if let Some(nonce) = intent_nonce {
                let _ = markers::mark_commit(storage, pkg, &nonce).await;
            }
            return Ok(StatusCode::OK);
        }

        // Every real flip consumes the exact sidecar version it observed. Two
        // nodes yanking during a partition may produce equal epochs (the merge
        // has a deterministic tie-break), but two writers on one bucket cannot
        // silently lose an increment through a blind overwrite.
        sc.yank_epoch = sc.yank_epoch.saturating_add(1);
        sc.yanked = desired.clone();
        record_origin = sc.origin.clone();
        if intent_nonce.is_none() {
            intent_nonce = markers::mark_intent(storage, pkg).await.ok();
        }
        let out = serde_json::to_vec(&sc).map_err(|e| internal("encode", e))?;
        match storage.put_if_match(&sc_key, &etag, out).await {
            Ok(Some(_)) => {
                wrote = true;
                break;
            }
            Ok(None) => continue,
            Err(e) => {
                return Err(internal("write", e));
            }
        }
    }
    if !wrote {
        return Err((
            StatusCode::CONFLICT,
            "sidecar changed repeatedly; retry the yank".to_string(),
        ));
    }

    if let Err(e) = markers::commit_marker(state, storage, pkg, intent_nonce).await {
        warn!(error=?e, "yank: failed to write commit marker");
    }
    let replicate_private = if !state.buckets.is_multi() {
        false
    } else if let Some(owner) = record_origin.as_deref() {
        owner == origin::PRIVATE
    } else {
        origin::read_origin(storage, pkg)
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error reading origin for replication: {e}"),
                )
            })?
            .as_deref()
            == Some(origin::PRIVATE)
    };
    if replicate_private {
        replicate::fanout_sync(state, pinned, pkg, filename).await;
    }
    Ok(StatusCode::OK)
}

/// Set a project's PEP 792 status (admin). The body is the status doc, e.g.
/// `{"status":"quarantined","reason":"..."}`. An `active` target is a logical
/// clear, retained as an epoch-bearing event for cross-bucket convergence. This
/// is how mirror-over-HTTP `sync` relays an upstream freeze.
pub(crate) async fn project_status_set(
    State(state): State<Arc<AppState>>,
    Path(package): Path<String>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Authenticate before parsing the body — an unauthenticated caller must not
    // be able to probe well-formed vs malformed JSON (400 vs 401/403).
    require_admin(&state, &headers)?;
    let doc: status::ProjectStatusDoc = serde_json::from_str(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid status doc: {e}")))?;
    write_project_status(&state, &package, doc).await
}

/// Clear a project's status, reverting it to the default `active` (admin).
pub(crate) async fn project_status_clear(
    State(state): State<Arc<AppState>>,
    Path(package): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    write_project_status(&state, &package, status::ProjectStatusDoc::default()).await
}

/// Record a project-status event, then rebuild the index — status changes what
/// the listing renders (a quarantine serves no files). Active clears remain as
/// epoch-bearing truth so an older status on another bucket cannot resurrect.
/// The intent/commit pair keeps the derived index crash-safe.
/// Callers MUST enforce admin auth first.
async fn write_project_status(
    state: &AppState,
    package: &str,
    doc: status::ProjectStatusDoc,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(pkg) = checked_pkg_name(package) else {
        return Err((StatusCode::NOT_FOUND, "no such package".to_string()));
    };

    let pinned = state.pin();
    let storage = pinned.storage.as_ref();
    let intent_nonce = if state.buckets.is_multi() {
        Some(markers::mark_intent(storage, &pkg).await.map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to reserve status write: {e}"),
            )
        })?)
    } else {
        markers::mark_intent(storage, &pkg).await.ok()
    };
    let status_origin = if state.buckets.is_multi() {
        let observed = origin::read_origin_observation(storage, &pkg)
            .await
            .map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage error reading origin for replication: {e}"),
                )
            })?;
        observed
            .as_ref()
            .map(|value| value.state)
            .and_then(|state| replicate::Origin::try_from(state).ok())
    } else {
        None
    };
    let replicate_private = status_origin == Some(replicate::Origin::Private);
    let result = if state.buckets.is_multi() {
        status::advance_status(storage, &pkg, &doc, status_origin)
            .await
            .map(|_| ())
    } else if doc.status.is_active() {
        status::clear_status(storage, &pkg).await
    } else {
        status::write_status(storage, &pkg, &doc).await
    };
    result.map_err(|e| internal("write", e))?;

    if let Err(e) = markers::commit_marker(state, storage, &pkg, intent_nonce).await {
        warn!(error=?e, "status: failed to write commit marker");
    }
    if replicate_private {
        replicate::fanout_sync(state, &pinned, &pkg, replicate::PROJECT_STATUS_MARKER).await;
    }
    Ok(StatusCode::OK)
}

/// A filename usable as an artifact key: no path separators, not a dotfile,
/// and not a sidecar/metadata companion. The backslash guard matters on the
/// upload, delete, and yank paths alike — keep them consistent.
fn valid_artifact_filename(filename: &str) -> bool {
    !filename.contains('/') && !filename.contains('\\') && sidecar::is_artifact(filename)
}

#[cfg(test)]
mod tests {
    use crate::buckets::{BucketHandle, BucketSet};

    use super::*;

    #[tokio::test]
    async fn single_bucket_post_publish_mirror_check_is_storage_io_free() {
        let storage = Arc::new(storage::test_support::InMemStorage::default());
        // If the helper accidentally reads this malformed claim, parsing fails.
        storage.insert(&origin::origin_key("pkg"), b"not an origin claim".to_vec());
        let state = AppState::headless(storage.clone());
        let expected = origin::OriginObservation {
            state: origin::OriginState::Mirror,
            etag: "unused-single-bucket-etag".to_string(),
        };

        assert!(
            post_publish_mirror_claim_is_current(&state, storage.as_ref(), "pkg", &expected,)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn multi_bucket_post_publish_mirror_check_reads_the_exact_claim() {
        let first = Arc::new(storage::test_support::InMemStorage::default());
        let second = Arc::new(storage::test_support::InMemStorage::default());
        origin::claim_origin(first.as_ref(), "pkg", origin::MIRROR)
            .await
            .unwrap();
        let expected = origin::read_origin_observation(first.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        let mut state = AppState::headless(first.clone());
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: first.clone(),
                name: "first".to_string(),
            },
            BucketHandle {
                storage: second,
                name: "second".to_string(),
            },
        ]));

        assert!(
            post_publish_mirror_claim_is_current(&state, first.as_ref(), "pkg", &expected,)
                .await
                .unwrap()
        );
        assert!(
            origin::demote_observed_mirror(first.as_ref(), "pkg", &expected)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            !post_publish_mirror_claim_is_current(&state, first.as_ref(), "pkg", &expected,)
                .await
                .unwrap()
        );
    }
}
