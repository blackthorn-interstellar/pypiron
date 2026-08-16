//! The read path: the `/simple/` index family, artifact and companion
//! downloads, the multi-bucket visibility fences and read-through, and the
//! on-demand proxy hooks. Split out of `app.rs`; the shared response/error
//! helpers, the origin settle checks, and the `Pins` context stay in `app.rs`
//! and are imported here.

use std::sync::Arc;

use anyhow::{bail, Result};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, Method, Response, StatusCode},
};
use tracing::warn;

use crate::app::{
    if_none_match, moved_permanently, not_found, read_error, recheck_settled,
    require_settled_package_read, simple_response, unauthorized, AppState, ArtifactDelivery, Pins,
    PACKAGES_PREFIX, SIMPLE_PREFIX,
};
use crate::buckets::Pinned;
use crate::names::{checked_pkg_name, infer_version_from_filename};
use crate::sidecar::{
    frozen_key, mirror_quarantined_key, sidecar_key, tombstone_key, Sidecar, METADATA_SUFFIX,
    PROVENANCE_SUFFIX,
};
use crate::storage::Storage;
use crate::{cache, names, origin, proxy, render, sidecar, storage};

/// User-Agent prefixes of clients whose artifact caches are keyed by package
/// filename rather than the URL that served the bytes, verified to follow
/// cross-host 302s. Only such clients may be redirected in `auto` mode —
/// anyone else (pip's CacheControl keys on the per-hop URL; unknown tools are
/// assumed to as well) gets streamed bytes under the stable `/files/` URL.
/// Grow this list by verified cache behavior, not by client popularity.
const REDIRECT_SAFE_UA_PREFIXES: &[&str] = &["uv/"];

fn redirect_safe_client(headers: &HeaderMap) -> bool {
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    REDIRECT_SAFE_UA_PREFIXES.iter().any(|p| ua.starts_with(p))
}

/// --- Simple index endpoints ----------------------------------------------
const CT_JSON: &str = render::SIMPLE_JSON_CONTENT_TYPE;
const CT_HTML: &str = render::SIMPLE_HTML_CONTENT_TYPE;
/// Indexes change on every rebuild: always revalidate, never stale.
const INDEX_CACHE_CONTROL: &str = "no-cache";
/// Filenames are immutable, so artifact bytes can be cached forever.
const ARTIFACT_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

/// Multi-bucket markers are visibility fences, not merely index hints. A
/// quarantined mirror body deliberately keeps its canonical key occupied, so
/// direct and presigned downloads must reject it too. A stale quarantine marker
/// becomes inert only after a private sidecar proves private precedence won.
async fn multi_bucket_file_visible(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_key: &str,
) -> Result<bool> {
    if !state.buckets.is_multi() {
        return Ok(true);
    }
    let Some(claim) = require_settled_package_read(state, storage, pkg).await? else {
        return Ok(false);
    };
    if claim.state == origin::OriginState::Unclaimed {
        return Ok(false);
    }
    let ((tombstoned, frozen), mirror_quarantined) = futures::future::try_join(
        futures::future::try_join(
            storage.head_exists(&tombstone_key(artifact_key)),
            storage.head_exists(&frozen_key(artifact_key)),
        ),
        storage.head_exists(&mirror_quarantined_key(artifact_key)),
    )
    .await?;
    if tombstoned || frozen {
        return Ok(false);
    }

    if mirror_quarantined || claim.state == origin::OriginState::Private {
        let sidecar = match storage.get_bytes(&sidecar_key(artifact_key)).await {
            Ok(bytes) => Some(serde_json::from_slice::<Sidecar>(&bytes)?),
            Err(error) if storage::is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        if mirror_quarantined
            && sidecar.as_ref().and_then(|value| value.origin.as_deref()) != Some(origin::PRIVATE)
        {
            return Ok(false);
        }
        if claim.state == origin::OriginState::Private
            && sidecar.as_ref().and_then(|value| value.origin.as_deref()) == Some(origin::MIRROR)
        {
            return Ok(false);
        }
    }
    if origin::read_origin_observation(storage, pkg)
        .await?
        .as_ref()
        != Some(&claim)
    {
        bail!("package '{pkg}' changed while checking artifact visibility");
    }
    Ok(true)
}

/// An unclaimed proxy companion may bypass local storage only when the package
/// has no local body, companion, or permanent visibility fence. The exact
/// claim recheck closes the LIST/HEAD window before the upstream fetch begins.
async fn unowned_companion_passthrough_safe(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_key: &str,
    companion_key: &str,
) -> Result<bool> {
    if !state.buckets.is_multi() {
        return Ok(true);
    }
    let before = require_settled_package_read(state, storage, pkg).await?;
    if before
        .as_ref()
        .is_some_and(|claim| claim.state != origin::OriginState::Unclaimed)
    {
        return Ok(false);
    }
    for key in [
        artifact_key,
        companion_key,
        &tombstone_key(artifact_key),
        &frozen_key(artifact_key),
        &mirror_quarantined_key(artifact_key),
    ] {
        if storage.head_exists(key).await? {
            return Ok(false);
        }
    }
    Ok(require_settled_package_read(state, storage, pkg).await? == before)
}

async fn companion_passthrough_visible(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_key: &str,
    companion_key: &str,
    expected_claim: &Option<origin::OriginObservation>,
) -> Result<bool> {
    let visible = if multi_bucket_file_visible(state, storage, pkg, artifact_key).await? {
        true
    } else {
        unowned_companion_passthrough_safe(state, storage, pkg, artifact_key, companion_key).await?
    };
    if !visible || !state.buckets.is_multi() {
        return Ok(visible);
    }
    Ok(require_settled_package_read(state, storage, pkg).await? == *expected_claim)
}

/// Visibility fence with read-through. Presence on the read pin is trusted; a
/// negative read-pin result is re-checked on the write pin before it can 404 (a
/// lagging region bucket must never make a client miss an acked file). The write
/// pin is authoritative — deletions and tombstones serialize there — so its
/// answer is final. Identical to a single [`multi_bucket_file_visible`] when the
/// two pins are the same bucket.
async fn file_visible_read_through(
    state: &AppState,
    pins: &Pins<'_>,
    pkg: &str,
    artifact_key: &str,
) -> Result<bool> {
    // A positive regional observation is not authoritative. In particular, a
    // lagging region may still describe a public mirror package after the write
    // home has claimed that normalized name privately. Only trust regional
    // presence while both pins agree on the complete origin observation.
    let read_claim = require_settled_package_read(state, pins.read.storage.as_ref(), pkg).await?;
    let write_claim = if pins.same_pin {
        read_claim.clone()
    } else {
        require_settled_package_read(state, pins.write.storage.as_ref(), pkg).await?
    };
    if read_claim == write_claim
        && multi_bucket_file_visible(state, pins.read.storage.as_ref(), pkg, artifact_key).await?
    {
        return Ok(true);
    }
    if pins.same_pin {
        return Ok(false);
    }
    multi_bucket_file_visible(state, pins.write.storage.as_ref(), pkg, artifact_key).await
}

async fn read_copy_is_authoritative(
    state: &AppState,
    pins: &Pins<'_>,
    pkg: &str,
    artifact_key: &str,
) -> Result<bool> {
    if pins.same_pin {
        return Ok(true);
    }
    let (read, write) = futures::future::try_join(
        require_settled_package_read(state, pins.read.storage.as_ref(), pkg),
        require_settled_package_read(state, pins.write.storage.as_ref(), pkg),
    )
    .await?;
    Ok(read == write
        && multi_bucket_file_visible(state, pins.read.storage.as_ref(), pkg, artifact_key).await?)
}

pub(crate) async fn simple_root(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_root_index(&state, IndexFormat::negotiated(&headers), &headers).await
}

pub(crate) async fn simple_root_json(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_root_index(&state, IndexFormat::Json, &headers).await
}

/// Which representation of a `/simple/` index to serve: PEP 691 JSON or the
/// legacy HTML. An explicit `…/index.json` route forces `Json`; a bare route
/// carries the format it declares (`Html`) and content-negotiates up from there.
#[derive(Clone, Copy, PartialEq, Eq)]
enum IndexFormat {
    Html,
    Json,
}

impl IndexFormat {
    fn is_json(self) -> bool {
        matches!(self, IndexFormat::Json)
    }

    /// The content-negotiated format for a route that didn't force one.
    fn negotiated(headers: &HeaderMap) -> Self {
        if accepts_json(headers) {
            IndexFormat::Json
        } else {
            IndexFormat::Html
        }
    }
}

/// The global `/simple/` index, in JSON or HTML.
async fn serve_root_index(
    state: &AppState,
    format: IndexFormat,
    headers: &HeaderMap,
) -> Response<Body> {
    if !state.is_reader(headers) {
        return unauthorized();
    }
    let pinned = state.pin();
    let (key, ct) = if format.is_json() {
        (format!("{SIMPLE_PREFIX}index.json"), CT_JSON)
    } else {
        (format!("{SIMPLE_PREFIX}index.html"), CT_HTML)
    };
    serve_index(state, &pinned, key, ct, INDEX_CACHE_CONTROL, headers).await
}

pub(crate) async fn simple_pkg(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_pkg_index(&state, &raw, IndexFormat::Html, &headers).await
}

pub(crate) async fn simple_pkg_json(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    serve_pkg_index(&state, &raw, IndexFormat::Json, &headers).await
}

/// A package's `/simple/<pkg>/` page. `force_json` is the explicit-`index.json`
/// route (otherwise the representation is content-negotiated); it also pins the
/// canonical-redirect target so URL-keyed caches never split entries.
async fn serve_pkg_index(
    state: &AppState,
    raw: &str,
    requested: IndexFormat,
    headers: &HeaderMap,
) -> Response<Body> {
    if !state.is_reader(headers) {
        return unauthorized();
    }
    let Some(pkg) = checked_pkg_name(raw) else {
        return not_found("invalid package name");
    };
    // PEP 503: the canonical URL is the normalized one; everything else 301s
    // there, so URL-keyed caches (CDNs, edge proxies) never split entries.
    if raw != pkg {
        let target = if requested.is_json() {
            format!("/simple/{pkg}/index.json")
        } else {
            format!("/simple/{pkg}/")
        };
        return moved_permanently(&target);
    }
    // Reads come from the region-local pin, and the origin claim that gates
    // serving is observed on whichever bucket actually serves the bytes — so a
    // region catch-up landing the `.origin` mid-serve can never trip the
    // coherence recheck. Any decision that could reach upstream — the proxy
    // index, and denying an "unclaimed" name — is settled on the write pin
    //.
    let read_pinned = state.read_pin();
    let write_pinned = state.pin();
    let pins = Pins::new(&read_pinned, &write_pinned);
    let format = if requested.is_json() {
        IndexFormat::Json
    } else {
        IndexFormat::negotiated(headers)
    };
    let (key, ct) = if format.is_json() {
        (format!("{SIMPLE_PREFIX}{pkg}/index.json"), CT_JSON)
    } else {
        (format!("{SIMPLE_PREFIX}{pkg}/index.html"), CT_HTML)
    };

    // Upstream (proxy) path: eligibility, render, and the mid-serve coherence
    // recheck all run on the write pin inside `proxy_package_index`.
    if let Some(resp) =
        proxy_package_index(state, write_pinned.storage.as_ref(), &pkg, format, headers).await
    {
        return resp;
    }

    // Local index. In multi-bucket, an "unclaimed" read-pin observation is
    // confirmed on the write pin before denying the name; a write-owned claim the
    // region bucket has not seen yet is served through from the write home.
    if state.buckets.is_multi() {
        let read_claim =
            match require_settled_package_read(state, read_pinned.storage.as_ref(), &pkg).await {
                Ok(claim) => claim,
                Err(error) => return read_error(error),
            };
        if read_claim
            .as_ref()
            .is_none_or(|value| value.state == origin::OriginState::Unclaimed)
        {
            match unclaimed_confirmed_absent(state, &pins, &pkg).await {
                Ok(true) => return not_found("no such package"),
                Ok(false) => {}
                Err(error) => return read_error(error),
            }
        }
        return serve_local_index_fenced(state, &pins, &pkg, key, ct, headers, read_claim).await;
    }
    serve_local_index_fenced(state, &pins, &pkg, key, ct, headers, None).await
}

/// Serve a package index from the read pin, reading through to the write home on
/// a miss — with the origin coherence recheck pinned to whichever bucket actually
/// serves the bytes. Served locally from the read pin → the pre-observation
/// (`read_baseline`, already read by the caller) and the recheck are the read
/// pin's; served through from the write home → both are the write pin's, so a
/// region catch-up mid-serve never 503s.
async fn serve_local_index_fenced(
    state: &AppState,
    pins: &Pins<'_>,
    pkg: &str,
    key: String,
    content_type: &'static str,
    headers: &HeaderMap,
    read_baseline: Option<origin::OriginObservation>,
) -> Response<Body> {
    // Presence is useful for locality, but the write home owns origin. Refuse
    // to serve a regional index when its claim is stale in either direction;
    // reading through also covers the normal replication-lag case.
    if !pins.same_pin {
        let write_baseline =
            match require_settled_package_read(state, pins.write.storage.as_ref(), pkg).await {
                Ok(claim) => claim,
                Err(error) => return read_error(error),
            };
        if read_baseline != write_baseline {
            if write_baseline
                .as_ref()
                .is_none_or(|claim| claim.state == origin::OriginState::Unclaimed)
            {
                return not_found("no such package");
            }
            let resp = serve_index_uncached(
                pins.write.storage.as_ref(),
                &key,
                content_type,
                INDEX_CACHE_CONTROL,
                headers,
            )
            .await;
            if let Some(resp) = recheck_settled(
                state,
                pins.write.storage.as_ref(),
                pkg,
                &write_baseline,
                "serving its index",
            )
            .await
            {
                return resp;
            }
            return resp;
        }
    }
    let resp = serve_index(
        state,
        pins.read,
        key.clone(),
        content_type,
        INDEX_CACHE_CONTROL,
        headers,
    )
    .await;
    if resp.status() != StatusCode::NOT_FOUND {
        // Served from the read pin: recheck the read pin against its baseline.
        if state.buckets.is_multi() {
            if let Some(resp) = recheck_settled(
                state,
                pins.read.storage.as_ref(),
                pkg,
                &read_baseline,
                "serving its index",
            )
            .await
            {
                return resp;
            }
            if !pins.same_pin {
                if let Some(resp) = recheck_settled(
                    state,
                    pins.write.storage.as_ref(),
                    pkg,
                    &read_baseline,
                    "serving its regional index",
                )
                .await
                {
                    return resp;
                }
            }
        }
        return resp;
    }
    if pins.same_pin {
        return resp; // 404 on the only bucket; nothing to read through to.
    }
    // Read-through to the write home: baseline and recheck are both the write
    // pin's, so a repair sweep landing the region bucket's `.origin` never trips.
    let write_baseline =
        match require_settled_package_read(state, pins.write.storage.as_ref(), pkg).await {
            Ok(claim) => claim,
            Err(error) => return read_error(error),
        };
    let resp = serve_index_uncached(
        pins.write.storage.as_ref(),
        &key,
        content_type,
        INDEX_CACHE_CONTROL,
        headers,
    )
    .await;
    if let Some(resp) = recheck_settled(
        state,
        pins.write.storage.as_ref(),
        pkg,
        &write_baseline,
        "serving its index",
    )
    .await
    {
        return resp;
    }
    resp
}

/// Confirm a read-pin "no claim" against the write pin before it can deny a
/// package. Returns true when the write pin also holds no real claim (a genuine
/// 404), false when the write home owns a claim the region bucket has not yet
/// seen (serve it through). Fail-closed on names.
async fn unclaimed_confirmed_absent(state: &AppState, pins: &Pins<'_>, pkg: &str) -> Result<bool> {
    if pins.same_pin {
        return Ok(true);
    }
    Ok(
        require_settled_package_read(state, pins.write.storage.as_ref(), pkg)
            .await?
            .is_none_or(|value| value.state == origin::OriginState::Unclaimed),
    )
}

/// Resolve the proxy for `pkg`, enforcing the eligibility gate (the
/// dependency-confusion defense) in one place. `None` = no proxy configured or
/// the name is ineligible (private / reserved prefix), so fall through to local
/// serving; `Some(Err)` = origin unreadable, an outage to surface rather than
/// answer "who owns this name" optimistically; `Some(Ok)` = serve upstream.
/// Whether the on-demand proxy should answer for `pkg`. `FallThrough`: serve the
/// local index instead (proxy off or the name ineligible); `Serve`: the eligible
/// upstream to fetch from; `Deny`: an origin-read outage, return this error
/// rather than answer a "who owns this name" question optimistically.
enum ProxyDecision<'a> {
    FallThrough,
    Serve(&'a Arc<proxy::Proxy>),
    Deny(Response<Body>),
}

async fn eligible_proxy<'a>(
    state: &'a AppState,
    storage: &dyn Storage,
    pkg: &str,
) -> ProxyDecision<'a> {
    let Some(proxy) = state.proxy.as_ref() else {
        return ProxyDecision::FallThrough;
    };
    match proxy::eligible(state, storage, pkg).await {
        Ok(true) => ProxyDecision::Serve(proxy),
        Ok(false) => ProxyDecision::FallThrough,
        Err(e) => ProxyDecision::Deny(read_error(e)),
    }
}

/// Proxy hook for package pages: `Some(response)` when the page is served
/// from upstream metadata, `None` to fall through to the local materialized
/// index (proxy off, package ineligible, or upstream unavailable).
async fn proxy_package_index(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    format: IndexFormat,
    headers: &HeaderMap,
) -> Option<Response<Body>> {
    let proxy = state.proxy.as_ref()?;
    // Coherence baseline on this (write) pin, taken before the eligibility read so
    // the fence covers the whole eligible→render span: serving an upstream index
    // for a name that gains a local claim mid-serve would be a dependency-confusion
    // leak, so the origin is rechecked on the same pin before the page is returned.
    // Free in single-bucket mode (no I/O) and skipped by the recheck below.
    let before = match require_settled_package_read(state, storage, pkg).await {
        Ok(claim) => claim,
        Err(e) => return Some(read_error(e)),
    };
    match proxy::eligible(state, storage, pkg).await {
        Ok(true) => {}
        Ok(false) => return None,
        // Origin unreadable is an outage: never answer "what owns this name"
        // questions optimistically (the dependency-confusion direction).
        Err(e) => return Some(read_error(e)),
    }
    let rendered = match proxy
        .package_index(state, storage, pkg, format.is_json())
        .await
    {
        Ok(Some(rendered)) => rendered,
        Ok(None) => return None,
        Err(error) => return Some(read_error(error)),
    };
    let revalidated = if_none_match(headers, &[&*rendered.etag]);
    let builder = Response::builder()
        .header(header::ETAG, &*rendered.etag)
        .header(header::CACHE_CONTROL, INDEX_CACHE_CONTROL);
    let response = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        builder
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                if format.is_json() { CT_JSON } else { CT_HTML },
            )
            .header(header::CONTENT_LENGTH, rendered.body.len())
            .body(Body::from(rendered.body.clone()))
    };
    if state.buckets.is_multi() {
        if let Some(resp) = recheck_settled(state, storage, pkg, &before, "serving its index").await
        {
            return Some(resp);
        }
    }
    Some(response.unwrap_or_else(not_found))
}

/// Serve a materialized index file with a content-hash ETag; conditional GETs
/// revalidate to 304. Bytes and ETag come from the in-memory cache — the hot
/// path costs zero storage calls and zero hashing (see cache.rs).
async fn serve_index(
    state: &AppState,
    pinned: &Pinned,
    key: String,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    let (identity, gzip) = match state
        .index_cache
        .get(pinned.storage.as_ref(), &key, pinned.generation)
        .await
    {
        Ok(Some(hit)) => hit,
        Ok(None) => return not_found("no such index"),
        Err(e) => return read_error(e),
    };
    render_index_variant(&identity, &gzip, content_type, cache_control, headers)
}

/// Serve `key` straight from a storage handle without touching the read-pin
/// index cache — the read-through fallback for a page missing on the region
/// bucket. Bounded to the rare region-lag case; the hot path stays the cached
/// read-pin [`serve_index`].
async fn serve_index_uncached(
    storage: &dyn Storage,
    key: &str,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    match storage.get_bytes(key).await {
        Ok(bytes) => {
            let (identity, gzip) = cache::build_variants(bytes);
            render_index_variant(&identity, &gzip, content_type, cache_control, headers)
        }
        Err(e) if storage::is_not_found(&e) => not_found("no such index"),
        Err(e) => read_error(e),
    }
}

/// Serve an index/companion from the read pin, reading through to the write pin
/// on a miss so a lagging region bucket never 404s a page the write home holds
///. Identical to [`serve_index`] when the two pins
/// are the same bucket. The write-pin fallback renders uncached, keeping package
/// keys populated only from the read pin.
async fn serve_index_local(
    state: &AppState,
    pins: &Pins<'_>,
    key: String,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    let resp = serve_index(
        state,
        pins.read,
        key.clone(),
        content_type,
        cache_control,
        headers,
    )
    .await;
    if pins.same_pin || resp.status() != StatusCode::NOT_FOUND {
        return resp;
    }
    serve_index_uncached(
        pins.write.storage.as_ref(),
        &key,
        content_type,
        cache_control,
        headers,
    )
    .await
}

/// Render one cached index representation, negotiating gzip and conditional GETs.
fn render_index_variant(
    identity: &cache::Variant,
    gzip: &Option<cache::Variant>,
    content_type: &'static str,
    cache_control: &'static str,
    headers: &HeaderMap,
) -> Response<Body> {
    // Content negotiation against the precompressed variant: zero per-request
    // CPU — big indexes were NIC-bound, and gzip is a ~5-7x cut in bytes.
    // Each representation carries its own strong ETag (hence Vary).
    let accepts_gzip = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("gzip"))
        .unwrap_or(false);
    let (variant, encoding) = match (gzip, accepts_gzip) {
        (Some(gz), true) => (gz, Some("gzip")),
        _ => (identity, None),
    };

    let revalidated = if_none_match(headers, &[&*variant.etag, &*identity.etag]);

    let mut builder = Response::builder()
        .header(header::ETAG, &*variant.etag)
        .header(header::VARY, "Accept-Encoding")
        .header(header::CACHE_CONTROL, cache_control);
    if let Some(enc) = encoding {
        builder = builder.header(header::CONTENT_ENCODING, enc);
    }

    let result = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        builder
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, variant.body.len())
            // Bytes clone = refcount bump; hyper streams the shared buffer.
            .body(Body::from(variant.body.clone()))
    };
    result.unwrap_or_else(not_found)
}

/// Serve an artifact's PEP 658 metadata or PEP 740 provenance companion: a
/// RAM-cached read-through with the multi-bucket fence check, then upstream
/// passthrough when the wheel isn't cached yet. Metadata and provenance differ
/// only in `companion` (suffix, content type, upstream fetch).
async fn serve_companion(
    state: &AppState,
    pins: &Pins<'_>,
    pkg: &str,
    filename: &str,
    headers: &HeaderMap,
    companion: Companion,
) -> Response<Body> {
    // The companion (`<file>.metadata` / `.provenance`) and the artifact it
    // annotates, both keyed the same way the caller keys them — derived here
    // from `(pkg, filename)` so this signature carries only what varies.
    let artifact_filename = filename
        .strip_suffix(companion.suffix())
        .unwrap_or(filename);
    let artifact_key = format!("{PACKAGES_PREFIX}{pkg}/{artifact_filename}");
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    match file_visible_read_through(state, pins, pkg, &artifact_key).await {
        Ok(true) => {}
        Ok(false) => {
            // Upstream passthrough is judged on the write pin: an unclaimed
            // origin must be authoritative before any fall-through.
            match unowned_companion_passthrough_safe(
                state,
                pins.write.storage.as_ref(),
                pkg,
                &artifact_key,
                &key,
            )
            .await
            {
                Ok(true) => {
                    if let Some(upstream) = proxy_companion_passthrough(
                        state,
                        pins.write.storage.as_ref(),
                        pkg,
                        filename,
                        companion,
                    )
                    .await
                    {
                        return upstream;
                    }
                }
                Ok(false) => {}
                Err(error) => return read_error(error),
            }
            return not_found("artifact is fenced");
        }
        Err(error) => return read_error(error),
    }
    let resp = serve_index_local(
        state,
        pins,
        key,
        companion.content_type(),
        ARTIFACT_CACHE_CONTROL,
        headers,
    )
    .await;
    // Not stored yet (wheel not cached): pass upstream companion bytes through
    // without writing anything — a resolver probing dozens of candidate wheels
    // must not stampede gigabytes into storage. The companion is stored when the
    // wheel itself is downloaded.
    if resp.status() == StatusCode::NOT_FOUND {
        if let Some(upstream) = proxy_companion_passthrough(
            state,
            pins.write.storage.as_ref(),
            pkg,
            filename,
            companion,
        )
        .await
        {
            return upstream;
        }
    }
    resp
}

/// --- Artifact download endpoint ------------------------------------------
/// Serves artifacts and their PEP 658 `<filename>.metadata` companions; both
/// are immutable. Sidecar JSON and dotfiles are not served.
pub(crate) async fn files_get(
    State(state): State<Arc<AppState>>,
    method: Method,
    Path((package, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    // A request is for an artifact or one of its served companions
    // (`.metadata`, `.provenance`); the sidecar JSON and dotfiles never serve.
    let servable = match filename
        .strip_suffix(METADATA_SUFFIX)
        .or_else(|| filename.strip_suffix(PROVENANCE_SUFFIX))
    {
        Some(base) => sidecar::is_artifact(base),
        None => sidecar::is_artifact(&filename),
    };
    let Some(pkg) = checked_pkg_name(&package)
        .filter(|_| servable && !filename.contains('/') && !filename.contains('\\'))
    else {
        return not_found("not an artifact");
    };

    // Pin both selections once for the whole download (design §3). Reads —
    // fences, companion cache, presign, streaming — run against the region-local
    // read pin; the proxy fill and every upstream-claim decision run against the
    // write pin (bytes from the near bucket, judgment from the write home).
    // In the common mode the two are one context.
    let read_pinned = state.read_pin();
    let write_pinned = state.pin();
    let pins = Pins::new(&read_pinned, &write_pinned);

    // Download attribution key, computed once: a real artifact only (companions
    // and the ranged-companion fall-through below parse to None), keyed
    // `<pkg>/<filename>` so the counter store rolls files up to versions. Counted
    // at the two delivery exits (302 redirect, 200 stream) — see counters.rs. A
    // HEAD transfers no body (axum routes it to this GET handler), so it is not a
    // download: gate on GET so a bodiless probe never inflates the count.
    let dl_key = (method == Method::GET && sidecar::is_artifact(&filename))
        .then(|| format!("{pkg}/{filename}"));

    // PEP 658 metadata and PEP 740 provenance companions are immutable, tiny,
    // and hammered by resolvers (uv fetches one per candidate wheel) — served
    // from the same RAM cache as the indexes, falling through to upstream
    // passthrough when the wheel isn't cached yet. Range requests fall through
    // to storage; nobody range-reads a companion.
    if filename.ends_with(METADATA_SUFFIX) && headers.get(header::RANGE).is_none() {
        return serve_companion(
            &state,
            &pins,
            &pkg,
            &filename,
            &headers,
            Companion::Metadata,
        )
        .await;
    }
    if filename.ends_with(PROVENANCE_SUFFIX) && headers.get(header::RANGE).is_none() {
        return serve_companion(
            &state,
            &pins,
            &pkg,
            &filename,
            &headers,
            Companion::Provenance,
        )
        .await;
    }

    // Past the companion exits: this is a real artifact request. Its storage key
    // and the artifact it resolves to (identical here, since a bare artifact has
    // no companion suffix to strip) are only needed on this path.
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    let artifact_filename = filename
        .strip_suffix(METADATA_SUFFIX)
        .or_else(|| filename.strip_suffix(PROVENANCE_SUFFIX))
        .unwrap_or(&filename);
    let artifact_key = format!("{PACKAGES_PREFIX}{pkg}/{artifact_filename}");

    // On-demand mirroring: make sure the artifact is in storage before the
    // presign/stream logic runs (a presigned redirect never observes a 404,
    // so the fetch can't be triggered by one). The fill runs entirely on the
    // write pin — origin claims and the 409-serialized PUT stay on the write home.
    match proxy_ensure_artifact(
        &state,
        write_pinned.storage.as_ref(),
        &pkg,
        &filename,
        write_pinned.generation,
    )
    .await
    {
        ProxyEnsure::Fail(resp) => return resp,
        ProxyEnsure::Serve { filled } => {
            if filled {
                // Off the request's critical path: a fresh fill just committed to
                // the write bucket, so replicate it to peers asynchronously. The
                // response is served without waiting on the per-peer notes.
                crate::replicate::spawn_proxy_fill_notes(
                    state.clone(),
                    write_pinned.index,
                    pkg.to_string(),
                    filename.clone(),
                );
            }
        }
    }
    // Malware byte gate: the single enforcement chokepoint, before the
    // presign/stream split so a cached signed URL is gated too. Origin is judged
    // on the write home. A no-op unless blocking is armed and a snapshot is fed.
    if let Some(resp) = advisory_byte_gate(
        &state,
        write_pinned.storage.as_ref(),
        &pkg,
        artifact_filename,
    )
    .await
    {
        return resp;
    }
    match file_visible_read_through(&state, &pins, &pkg, &artifact_key).await {
        Ok(true) => {}
        Ok(false) => return not_found("artifact is fenced"),
        Err(error) => return read_error(error),
    }
    let use_read_pin = match read_copy_is_authoritative(&state, &pins, &pkg, &artifact_key).await {
        Ok(matches) => matches,
        Err(error) => return read_error(error),
    };

    // S3 serves the megabytes, this node serves kilobytes of index: redirect
    // artifact downloads to a presigned URL — but only for clients whose
    // caches survive URL churn (see ArtifactDelivery). Metadata companions
    // are tiny and resolution-critical, so they always stream. The redirect
    // itself must not be cached — the signature expires.
    let redirect = match state.artifact_delivery {
        ArtifactDelivery::Stream => false,
        ArtifactDelivery::Redirect => true,
        ArtifactDelivery::Auto => redirect_safe_client(&headers),
    };
    if redirect && !filename.ends_with(METADATA_SUFFIX) && !filename.ends_with(PROVENANCE_SUFFIX) {
        // No artifact-existence check: presigning itself is local HMAC math.
        // Multi-bucket mode already paid its origin/marker visibility reads
        // above; single-bucket mode still adds no storage round trip here. A
        // signed URL to a missing key gets S3's own 404 (the server's
        // credentials carry s3:ListBucket —
        // required for index rebuilds — which is what makes S3 say 404
        // rather than 403). Existence is the index's job, not this path's.
        // Immutability also makes signed URLs reusable across clients: serve
        // a cached one while it has plenty of validity left (see cache.rs).
        // The presign cache is keyed by the (shared) read-pin generation and
        // populated only from this read-pin-routed path.
        if use_read_pin {
            if let Some(url) = state.presign_cache.fresh(&key, read_pinned.generation) {
                if let Some(k) = &dl_key {
                    state.counters.record("downloads", k);
                    state.metrics.record_download();
                }
                return found_redirect(&url);
            }
        }
        // Presign the bucket that actually holds the bytes: the read pin when the
        // object is present there, otherwise the write pin — never hand out a URL
        // that will 404. The HEAD is skipped when the two pins are one bucket, so
        // single-region and no-affinity nodes add no round trip here.
        let presign_storage = if !use_read_pin {
            write_pinned.storage.clone()
        } else if pins.same_pin {
            read_pinned.storage.clone()
        } else {
            match read_pinned.storage.head_exists(&key).await {
                Ok(true) => read_pinned.storage.clone(),
                Ok(false) => write_pinned.storage.clone(),
                Err(e) => {
                    warn!(error=?e, %key, "read-pin existence check failed; presigning the write pin");
                    write_pinned.storage.clone()
                }
            }
        };
        match presign_storage
            .presign_get(&key, cache::PRESIGN_EXPIRY)
            .await
        {
            Ok(Some(url)) => {
                let url: Arc<str> = url.into();
                if use_read_pin {
                    state
                        .presign_cache
                        .put(&key, url.clone(), read_pinned.generation);
                }
                if let Some(k) = &dl_key {
                    state.counters.record("downloads", k);
                    state.metrics.record_download();
                }
                return found_redirect(&url);
            }
            Ok(None) => {} // disk backend: fall through to streaming
            Err(e) => warn!(error=?e, %key, "presign failed; falling back to streaming"),
        }
    }

    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    // Stream from the read pin; on any failure (a not-found on a lagging region
    // bucket, or an error) read through to the write pin once before mapping to
    // 404/503.
    let primary = if use_read_pin {
        &read_pinned.storage
    } else {
        &write_pinned.storage
    };
    let mut resp = match primary.serve_artifact(&key, range).await {
        Ok(resp) => resp,
        Err(read_err) => {
            if pins.same_pin || !use_read_pin {
                return read_error(read_err);
            }
            match write_pinned.storage.serve_artifact(&key, range).await {
                Ok(resp) => resp,
                Err(e) => return read_error(e),
            }
        }
    };
    // Count only a full delivered body (200): a 206 range read is a partial of
    // one logical download, a 416 is none. (A whole-file range served as 206 —
    // rare, e.g. `curl -C-`/`wget -c` — is undercounted; download stats are
    // best-effort, so we don't parse Content-Range.)
    if resp.status() == StatusCode::OK {
        if let Some(k) = &dl_key {
            state.counters.record("downloads", k);
            state.metrics.record_download();
        }
    }
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(ARTIFACT_CACHE_CONTROL),
    );
    resp
}

/// A `403 application/json` refusal with no-store caching — the malware byte
/// gate's response shape ([`json_response`]'s 403 sibling; that one is 200-only).
fn blocked_response(value: serde_json::Value) -> Response<Body> {
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    simple_response(StatusCode::FORBIDDEN, "application/json", "no-store", bytes)
}

/// The malware byte gate: the single enforcement chokepoint where advisory-blocked
/// bytes are refused. Runs once in [`files_get`] before the presign/stream split,
/// so a cached signed URL is gated too. `Some(403)` refuses; `None` allows.
///
/// The common path is a pure hash probe with zero I/O: disabled, unfed, or no hit
/// all return `None` before any storage read. Only a genuine advisory/quarantine
/// hit pays the origin read that proves the name isn't private — OSV names live in
/// PyPI's namespace, and origin exclusivity is the proof that a same-named private
/// package is not that package. `storage` is the write-pin (origin claims are
/// judged on the write home). Fail-closed throughout: an unclaimed/mirror origin
/// or a storage error on a hit blocks.
async fn advisory_byte_gate(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    artifact_filename: &str,
) -> Option<Response<Body>> {
    let snap = state.advisory_snapshot();
    // Two independent protections share this chokepoint:
    //   * PEP 792 quarantine — a project whose upstream status blocks downloads.
    //     Enforced whenever a quarantine set is loaded, INDEPENDENT of the malware
    //     toggle: `--malware-block=false` disables OSV blocking, not quarantine.
    //   * OSV MAL-* advisory blocking — gated by `--malware-block`.
    let quarantined = snap.quarantined.contains(pkg);
    let version = infer_version_from_filename(artifact_filename);
    let ids: Vec<String> = if state.malware_block {
        // Baseline block set ∪ the per-node probe overlay.
        snap.blocking(pkg, version.as_deref())
    } else {
        Vec::new()
    };
    if ids.is_empty() && !quarantined {
        return None; // the common path: no origin read, no I/O
    }

    // A hit — but a private-origin name never consults either set. Fast pre-check
    // on the configured private prefix (no I/O), then the authoritative claim.
    if let Some(prefix) = &state.private_prefix {
        if names::matches_prefix(pkg, prefix) {
            return None;
        }
    }
    match origin::read_origin_claim(storage, pkg).await {
        Ok(Some(origin::OriginState::Private)) => return None,
        // Mirror, the unclaimed sentinel, or no claim at all: not proven private,
        // so a same-named blocked artifact is refused (fail-closed).
        Ok(_) => {}
        Err(e) => {
            // Only reachable on a probe hit; a storage read error fails closed.
            warn!(error = ?e, %pkg, "advisory gate: origin read failed; blocking fail-closed");
        }
    }

    state.metrics.record_blocked_download();
    if ids.is_empty() {
        warn!(%pkg, "blocked download: project quarantined upstream");
        return Some(blocked_response(serde_json::json!({
            "error": "project quarantined upstream",
            "package": pkg,
        })));
    }
    warn!(%pkg, version = ?version, advisories = ?ids, "blocked download: malware advisory");
    Some(blocked_response(serde_json::json!({
        "error": "blocked by malware advisory",
        "package": pkg,
        "version": version,
        "advisories": ids,
    })))
}

/// Outcome of the proxy artifact hook. `Fail` is a hard failure response
/// (storage outage, upstream verification failure). `Serve` falls through to
/// normal serving; `filled` says THIS request committed a fresh cache fill, so
/// the caller schedules the off-request-path peer replication notes.
enum ProxyEnsure {
    Serve { filled: bool },
    Fail(Response<Body>),
}

/// Proxy hook for artifact downloads: fetch-and-commit on a local miss.
async fn proxy_ensure_artifact(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    generation: u64,
) -> ProxyEnsure {
    let Some(proxy) = state.proxy.as_ref() else {
        return ProxyEnsure::Serve { filled: false };
    };
    let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    // Warm-hit fast path: an artifact already in local storage is always safe to
    // serve as-is. The origin/eligibility fence exists to gate *upstream fetches*,
    // not local delivery, so a cached file skips it entirely — and skips the
    // origin read it would cost — and falls through to normal serving. This is the
    // whole optimization: a warm proxied download drops from three storage ops
    // (origin read + existence HEAD + serve) to one (serve).
    match proxy
        .artifact_cached_locally(storage, &key, generation)
        .await
    {
        Ok(true) => return ProxyEnsure::Serve { filled: false },
        Ok(false) => {}
        Err(e) => return ProxyEnsure::Fail(read_error(e)),
    }
    // Local miss: the full fence applies before any upstream contact. A private or
    // out-of-scope name stops here and never reaches upstream. `eligible` has no
    // side effects, so gating it behind the existence check changes nothing for a
    // name that does fall through — it just no longer pays on the warm path.
    match proxy::eligible(state, storage, pkg).await {
        Ok(true) => {}
        Ok(false) => return ProxyEnsure::Serve { filled: false },
        Err(e) => return ProxyEnsure::Fail(read_error(e)),
    }
    match proxy
        .ensure_artifact_cached(state, storage, pkg, filename)
        .await
    {
        Ok(filled) => ProxyEnsure::Serve { filled },
        Err(e) => ProxyEnsure::Fail(read_error(e)),
    }
}

/// Which sidecar companion of an artifact is being served. Metadata (PEP 658)
/// and provenance (PEP 740) follow identical fence, passthrough, and caching
/// rules; only the suffix, upstream fetch, and content type differ.
#[derive(Clone, Copy)]
enum Companion {
    Metadata,
    Provenance,
}

impl Companion {
    fn suffix(self) -> &'static str {
        match self {
            Companion::Metadata => METADATA_SUFFIX,
            Companion::Provenance => PROVENANCE_SUFFIX,
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Companion::Metadata => "text/plain; charset=utf-8",
            Companion::Provenance => "application/json",
        }
    }
}

/// Serve an artifact's PEP 658 metadata or PEP 740 provenance companion straight
/// from upstream, no storage writes.
async fn proxy_companion_passthrough(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    companion: Companion,
) -> Option<Response<Body>> {
    let before = match require_settled_package_read(state, storage, pkg).await {
        Ok(claim) => claim,
        Err(error) => return Some(read_error(error)),
    };
    let proxy = match eligible_proxy(state, storage, pkg).await {
        ProxyDecision::Serve(proxy) => proxy,
        ProxyDecision::Deny(resp) => return Some(resp),
        ProxyDecision::FallThrough => return None,
    };
    let bytes = match companion {
        Companion::Metadata => proxy.fetch_metadata(state, pkg, filename).await,
        Companion::Provenance => proxy.fetch_provenance(state, pkg, filename).await,
    }?;
    let artifact_filename = filename
        .strip_suffix(companion.suffix())
        .unwrap_or(filename);
    let artifact_key = format!("{PACKAGES_PREFIX}{pkg}/{artifact_filename}");
    let companion_key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
    match companion_passthrough_visible(state, storage, pkg, &artifact_key, &companion_key, &before)
        .await
    {
        Ok(true) => {}
        Ok(false) => return Some(not_found("artifact is fenced")),
        Err(error) => return Some(read_error(error)),
    }
    Some(
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, companion.content_type())
            .header(header::CACHE_CONTROL, ARTIFACT_CACHE_CONTROL)
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(Body::from(bytes))
            .unwrap_or_else(not_found),
    )
}

fn found_redirect(url: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, url)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::empty())
        .unwrap_or_else(not_found)
}

/// --- Helpers --------------------------------------------------------------
/// Check if the client accepts JSON response (PEP 691)
fn accepts_json(headers: &HeaderMap) -> bool {
    if let Some(accept) = headers.get(header::ACCEPT) {
        if let Ok(accept_str) = accept.to_str() {
            // Check for PEP 691 media type or generic application/json
            return accept_str.contains("application/vnd.pypi.simple.v1+json")
                || accept_str.contains("application/json");
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::buckets::{BucketHandle, BucketSet};

    use super::*;

    #[tokio::test]
    async fn multi_bucket_download_rejects_freeze_and_delete_markers() {
        let first = Arc::new(storage::test_support::InMemStorage::default());
        let second = Arc::new(storage::test_support::InMemStorage::default());
        origin::claim_origin(first.as_ref(), "pkg", origin::PRIVATE)
            .await
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
        let key = format!("{PACKAGES_PREFIX}pkg/pkg-1.whl");
        first.insert(&key, b"bytes".to_vec());
        first.insert(&frozen_key(&key), b"{}".to_vec());
        assert!(
            !multi_bucket_file_visible(&state, first.as_ref(), "pkg", &key)
                .await
                .unwrap()
        );

        first.delete_keys(&[frozen_key(&key)]).await.unwrap();
        first.insert(&tombstone_key(&key), b"{}".to_vec());
        assert!(
            !multi_bucket_file_visible(&state, first.as_ref(), "pkg", &key)
                .await
                .unwrap()
        );
    }
}
