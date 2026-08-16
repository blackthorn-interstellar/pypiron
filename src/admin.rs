//! Operational and admin HTTP surface: liveness/metrics, download stats, the
//! org audit report, install-token minting, and the admin-gated sync-cursor and
//! advisory-feed endpoints. Split out of `app.rs`; the generic response helpers,
//! the auth guards, and the multi-bucket settle check these lean on stay in
//! `app.rs`/`auth.rs` and are imported here.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, Method, Response, StatusCode},
    response::IntoResponse,
    Json,
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::warn;

use crate::app::{
    if_none_match, internal, not_found, read_error, require_settled_package_read, simple_response,
    unauthorized, AppState, SIMPLE_PREFIX,
};
use crate::auth::{nonempty, require_admin};
use crate::names::{checked_pkg_name, infer_version_from_filename};
use crate::pages::{html_ok, page_context, rank_packages};
use crate::{advisories, counters, html, origin, storage, sync, token};

/// Liveness: is the process up? Always `200 {"status":"ok"}` while the listener
/// is serving — no storage I/O, and it stays `200` right through a graceful
/// shutdown. This is the probe a Kubernetes `livenessProbe` keys on, so a storage
/// blip or an in-progress drain never gets the pod killed and restarted.
/// Whether this node can actually serve reads is the separate question answered
/// by [`ready`] — which is what a load balancer keys on.
pub(crate) async fn health() -> Response<Body> {
    simple_response(
        StatusCode::OK,
        "application/json",
        "no-cache",
        r#"{"status":"ok"}"#,
    )
}

/// Readiness: can this node serve reads right now? During a graceful shutdown it
/// reports `503 {"status":"draining"}` first, so a load balancer pulls the node
/// out of rotation before the listener stops accepting (see the shutdown path in
/// `run_serve`) — the pre-drain pause only works if the LB keys on this endpoint,
/// not on [`health`]. Otherwise it HEAD-probes the read pin's storage: a reply
/// (even `Ok(false)`, the probe object missing) proves storage answers, so
/// `200 {"status":"ready"}`; a storage error is `503 {"status":"degraded"}`.
/// The read pin is the bucket this node actually reads from, so a node whose near
/// bucket is down reports not-ready while liveness stays up.
pub(crate) async fn ready(State(state): State<Arc<AppState>>) -> Response<Body> {
    if state
        .shutting_down
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return simple_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "application/json",
            "no-cache",
            r#"{"status":"draining"}"#,
        );
    }
    let probe = format!("{SIMPLE_PREFIX}index.json");
    let (status, body) = match state.read_pin().storage.head_exists(&probe).await {
        Ok(_) => (StatusCode::OK, r#"{"status":"ready"}"#),
        Err(e) => {
            warn!(error=?e, "ready: storage probe failed");
            (StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"degraded"}"#)
        }
    };
    simple_response(status, "application/json", "no-cache", body)
}

/// Prometheus text exposition of the process counters.
pub(crate) async fn serve_metrics(State(state): State<Arc<AppState>>) -> Response<Body> {
    simple_response(
        StatusCode::OK,
        "text/plain; version=0.0.4",
        "no-cache",
        state.metrics.render(),
    )
}
/// Per-package counter series: `GET /stats/:metric/:package` (read-auth gated).
/// The inclusive date window both `/stats` surfaces report over: today back
/// through the 29 prior days (30 days total).
fn last_30d_window() -> (time::Date, time::Date) {
    let to = OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    (from, to)
}

/// Up to the last 30 days of daily counts, filenames rolled up to versions, plus
/// a grand total. Frozen days are exact; today is best-effort. Deliberately a
/// separate surface from `/metrics`, which stays low-cardinality.
pub(crate) async fn stats_get(
    State(state): State<Arc<AppState>>,
    Path((metric, package)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let Some(pkg) = checked_pkg_name(&package) else {
        return not_found("not a package");
    };
    let (from, to) = last_30d_window();
    let series = cached_package_series(&state, &metric, &pkg, from, to).await;

    let mut days: std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>> =
        std::collections::BTreeMap::new();
    let mut total: u64 = 0;
    for (day, files) in series.iter() {
        let by_ver = days.entry(day.clone()).or_default();
        for (filename, count) in files {
            total += count;
            let ver = infer_version_from_filename(filename).unwrap_or_else(|| "unknown".into());
            *by_ver.entry(ver).or_insert(0) += count;
        }
    }
    json_response(serde_json::json!({
        "metric": metric,
        "package": pkg,
        "total": total,
        "days": days,
    }))
}

/// How long a cached `/stats` set — the global `:metric` summary or a per-package
/// `:metric/:package` series — stays warm before a rescan. Matches the download
/// board's interval and rationale: the totals already lag a counter flush, so a
/// minute of staleness is invisible while it spares a repeated poll a full
/// counter-store rescan (a 30-day window over an open day is ~100 reads + ~70
/// cross-shard lists for the summary, ~30 reads + ~30 lists for one package —
/// the cost these endpoints otherwise paid every hit).
const SUMMARY_CACHE_TTL: Duration = Duration::from_secs(60);

/// Cap on distinct `:metric` keys held in the summary cache. `:metric` is a
/// read-gated path param, so a caller cycling it must never grow the map
/// without bound; past the cap it is cleared wholesale (the presign-cache
/// idiom). In practice only `downloads` is ever recorded, so this is never hit.
const SUMMARY_CACHE_MAX_METRICS: usize = 64;

/// Cap on distinct `(metric, package)` keys held in the per-package stats cache.
/// Unlike `:metric`, `:package` is high-cardinality, so the cap protects against
/// a caller cycling it (or a large fleet of watched packages); past it the map
/// is pruned of expired entries then, if still over, cleared wholesale — the
/// same bounding `SUMMARY_CACHE_MAX_METRICS` applies, sized for real dashboards.
const PACKAGE_STATS_CACHE_MAX: usize = 4096;

/// The global day-summaries for `metric`, served from a short metric-keyed TTL
/// cache so a repeated `/stats/:metric` poll doesn't rescan the counter store on
/// every hit — the same TTL-cache idiom the download leaderboard applies to the
/// homepage marquee. Computes and stores on a cold or expired entry.
async fn cached_summaries(
    state: &AppState,
    metric: &str,
    from: time::Date,
    to: time::Date,
) -> Arc<std::collections::BTreeMap<String, counters::DaySummary>> {
    {
        let guard = state
            .summary_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((at, summaries)) = guard.get(metric) {
            if at.elapsed() < SUMMARY_CACHE_TTL {
                return summaries.clone();
            }
        }
    }
    let summaries = Arc::new(state.counters.query_summaries(metric, from, to).await);
    let mut guard = state
        .summary_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    guard.insert(metric.to_string(), (Instant::now(), summaries.clone()));
    // Bound the map: drop expired entries once past the cap, and clear outright
    // if the live set still exceeds it (a caller cycling `:metric` can't grow it
    // without bound). Only `downloads` is ever recorded, so this never fires.
    if guard.len() > SUMMARY_CACHE_MAX_METRICS {
        guard.retain(|_, (at, _)| at.elapsed() < SUMMARY_CACHE_TTL);
        if guard.len() > SUMMARY_CACHE_MAX_METRICS {
            guard.clear();
        }
    }
    summaries
}

/// The per-package daily series for `(metric, package)`, served from the short
/// TTL cache keyed on the pair so a repeated `/stats/:metric/:package` poll
/// doesn't rescan the package's 30-day counter window on every hit — the
/// per-package twin of [`cached_summaries`]. Computes and stores on a cold or
/// expired entry; the worker drops the whole cache after each counter flush, so
/// a same-node poll never lags its own writes past a flush interval.
async fn cached_package_series(
    state: &AppState,
    metric: &str,
    pkg: &str,
    from: time::Date,
    to: time::Date,
) -> Arc<std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>>> {
    let cache_key = (metric.to_string(), pkg.to_string());
    {
        let guard = state
            .package_stats_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((at, series)) = guard.get(&cache_key) {
            if at.elapsed() < SUMMARY_CACHE_TTL {
                return series.clone();
            }
        }
    }
    let series = Arc::new(state.counters.query_package(metric, pkg, from, to).await);
    let mut guard = state
        .package_stats_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    guard.insert(cache_key, (Instant::now(), series.clone()));
    // Bound the map: past the cap drop expired entries, and clear outright if the
    // live set still exceeds it — `:package` is a high-cardinality read-gated path
    // param, so a caller cycling it can't grow the map without bound.
    if guard.len() > PACKAGE_STATS_CACHE_MAX {
        guard.retain(|_, (at, _)| at.elapsed() < SUMMARY_CACHE_TTL);
        if guard.len() > PACKAGE_STATS_CACHE_MAX {
            guard.clear();
        }
    }
    series
}

/// Global counter summary: `GET /stats/:metric` (read-auth gated). The last 30
/// days of per-day totals and the busiest packages, from the leader-written
/// per-day summaries (top keys are rolled up to packages — approximate at the
/// tail, fine for a dashboard glance). Served from a short TTL cache
/// ([`cached_summaries`]) so a repeated poll doesn't rescan the counter store.
pub(crate) async fn stats_summary_get(
    State(state): State<Arc<AppState>>,
    Path(metric): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let (from, to) = last_30d_window();
    let summaries = cached_summaries(&state, &metric, from, to).await;

    let mut total: u64 = 0;
    let mut days: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for (day, s) in summaries.iter() {
        total += s.total;
        days.insert(day.clone(), s.total);
    }
    let mut ranked = rank_packages(&summaries);
    ranked.truncate(20);
    json_response(serde_json::json!({
        "metric": metric,
        "total": total,
        "days": days,
        "top": ranked.into_iter().collect::<std::collections::BTreeMap<_, _>>(),
    }))
}
fn json_response(value: serde_json::Value) -> Response<Body> {
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    simple_response(StatusCode::OK, "application/json", "no-store", bytes)
}
/// Read the sync-cursor blob (the server-side memo a mirror-over-HTTP sync
/// reads to stay conditional). Admin-gated; an absent blob is an empty object,
/// not a 404 — a first-ever sync run is the normal case.
pub(crate) async fn sync_cursors_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let bytes = match state.pin().storage.get_bytes(sync::CURSORS_KEY).await {
        Ok(b) => b,
        Err(e) if storage::is_not_found(&e) => b"{}".to_vec(),
        Err(e) => return Err(internal("read", e)),
    };
    Ok(([(header::CONTENT_TYPE, "application/json")], bytes))
}

/// Replace the sync-cursor blob. Admin-gated. The body must be a JSON object
/// (sync's own format); we validate that much so a malformed PUT can't poison
/// the next sync's reads, but the contents are otherwise opaque to the server.
pub(crate) async fn sync_cursors_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    if serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&body).is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            "cursors body must be a JSON object".into(),
        ));
    }
    state
        .pin()
        .storage
        .put_bytes(
            sync::CURSORS_KEY,
            body.into_bytes(),
            Some("application/json"),
        )
        .await
        .map_err(|e| internal("write", e))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Serve the stored advisory-snapshot zip (reader-gated), so a mirror-over-HTTP
/// `sync` can pull it from an upstream pypiron. The ETag is the zip's sha256;
/// `If-None-Match` short-circuits to 304 and HEAD sends headers only (each sync
/// poll is one of these). 404 when no snapshot has been delivered yet.
pub(crate) async fn advisories_feed_get(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let storage = state.pin().storage.clone();
    // The current storage etag (a 1-key LIST, no body) is the cheap currency
    // check. When it matches the loaded snapshot, serve the ETag and bytes from
    // memory — a sync poll (HEAD/304) then costs no 32 MB read and no re-hash.
    let storage_etag = match advisories::feed_storage_etag(storage.as_ref()).await {
        Ok(Some(e)) => e,
        Ok(None) => return not_found("no advisory snapshot"),
        Err(e) => return read_error(e),
    };
    let snap = state.advisory_snapshot();
    if snap.storage_etag.as_deref() == Some(storage_etag.as_str()) {
        if let (Some(sha), Some(zip)) = (&snap.zip_sha256, &snap.zip) {
            return serve_advisory_bytes(&method, &headers, &format!("\"{sha}\""), zip);
        }
    }
    // Slow path: storage moved under us (or this node hasn't loaded it) — read the
    // bytes and hash them so the ETag always matches the body served.
    let bytes = match advisories::stored_feed_bytes(storage.as_ref()).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return not_found("no advisory snapshot"),
        Err(e) => return read_error(e),
    };
    let etag = format!("\"{}\"", crate::hash::sha256_hex(&bytes));
    serve_advisory_bytes(&method, &headers, &etag, &bytes)
}

/// Build the advisory-feed response: 304 on a matching `If-None-Match`, headers
/// only for HEAD, else the full zip body. Shared by the fast (in-memory) and slow
/// (read-through) paths so both negotiate identically.
fn serve_advisory_bytes(
    method: &Method,
    headers: &HeaderMap,
    etag: &str,
    bytes: &[u8],
) -> Response<Body> {
    let revalidated = if_none_match(headers, &[etag]);
    let builder = Response::builder().header(header::ETAG, etag);
    let response = if revalidated {
        builder.status(StatusCode::NOT_MODIFIED).body(Body::empty())
    } else {
        let builder = builder
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/zip")
            .header(header::CONTENT_LENGTH, bytes.len());
        // HEAD advertises the size without materializing (or sending) the body.
        if method == Method::HEAD {
            builder.body(Body::empty())
        } else {
            builder.body(Body::from(bytes.to_vec()))
        }
    };
    response.unwrap_or_else(not_found)
}

/// Accept a pushed advisory snapshot (admin) — the sync-delivery path for
/// air-gapped destinations. Authenticate before parsing (a client must not probe
/// well-formed vs malformed via the status), validate it parses, persist it
/// verbatim, then arm an immediate worker reload so blocking self-arms without a
/// restart.
pub(crate) async fn advisories_feed_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    // Validate before persisting — the stored copy is what every node loads, so
    // a garbage PUT must never overwrite a good snapshot. Parse off the runtime.
    let bytes = body.to_vec();
    let for_parse = bytes.clone();
    match tokio::task::spawn_blocking(move || advisories::parse_feed(&for_parse)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("not a valid advisory feed: {e}"),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "advisory parse task failed".into(),
            ))
        }
    }
    // Write-through to every healthy bucket: the selected bucket is authoritative
    // (its etag drives the reload); peers get it best-effort so a failover to any
    // bucket serves the admin-pushed snapshot. Single-bucket mode writes once.
    let pinned = state.pin();
    let replicas = state.singleton_replicas(pinned.index);
    crate::layout::write_singleton(
        pinned.storage.as_ref(),
        &replicas,
        advisories::FEED_KEY,
        bytes,
        Some("application/zip"),
    )
    .await
    .map_err(|e| internal("write", e))?;
    // Load it this worker tick regardless of the reconcile period, and wake the
    // worker now so the load is ~immediate rather than up to a tick away.
    state
        .advisory_reload_asap
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state.worker_nudge.notify_one();
    Ok(StatusCode::NO_CONTENT)
}

/// The note surfaced (JSON field + HTML banner) when no audit report has been
/// materialized yet — a feed is set but no leader sweep has run, or no snapshot
/// is loaded. Distinct from an empty-but-materialized report (a loaded feed that
/// nothing hosted matches), which renders as an empty row set with no note.
const AUDIT_ABSENT_NOTE: &str = "no advisory snapshot loaded yet";

/// The org audit as JSON (admin-gated): the ranked hosted (package, version) rows
/// a known advisory affects — ids, fixed-in, 30-day downloads, blocked flag.
/// Served byte-verbatim from the stored report (admin-only, low traffic, so no
/// cache). An unmaterialized report is an empty-rows body with a note, never a 404
/// — the endpoint always exists. An org's ranked vulnerability list is attacker
/// recon, so it rides the strongest credential.
pub(crate) async fn audit_json(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response<Body>, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    match advisories::stored_report_bytes(state.pin().storage.as_ref()).await {
        // Verbatim bytes from the stored report; the shared no-store shape.
        Ok(Some(bytes)) => Ok(simple_response(
            StatusCode::OK,
            "application/json",
            "no-store",
            bytes,
        )),
        Ok(None) => {
            let empty = serde_json::json!({
                "generated_unix": 0,
                "feed_sha256": "",
                "rows": [],
                "note": AUDIT_ABSENT_NOTE,
            });
            Ok(json_response(empty))
        }
        Err(e) => Err(internal("reading audit report", e)),
    }
}

/// The org audit as server-rendered HTML (admin-gated): the same ranked rows as
/// `/audit.json`, rendered the house way. Reads the stored report per request (no
/// cache — admin-only, low traffic); an unmaterialized report renders an empty
/// table under a banner note, never a 404.
pub(crate) async fn audit_page(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response<Body> {
    if let Err((code, msg)) = require_admin(&state, &headers) {
        return (code, msg).into_response();
    }
    let report = match advisories::stored_report(state.pin().storage.as_ref()).await {
        Ok(report) => report,
        Err(e) => return read_error(e),
    };
    html_ok(html::audit_html(
        &page_context(&state, &headers),
        report.as_ref(),
        AUDIT_ABSENT_NOTE,
    ))
}

/// The locally-materialized PEP 691 index for a package, read straight from
/// storage so the on-demand proxy never shadows it. Admin-gated; a package with
/// no local index yet is an empty listing, not a 404 (so the caller treats it
/// as "nothing mirrored", not "endpoint missing").
pub(crate) async fn sync_local_index(
    State(state): State<Arc<AppState>>,
    Path(package): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let Some(pkg) = checked_pkg_name(&package) else {
        return Err((StatusCode::NOT_FOUND, "no such package".to_string()));
    };
    let pinned = state.pin();
    let before = require_settled_package_read(&state, pinned.storage.as_ref(), &pkg)
        .await
        .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
    if state.buckets.is_multi()
        && before
            .as_ref()
            .is_none_or(|claim| claim.state == origin::OriginState::Unclaimed)
    {
        return Ok((
            [
                (header::CONTENT_TYPE, "application/json"),
                (
                    header::HeaderName::from_static("x-pypiron-origin"),
                    origin::UNCLAIMED,
                ),
            ],
            br#"{"files":[]}"#.to_vec(),
        ));
    }
    let key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
    let bytes = match pinned.storage.get_bytes(&key).await {
        Ok(b) => b,
        Err(e) if storage::is_not_found(&e) => br#"{"files":[]}"#.to_vec(),
        Err(e) => return Err(internal("read", e)),
    };
    if state.buckets.is_multi() {
        let after = require_settled_package_read(&state, pinned.storage.as_ref(), &pkg)
            .await
            .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
        if after != before {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("package '{pkg}' changed while reading its local index"),
            ));
        }
    }
    let owner = before
        .as_ref()
        .map_or(origin::UNCLAIMED, |claim| claim.state.as_str());
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::HeaderName::from_static("x-pypiron-origin"), owner),
        ],
        bytes,
    ))
}
/// How long a minted install token is valid. Deliberately short: tokens are
/// single-session, basically single-use, so a leaked one is dead within
/// minutes — which is also why they need no revocation list (and no storage).
const TOKEN_TTL_SECS: i64 = 300;

/// Hold a gathered attribution value to something sane before it is signed into
/// a token: trim, drop control chars (so it can't later forge a log line), cap
/// length, and treat empty as absent. Charset is otherwise unrestricted — what
/// we gather is independent of where it is later routed.
fn clip_meta(value: Option<String>) -> Option<String> {
    let v: String = value?
        .trim()
        .chars()
        .filter(|c| !c.is_control())
        .take(256)
        .collect();
    (!v.is_empty()).then_some(v)
}

#[derive(serde::Deserialize, Default)]
struct MintRequest {
    /// Requested role; defaults to `reader`. Cannot exceed what the presented
    /// credential already grants.
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    repo: Option<String>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default)]
    user: Option<String>,
}

#[derive(serde::Serialize)]
struct MintResponse {
    token: String,
    username: &'static str,
    role: &'static str,
    expires_in: i64,
    expires_at: String,
}

/// Mint a short-lived install token. Fail-closed: token auth must be configured
/// (a signing key), and the presenting credential must already grant the
/// requested role — a token can never escalate beyond the credential that
/// minted it. On an open (public-read) server, a reader token needs no
/// credential, since reader access is already public.
pub(crate) async fn mint_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let key = nonempty(state.token_signing_key.as_deref()).ok_or((
        StatusCode::FORBIDDEN,
        "token minting is disabled (no --token-signing-key configured)".to_string(),
    ))?;
    // A token cannot mint tokens. Minting requires a base (username/password)
    // credential, so a leaked token can't refresh itself into a fresh full TTL
    // indefinitely — the short expiry stays meaningful, which is the whole basis
    // for carrying no revocation list (see TOKEN_TTL_SECS). token_role is Some
    // only when the presented credential is itself a valid __token__ bearer.
    if state.token_role(&headers).is_some() {
        return Err((
            StatusCode::FORBIDDEN,
            "a token cannot mint tokens; authenticate with a configured credential".to_string(),
        ));
    }
    let req: MintRequest = if body.trim().is_empty() {
        MintRequest::default()
    } else {
        serde_json::from_str(&body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")))?
    };
    let role = match req.role.as_deref() {
        None => token::Role::Reader,
        Some(r) => {
            token::Role::parse(r).ok_or((StatusCode::BAD_REQUEST, format!("unknown role: {r}")))?
        }
    };
    let granted = match role {
        token::Role::Reader => state.is_reader(&headers),
        token::Role::Uploader => state.is_uploader(&headers),
        token::Role::Admin => state.is_admin(&headers),
    };
    if !granted {
        return Err((
            StatusCode::UNAUTHORIZED,
            format!("the supplied credential does not grant {role} (cannot mint a {role} token)"),
        ));
    }

    let now = OffsetDateTime::now_utc().unix_timestamp();
    let claims = token::Claims {
        role,
        repo: clip_meta(req.repo),
        commit: clip_meta(req.commit),
        user: clip_meta(req.user),
        iat: now,
        exp: now + TOKEN_TTL_SECS,
    };
    let token = token::mint(key, &claims).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("minting token: {e}"),
        )
    })?;
    let expires_at = OffsetDateTime::from_unix_timestamp(claims.exp)
        .ok()
        .and_then(|t| t.format(&Rfc3339).ok())
        .unwrap_or_default();
    Ok(Json(MintResponse {
        token,
        username: token::TOKEN_USERNAME,
        role: role.as_str(),
        expires_in: TOKEN_TTL_SECS,
        expires_at,
    }))
}
