//! The human dashboard: the landing page, package browser, project pages, and
//! download leaderboard, plus the ranking logic behind them. These are rendered
//! on demand from storage truth (no materialized view) and gated by read auth in
//! each handler. The generic response helpers and the multi-bucket settle check
//! they lean on stay in `app.rs` and are imported here.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Response, StatusCode},
};
use time::OffsetDateTime;
use tracing::warn;

use crate::app::{
    moved_permanently, not_found, read_error, recheck_settled, require_settled_package_read,
    unauthorized, AppState, ArtifactDelivery, PACKAGES_PREFIX, SIMPLE_PREFIX, VERSION,
};
use crate::{
    advisories, coremeta, counters, names, origin, project_cache, provenance, render, sidecar,
    storage, web, worker,
};
use names::{checked_pkg_name, infer_version_from_filename};
use sidecar::Yanked;
use storage::Storage;

/// The root landing page: a self-contained HTML front door with copy-paste
/// client config. Public (no secrets) — like `/health`, it carries no auth.
/// The live activity panel (traffic counters, project-tag names) is folded in
/// only for an authorized reader, so a public deployment never leaks stats: it
/// surfaces the same data as `/metrics`, but legibly, and only to operators who
/// can already read. When reads are public (no read credential), everyone sees
/// it — consistent with that deployment's open posture.
pub async fn root(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response<Body> {
    let ctx = page_context(&state, &headers);
    // Registry inventory (counts only) is public — shown under the header.
    let inventory = state.metrics.inventory();
    if !state.is_reader(&headers) {
        return html_ok(web::landing_html(&ctx, inventory.as_ref(), None));
    }
    let snap = state.metrics.snapshot();
    let (cache_hits, cache_misses) = state.index_cache.stats();
    let board = download_leaderboard(&state).await;
    let dash = web::DashboardData {
        snapshot: &snap,
        cache_hits,
        cache_misses,
        top_downloads: &board,
    };
    html_ok(web::landing_html(&ctx, inventory.as_ref(), Some(&dash)))
}

/// Optional `?q=` search term for the package browser.
#[derive(serde::Deserialize)]
pub struct BrowseQuery {
    q: Option<String>,
}

/// The human package browser (`/projects/`), which doubles as the search results
/// page: every hosted package (or those matching `?q=`), linked to its project
/// page. Read-only and gated by read auth like the activity panel, so a `?q=`
/// search can never enumerate private names on a credentialed deployment.
pub async fn projects_page(
    State(state): State<Arc<AppState>>,
    Query(browse): Query<BrowseQuery>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let pinned = state.pin();
    let names = match worker::global_package_names(&state, pinned.storage.as_ref()).await {
        Ok(names) => names,
        Err(e) => return read_error(e),
    };
    html_ok(web::projects_html(
        &page_context(&state, &headers),
        &names,
        browse.q.as_deref().unwrap_or(""),
    ))
}

/// The download leaderboard (`/downloads/`): the most-downloaded packages over
/// the last 30 days, busiest first (top 500). Read-only and gated by read auth
/// like the dashboard, so it never enumerates private names on a credentialed
/// deployment. Served from the same TTL-cached board as the homepage marquee.
pub async fn downloads_page(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.is_reader(&headers) {
        return unauthorized();
    }
    let board = download_leaderboard(&state).await;
    html_ok(web::downloads_html(&page_context(&state, &headers), &board))
}

/// The human project page (`/project/<pkg>/`): the latest version, tabbed into
/// description / release history / download files. Read-only and gated by read
/// auth like the dashboard. Rendered on demand — no materialized view.
pub async fn project_page(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    render_project(&state, &headers, &raw, None).await
}

/// The per-version page (`/project/<pkg>/<version>/`): the same page focused on
/// one release, with a version-pinned install snippet.
pub async fn project_version_page(
    State(state): State<Arc<AppState>>,
    Path((raw, version)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    render_project(&state, &headers, &raw, Some(&version)).await
}

/// The PEP 700 `versions` list from a package's materialized JSON index — the
/// evidence the project page rules on before it pays for a full metadata scan.
/// `None` means "inconclusive, do the scan": no field, or a body that doesn't
/// parse (a not-yet-rewritten pre-PEP 700 view must widen to the scan, never
/// 404 a hosted release).
///
/// The view's list is derived exactly as the page's own authoritative list is —
/// sidecar version, filename-inferred fallback (`render::pep691_package_json` /
/// `web::file_version`) — so membership here has parity with the real check,
/// legacy formats included. Staleness only lags in safe directions: a deleted
/// version still listed costs one scan that 404s properly, and a just-uploaded
/// version missing from the view isn't visible on `/simple/` yet either.
fn versions_from_package_index(body: &[u8]) -> Option<Vec<String>> {
    #[derive(serde::Deserialize)]
    struct VersionsOnly {
        versions: Option<Vec<String>>,
    }
    serde_json::from_slice::<VersionsOnly>(body).ok()?.versions
}

/// Shared project-page renderer. `requested_version` is `None` for the latest
/// view; when present it's validated against the hosted versions (so an
/// arbitrary path segment is never reflected) and pins the install snippet.
async fn render_project(
    state: &AppState,
    headers: &HeaderMap,
    raw: &str,
    requested_version: Option<&str>,
) -> Response<Body> {
    if !state.is_reader(headers) {
        return unauthorized();
    }
    let Some(pkg) = checked_pkg_name(raw) else {
        return not_found("invalid package name");
    };
    // Canonical URL is the normalized package name; everything else 301s there.
    // The version segment is percent-encoded so a hostile, not-yet-validated
    // value (e.g. `..%2f..%2fsimple`, which axum has already decoded) can't cross
    // a path boundary in the `Location` header — it lands back here and 404s.
    if raw != pkg {
        let dest = match requested_version {
            Some(v) => format!(
                "/project/{pkg}/{}/",
                percent_encoding::utf8_percent_encode(v, PATH_SEGMENT)
            ),
            None => format!("/project/{pkg}/"),
        };
        return moved_permanently(&dest);
    }
    // Serve the rendered page straight from RAM when it's warm: the human project
    // page is otherwise a full package-prefix scan + per-file sidecar parse on
    // every hit (see project_cache.rs). The render embeds the request's base URL
    // (the install snippet's index URL) as a sentinel that serve_project_page
    // fills in per request, so the cache key is host-independent — a forged Host /
    // X-Forwarded-Host can't thrash it with distinct keys, each a full scan. The
    // worker drops a package's entries the instant it rebuilds, and the TTL bounds
    // the rest. `write_pin` is captured once (design §3) so the cache generation
    // and the storage handle come from the same selection.
    let write_pin = state.pin();
    let generation = write_pin.generation;
    let cache_key = project_cache::key(&pkg, requested_version);
    // Serve a fresh — or, under a concurrent burst past the TTL, single-flight
    // stale — page straight from RAM. Only the one reader that claims the
    // re-render falls through to the scan below, so a hot key costs one render
    // per TTL, not one per request. The claim rides along to the `put` below and
    // releases when it drops (at the put, or on any early return here), so an
    // aborted render can't strand the key as forever-refilling.
    let _render_claim = match state.project_cache.get(&cache_key, generation) {
        project_cache::Lookup::Fresh(cached) | project_cache::Lookup::Stale(cached) => {
            return serve_project_page(cached, headers);
        }
        project_cache::Lookup::MustRender(claim) => claim,
    };
    // `storage` (not `pinned`, which below is the version-pinned flag) is this
    // render's captured handle (design §3), threaded to every read.
    let storage = write_pin.storage.clone();
    // Settle "no such version" *before* the scan below, which is one LIST plus a
    // sidecar GET per artifact file. A negative answer is never cached (each
    // unknown version is a distinct cache key, and we don't store 404s), so
    // without this an anonymous client cycling `/project/<popular-pkg>/<random>/`
    // turns every request into a full package scan — request amplification /
    // denial of wallet, defeating the page cache exactly as the forged-Host cache
    // key did (eaa18d8). The worker already materializes the answer: the PEP 700
    // `versions` list in the package's JSON index view, one small GET whatever
    // the package's size. Only inconclusive evidence widens to the scan — a
    // missing view (package never indexed, or not hosted at all: the scan path
    // still answers "no such project"), a pre-PEP 700 body, or an empty list
    // (a quarantined package's view is emptied while its page still renders).
    if let Some(requested) = requested_version {
        let view_key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
        match storage.get_bytes(&view_key).await {
            Ok(body) => {
                if let Some(versions) = versions_from_package_index(&body) {
                    if !versions.is_empty() && !versions.iter().any(|known| known == requested) {
                        return not_found("no such version");
                    }
                }
            }
            Err(e) if storage::is_not_found(&e) => {}
            Err(e) => return read_error(e),
        }
    }
    let before = match require_settled_package_read(state, storage.as_ref(), &pkg).await {
        Ok(claim) => claim,
        Err(error) => return read_error(error),
    };
    if state.buckets.is_multi()
        && before
            .as_ref()
            .is_none_or(|value| value.state == origin::OriginState::Unclaimed)
    {
        return not_found("no such project");
    }
    let listed = if state.buckets.is_multi() {
        worker::list_artifacts_readonly(storage.as_ref(), &pkg).await
    } else {
        worker::list_artifacts(storage.as_ref(), &pkg).await
    };
    let files = match listed {
        Ok((files, _raw)) => files,
        Err(e) => return read_error(e),
    };
    if state.buckets.is_multi() {
        if let Some(resp) =
            recheck_settled(state, storage.as_ref(), &pkg, &before, "rendering").await
        {
            return resp;
        }
    }
    if files.is_empty() {
        return not_found("no such project");
    }

    // Pick the version to display: the requested one (must be hosted), else the
    // latest by PEP 440 order.
    let mut versions: Vec<String> = files
        .iter()
        .filter_map(web::file_version)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    versions.sort_by(|a, b| names::version_cmp_desc(a, b));
    let (selected, pinned) = match requested_version {
        Some(v) => {
            if !versions.iter().any(|known| known == v) {
                return not_found("no such version");
            }
            (v.to_string(), true)
        }
        // The bare page headlines the newest *stable*, not-fully-yanked release
        // (pypi.org behavior), falling back to the newest overall, then to empty
        // for legacy artifacts with no derivable version (still rendered).
        None => (default_display_version(&files, &versions), false),
    };

    // Representative files of the selected version — the newest carrying each
    // companion. Metadata and provenance may ride on different artifacts.
    let meta_rep = representative(&files, &selected, |f| f.core_metadata);
    let meta = match meta_rep {
        Some(f) => load_core_metadata(storage.as_ref(), &pkg, f).await,
        None => None,
    };
    let publisher = match representative(&files, &selected, |f| f.provenance) {
        Some(f) => load_provenance(storage.as_ref(), &pkg, f).await,
        None => None,
    };

    let downloads = download_summary(state, &pkg).await;
    if state.buckets.is_multi() {
        if let Some(resp) =
            recheck_settled(state, storage.as_ref(), &pkg, &before, "rendering").await
        {
            return resp;
        }
    }

    // Per-package advisory panel (rung 8): the in-memory audit index against the
    // hosted versions. The cached page may lag the live db by a TTL — accepted,
    // the rows are informational (the byte gate is the guarantee either way).
    let advisory_panel = advisory_panel_rows(state, storage.as_ref(), &pkg, &versions).await;
    // Render host-independently: the base URL is a sentinel that serve_project_page
    // replaces with the request's real host, so the cached bytes are identical for
    // every host and safe to share.
    let mut ctx = page_context(state, headers);
    ctx.base_url = project_cache::BASE_URL_SENTINEL.to_string();
    let html = web::project_html(
        &ctx,
        &pkg,
        &files,
        &selected,
        pinned,
        meta.as_ref(),
        publisher.as_ref(),
        &downloads,
        &advisory_panel,
    );
    let body = bytes::Bytes::from(html);
    state.project_cache.put(cache_key, body.clone(), generation);
    serve_project_page(body, headers)
}

/// Serve a project page whose cached bytes carry [`project_cache::BASE_URL_SENTINEL`]:
/// fill in this request's real host and hand back the response. Because the cache
/// key is host-independent, a forged Host / X-Forwarded-Host can neither thrash
/// the cache with distinct keys — each a full package-prefix scan — nor poison
/// another visitor's install snippet. The substitution is one scan of an
/// already-rendered page, negligible beside the storage scan the cache avoids.
fn serve_project_page(body: bytes::Bytes, headers: &HeaderMap) -> Response<Body> {
    match std::str::from_utf8(&body) {
        Ok(text) if text.contains(project_cache::BASE_URL_SENTINEL) => {
            let base_url = base_url_from_headers(headers);
            html_ok_bytes(bytes::Bytes::from(
                text.replace(project_cache::BASE_URL_SENTINEL, &base_url),
            ))
        }
        // No sentinel (or non-UTF8, which a rendered page never is): serve as-is.
        _ => html_ok_bytes(body),
    }
}

/// The per-package advisory panel rows: the in-memory audit index matched against
/// the package's hosted `versions`. Empty (no panel) when the db is unfed, the
/// package is private-origin (origin exclusivity — a same-named private package is
/// not the one OSV named), or nothing matches. Mirrors the byte gate's laziness:
/// the origin read is paid only on an actual advisory match, and a read error
/// suppresses the panel (informational rows must never falsely flag a private
/// package). The `blocked` badge marks `MAL-*` matches (what the gate 403s).
async fn advisory_panel_rows(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    versions: &[String],
) -> Vec<web::AdvisoryPanelRow> {
    let snap = state.advisory_snapshot();
    let Some(db) = snap.db.as_deref() else {
        return Vec::new();
    };
    // A quarantined project is refused wholesale at the byte gate, so its rows are
    // blocked regardless of advisory kind — mirror the /audit report's roll-in.
    let quarantined = snap.quarantined.contains(pkg);
    let mut rows: Vec<web::AdvisoryPanelRow> = Vec::new();
    for version in versions {
        for record in advisories::advisories_for(db, pkg, version) {
            rows.push(web::AdvisoryPanelRow {
                version: version.clone(),
                id: record.id.clone(),
                severity: record.severity.clone(),
                fixed_in: record.fixed_in.clone(),
                blocked: quarantined || record.id.starts_with("MAL-"),
            });
        }
    }
    if rows.is_empty() {
        return rows; // no match → no origin read, no panel
    }
    // A match — but a private-origin name is never the package OSV named. Fast
    // private-prefix pre-check (no I/O), then the authoritative claim.
    if let Some(prefix) = &state.private_prefix {
        if names::matches_prefix(pkg, prefix) {
            return Vec::new();
        }
    }
    match origin::read_origin_claim(storage, pkg).await {
        Ok(Some(origin::OriginState::Private)) => return Vec::new(),
        Ok(_) => {}
        Err(e) => {
            warn!(error = ?e, %pkg, "advisory panel: origin read failed; omitting panel");
            return Vec::new();
        }
    }
    // Deterministic: newest version first, then advisory id; drop exact dupes.
    rows.sort_by(|a, b| {
        names::version_cmp_desc(&a.version, &b.version).then_with(|| a.id.cmp(&b.id))
    });
    rows.dedup_by(|a, b| a.version == b.version && a.id == b.id);
    rows
}

/// Last-30-day download counts for a package, filenames rolled up to versions
/// and sorted busiest first — the data behind the project page's Downloads card.
/// Empty (no traffic, or counters disabled) renders nothing.
async fn download_summary(state: &AppState, pkg: &str) -> Vec<(String, u64)> {
    let to = OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    let series = state
        .counters
        .query_package("downloads", pkg, from, to)
        .await;
    let mut by_ver: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for files in series.values() {
        for (filename, count) in files {
            let ver = infer_version_from_filename(filename).unwrap_or_else(|| "unknown".into());
            *by_ver.entry(ver).or_insert(0) += count;
        }
    }
    let mut out: Vec<(String, u64)> = by_ver.into_iter().collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// How long the cached download leaderboard stays warm. Downloads already lag a
/// flush interval (300 s default), so a minute of staleness is invisible — but
/// it spares a public, S3-backed homepage a counter-store rescan on every hit.
const DOWNLOAD_BOARD_TTL: Duration = Duration::from_secs(60);

/// Rank packages by total downloads from the per-day counter summaries, busiest
/// first. Each summary's top keys are `<pkg>/<filename>`; we roll them up to the
/// package. Approximate at the tail (a day keeps only its top keys), which is
/// fine for a leaderboard glance. Shared by the global `/stats` JSON and the
/// human leaderboard so both rank identically.
pub fn rank_packages(
    summaries: &std::collections::BTreeMap<String, counters::DaySummary>,
) -> Vec<(String, u64)> {
    let mut by_pkg: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for s in summaries.values() {
        for (k, v) in &s.top {
            let pkg = k.split('/').next().unwrap_or(k);
            *by_pkg.entry(pkg.to_string()).or_insert(0) += v;
        }
    }
    let mut ranked: Vec<(String, u64)> = by_pkg.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked
}

/// The most-downloaded packages over the last 30 days, ranked busiest first and
/// capped at 500. The uncached compute behind [`download_leaderboard`].
async fn compute_download_board(state: &AppState) -> Vec<(String, u64)> {
    let to = OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    let summaries = state.counters.query_summaries("downloads", from, to).await;
    let mut ranked = rank_packages(&summaries);
    ranked.truncate(500);
    ranked
}

/// The download leaderboard, served from a short TTL cache so a public homepage
/// (where every viewer sees the activity panel) doesn't rescan the counter store
/// on every request. Returns up to the top 500 packages; callers slice as needed.
async fn download_leaderboard(state: &AppState) -> Vec<(String, u64)> {
    {
        let guard = state
            .download_board
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((at, board)) = guard.as_ref() {
            if at.elapsed() < DOWNLOAD_BOARD_TTL {
                return board.clone();
            }
        }
    }
    let board = compute_download_board(state).await;
    let mut guard = state
        .download_board
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *guard = Some((std::time::Instant::now(), board.clone()));
    board
}

/// Characters refused raw in a redirect path segment — anything that could
/// cross a path boundary or restructure the URL. `versions` come in
/// already-percent-decoded by axum, so a `/` or control byte here is hostile.
const PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
    .add(b'/')
    .add(b'%')
    .add(b'?')
    .add(b'#')
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'|');

/// The version to headline on the bare project page: the newest stable release
/// (not a pre-release/dev) that isn't fully yanked, like pypi.org — falling back
/// to the newest overall, then to empty (legacy artifacts with no version).
fn default_display_version(files: &[render::FileMetadata], versions: &[String]) -> String {
    versions
        .iter()
        .find(|v| !is_prerelease(v) && !fully_yanked(files, v))
        .or_else(|| versions.first())
        .cloned()
        .unwrap_or_default()
}

/// Whether a version string parses as a PEP 440 pre-release or dev release.
fn is_prerelease(v: &str) -> bool {
    v.parse::<pep440_rs::Version>()
        .map(|ver| ver.any_prerelease())
        .unwrap_or(false)
}

/// Whether every file of `version` is yanked (so the release shouldn't headline).
fn fully_yanked(files: &[render::FileMetadata], version: &str) -> bool {
    let mut vers = files
        .iter()
        .filter(|f| web::file_version(f).as_deref() == Some(version))
        .peekable();
    vers.peek().is_some() && vers.all(|f| !matches!(f.yanked, Yanked::Flag(false)))
}

/// The newest-uploaded file of `version` for which `want` holds — used to pick
/// the artifact whose `.metadata` / `.provenance` companion represents a release.
fn representative<'a>(
    files: &'a [render::FileMetadata],
    version: &str,
    want: impl Fn(&render::FileMetadata) -> bool,
) -> Option<&'a render::FileMetadata> {
    files
        .iter()
        .filter(|f| want(f) && web::file_version(f).as_deref() == Some(version))
        .max_by(|a, b| a.upload_time.cmp(&b.upload_time))
}

/// Parse a representative file's `.metadata` companion. Best-effort: any miss
/// returns `None` and the page renders without a sidebar.
async fn load_core_metadata(
    storage: &dyn Storage,
    pkg: &str,
    rep: &render::FileMetadata,
) -> Option<coremeta::CoreMetadata> {
    let key = sidecar::metadata_key(&format!("{PACKAGES_PREFIX}{pkg}/{}", rep.filename));
    let bytes = storage.get_bytes(&key).await.ok()?;
    Some(coremeta::parse(&bytes))
}

/// Parse a representative file's relayed `.provenance` companion into its
/// publisher. Best-effort: a miss or malformed bundle returns `None` and the
/// page renders without a "Verified details" section.
async fn load_provenance(
    storage: &dyn Storage,
    pkg: &str,
    rep: &render::FileMetadata,
) -> Option<provenance::Publisher> {
    let key = sidecar::provenance_key(&format!("{PACKAGES_PREFIX}{pkg}/{}", rep.filename));
    let bytes = storage.get_bytes(&key).await.ok()?;
    provenance::parse_publisher(&bytes)
}

/// Build the request-derived context both pages share. The base URL honors a
/// reverse proxy's `X-Forwarded-Proto`/`-Host`, falling back to the `Host`
/// header; the host is restricted to a plausible charset (it lands in the page
/// as escaped text, but we keep it tidy too).
pub fn page_context(state: &AppState, headers: &HeaderMap) -> web::PageContext {
    web::PageContext {
        base_url: base_url_from_headers(headers),
        version: VERSION,
        proxy_enabled: state.proxy.is_some(),
        delivery: match state.artifact_delivery {
            ArtifactDelivery::Auto => "auto",
            ArtifactDelivery::Redirect => "redirect",
            ArtifactDelivery::Stream => "stream",
        },
        reads_authenticated: state.read_credential().is_some(),
        uptime_secs: state.started.elapsed().as_secs(),
    }
}

fn base_url_from_headers(headers: &HeaderMap) -> String {
    let first = |v: &HeaderValue| -> Option<String> {
        v.to_str()
            .ok()
            .and_then(|s| s.split(',').next())
            .map(|s| s.trim().to_string())
    };
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(first)
        .filter(|s| s == "http" || s == "https")
        .unwrap_or_else(|| "http".to_string());
    let host = headers
        .get("x-forwarded-host")
        .and_then(first)
        .or_else(|| headers.get(header::HOST).and_then(first))
        .filter(|h| is_plausible_host(h))
        .unwrap_or_else(|| "localhost:8080".to_string());
    format!("{proto}://{host}")
}

/// A host:port we're willing to echo into the page verbatim — letters, digits,
/// and the few punctuation marks a real authority uses. Anything else (spaces,
/// control bytes, quotes) falls back to the default.
fn is_plausible_host(h: &str) -> bool {
    !h.is_empty()
        && h.len() <= 255
        && h.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']'))
}

pub fn html_ok(body: String) -> Response<Body> {
    html_ok_bytes(bytes::Bytes::from(body))
}

/// Like [`html_ok`] but from already-rendered bytes — a cached-page serve is a
/// refcount bump, not a copy (see project_cache.rs).
fn html_ok_bytes(body: bytes::Bytes) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(body))
        .unwrap_or_else(not_found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_read_from_the_pep700_view_and_absence_is_inconclusive() {
        // The real view shape: versions present → authoritative membership.
        let view = br#"{"meta":{"api-version":"1.4"},"name":"pkg","versions":["1.0.0","1.2.0"],"files":[]}"#;
        assert_eq!(
            versions_from_package_index(view).as_deref(),
            Some(&["1.0.0".to_string(), "1.2.0".to_string()][..])
        );
        // A pre-PEP 700 body (no `versions`) and junk are both inconclusive —
        // the caller must widen to the scan, never 404 off missing evidence.
        assert_eq!(
            versions_from_package_index(br#"{"meta":{"api-version":"1.0"},"files":[]}"#),
            None
        );
        assert_eq!(versions_from_package_index(b"not json"), None);
    }

    #[test]
    fn rank_packages_rolls_up_files_and_ranks_busiest_first() {
        use std::collections::BTreeMap;
        let day = |entries: &[(&str, u64)]| counters::DaySummary {
            total: entries.iter().map(|(_, c)| c).sum(),
            top: entries.iter().map(|(k, c)| (k.to_string(), *c)).collect(),
        };
        let mut summaries: BTreeMap<String, counters::DaySummary> = BTreeMap::new();
        summaries.insert(
            "2026-06-20".into(),
            day(&[
                ("requests/requests-2.31.0-py3-none-any.whl", 6),
                ("requests/requests-2.30.0-py3-none-any.whl", 4),
                ("flask/flask-3.0.0-py3-none-any.whl", 7),
            ]),
        );
        summaries.insert(
            "2026-06-21".into(),
            day(&[
                ("requests/requests-2.31.0-py3-none-any.whl", 5),
                ("zeta/zeta-1.0.0-py3-none-any.whl", 7),
            ]),
        );
        // requests rolls up across files AND days (6+4+5=15) and ranks first;
        // flask & zeta tie at 7, broken by name ascending (flask before zeta).
        assert_eq!(
            rank_packages(&summaries),
            vec![
                ("requests".to_string(), 15),
                ("flask".to_string(), 7),
                ("zeta".to_string(), 7),
            ]
        );
    }
}
