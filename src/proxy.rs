//! On-demand mirroring: serve unknown packages from an upstream simple index,
//! caching artifacts in storage on first download.
//!
//! This is `sync`, made lazy. The same rules hold: the origin model is the
//! dependency-confusion defense, so a name claimed `private` (or inside
//! `--private-prefix`) never falls through to upstream, and the first
//! upstream artifact write claims the name `mirror` — atomically, exactly as
//! `sync` does. Artifacts are immutable, so caching them is trivially
//! correct; only the package *listing* needs freshness, and it is fetched
//! from the upstream PEP 691 API (which carries PEP 700 upload times, so
//! `--exclude-newer` stays historically correct) and cached for
//! [`LISTING_TTL`].
//!
//! Package pages are rendered from the upstream listing with our own
//! `/files/` URLs; artifact GETs download-verify-commit through the upload
//! spool (bounded memory, whatever the wheel size), then fall through to the
//! normal serving path. PEP 658 companions for not-yet-cached wheels are
//! passed through from upstream without writing anything — a resolver
//! probing dozens of candidate wheels must not stampede gigabytes into
//! storage. When upstream is down, callers fall back to the local
//! materialized index: already-cached packages keep installing.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use futures::StreamExt;
use pep440_rs::{Version, VersionSpecifiers};
use reqwest::Client;
use tracing::{info, warn};

use crate::names::{infer_version_from_filename, matches_prefix};
use crate::origin;
use crate::render::{self, FileMetadata};
use crate::sidecar::{
    metadata_key, provenance_key, sidecar_key, Sidecar, METADATA_SUFFIX, PROVENANCE_SUFFIX,
};
use crate::simple::{self, SimpleFile};
use crate::sync::{matches_filters, ResolvedFilter};
use crate::upload::{FinishedSpool, UploadSpool};
use crate::{AppState, PACKAGES_PREFIX};

/// How long an upstream package listing (or its absence) is reused before
/// refetching. Bounds the lag for "a new release appeared upstream"; the
/// artifacts themselves are immutable and cached forever.
const LISTING_TTL: Duration = Duration::from_secs(60);
/// Hard ceiling on cached listings. Each `/simple/:pkg` miss against a proxy
/// upstream inserts one entry (including negative `Missing` ones for 404s), and
/// there are unbounded distinct normalized names, so without a cap a stream of
/// nonexistent-package requests grows the map until OOM.
const MAX_LISTINGS: usize = 8192;
/// Listing and metadata fetches are small; bound them hard.
const SMALL_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
/// Same retry budget as `sync`: at CDN scale, transient errors are routine.
const DOWNLOAD_ATTEMPTS: u32 = 3;

/// A package page rendered from the upstream listing, ETag precomputed.
#[derive(Clone)]
pub struct RenderedIndex {
    pub body: bytes::Bytes,
    pub etag: Arc<str>,
}

fn rendered(body: String) -> RenderedIndex {
    RenderedIndex {
        etag: crate::cache::quoted_sha256(body.as_bytes()),
        body: bytes::Bytes::from(body),
    }
}

/// Upstream listing, filtered and pre-rendered. Rendering happens once per
/// fill, so the per-request cost of a proxied page is a map lookup.
struct Found {
    files: Vec<SimpleFile>,
    html: RenderedIndex,
    json: RenderedIndex,
}

enum Listing {
    Found(Arc<Found>),
    /// Upstream said 404 — cached as hard as a hit, or a stampede of typo'd
    /// installs becomes an upstream hammer.
    Missing,
}

struct CacheEntry {
    listing: Listing,
    fetched: Instant,
}

pub struct Proxy {
    upstream: String,
    filter: ResolvedFilter,
    /// The package scope as a fast name → version-constraints lookup, derived
    /// once from `filter.packages`. `None` means no scope is configured (serve
    /// any non-private name — the open-proxy default). A present map is a
    /// fail-closed allowlist: a name absent from it never falls through, and a
    /// name's constraints gate which versions are served. A name may carry
    /// several constraints (duplicate list entries); a version passes if any
    /// allows it, matching `sync`'s union semantics.
    scope: Option<HashMap<String, Vec<Option<VersionSpecifiers>>>>,
    client: Client,
    listings: Mutex<HashMap<String, CacheEntry>>,
    /// Single-flight guard: at most one in-flight download per artifact key.
    /// Without it, N concurrent GETs for the same uncached wheel each stream a
    /// full copy into N separate spool files — an anonymous client could
    /// amplify one request for a large wheel into N full-size downloads
    /// (disk-fill + upstream bandwidth). The map self-prunes (see
    /// [`DownloadSlot`]), so it stays bounded by live concurrency, not by the
    /// number of distinct artifacts ever proxied.
    inflight: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

/// Held for the whole download-verify-commit of one artifact key. Dropping it
/// removes the map entry once no other task is waiting on that key, keeping
/// [`Proxy::inflight`] bounded.
struct DownloadSlot {
    inflight: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    key: String,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for DownloadSlot {
    fn drop(&mut self) {
        let mut map = self.inflight.lock().expect("inflight mutex poisoned");
        if let Some(lock) = map.get(&self.key) {
            // We still hold `_guard` (one strong ref) and the map holds one.
            // Any count beyond that is a task already waiting on this key, so
            // only collapse the entry when we are its last user.
            if Arc::strong_count(lock) <= 2 {
                map.remove(&self.key);
            }
        }
    }
}

/// May this package be served from upstream at all? Private names, the reserved
/// prefix, and (when a scope is configured) names outside the allowlist never
/// fall through — that is the entire defense.
pub async fn eligible(state: &AppState, pkg: &str) -> Result<bool> {
    if let Some(prefix) = &state.private_prefix {
        if matches_prefix(pkg, prefix) {
            return Ok(false);
        }
    }
    // The package allowlist is fail-closed and pure (no I/O), so it gates before
    // the origin read — an unapproved name never even touches storage.
    if let Some(proxy) = &state.proxy {
        if !proxy.name_in_scope(pkg) {
            return Ok(false);
        }
    }
    match origin::read_origin(state.storage.as_ref(), pkg).await? {
        Some(owner) if owner == origin::PRIVATE => Ok(false),
        _ => Ok(true),
    }
}

/// Whether a file's inferred version satisfies a name's scope constraints. A
/// bare entry (no specifiers) allows every version; otherwise the version must
/// parse and match at least one specifier — a file whose version can't be
/// parsed can't be proven to match, so it's dropped (the same conservative rule
/// `sync` applies).
fn version_allowed(constraints: &[Option<VersionSpecifiers>], filename: &str) -> bool {
    if constraints.iter().any(Option::is_none) {
        return true;
    }
    let Some(version) =
        infer_version_from_filename(filename).and_then(|v| Version::from_str(&v).ok())
    else {
        return false;
    };
    constraints
        .iter()
        .flatten()
        .any(|specifiers| specifiers.contains(&version))
}

impl Proxy {
    pub fn new(upstream: &str, filter: ResolvedFilter) -> Result<Self> {
        let upstream = upstream.trim_end_matches('/').to_string();
        if !upstream.starts_with("http://") && !upstream.starts_with("https://") {
            bail!("--proxy-upstream must be an http(s) URL, got '{upstream}'");
        }
        // Derive the request-time allowlist index once. Empty scope → None, so
        // name_in_scope() short-circuits to "allow all" (the open-proxy default).
        let scope = (!filter.packages.is_empty()).then(|| {
            let mut map: HashMap<String, Vec<Option<VersionSpecifiers>>> = HashMap::new();
            for spec in &filter.packages {
                map.entry(spec.name.clone())
                    .or_default()
                    .push(spec.specifiers.clone());
            }
            map
        });
        Ok(Self {
            upstream,
            filter,
            scope,
            client: Client::builder()
                .user_agent(
                    "pypiron-proxy/0.1 (+https://github.com/blackthorn-interstellar/pypiron)",
                )
                .connect_timeout(Duration::from_secs(10))
                // Inactivity timeout between reads, reset on each chunk: an
                // upstream that connects then stalls mid-stream can't hang a
                // client-facing request forever. Does NOT bound large downloads
                // that keep streaming. download_verified's retry loop turns the
                // resulting error into a clean retry.
                .read_timeout(Duration::from_secs(30))
                .build()?,
            listings: Mutex::new(HashMap::new()),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Acquire the single-flight slot for an artifact key, waiting if another
    /// task is already downloading it. Held until the returned guard drops.
    async fn acquire_download_slot(&self, key: &str) -> DownloadSlot {
        let lock = self
            .inflight
            .lock()
            .expect("inflight mutex poisoned")
            .entry(key.to_string())
            .or_default()
            .clone();
        let guard = lock.lock_owned().await;
        DownloadSlot {
            inflight: self.inflight.clone(),
            key: key.to_string(),
            _guard: guard,
        }
    }

    /// Is this (PEP 503-normalized) name allowed to fall through to upstream?
    /// True when no scope is configured; otherwise true only if the name is on
    /// the allowlist. The version axis is enforced separately, per file, in the
    /// listing.
    pub fn name_in_scope(&self, pkg: &str) -> bool {
        self.scope.as_ref().is_none_or(|m| m.contains_key(pkg))
    }

    /// Does this file satisfy the name's version constraints? Allowed when no
    /// scope is configured; otherwise the name's constraints gate the version.
    /// A name in scope is reached here only after [`name_in_scope`], so a miss
    /// in the map can't normally happen — treated fail-closed if it does.
    fn version_in_scope(&self, pkg: &str, filename: &str) -> bool {
        match self.scope.as_ref() {
            None => true,
            Some(map) => map
                .get(pkg)
                .is_some_and(|constraints| version_allowed(constraints, filename)),
        }
    }

    pub fn upstream(&self) -> &str {
        &self.upstream
    }

    /// The package page rendered from the upstream listing; `None` means
    /// "serve the local index instead" (upstream 404 or unreachable).
    pub async fn package_index(
        &self,
        state: &AppState,
        pkg: &str,
        json: bool,
    ) -> Option<RenderedIndex> {
        let found = self.listing(state, pkg).await?;
        Some(if json {
            found.json.clone()
        } else {
            found.html.clone()
        })
    }

    /// Download-verify-commit one artifact on a local miss. `Ok(())` always
    /// falls through to normal serving — including when the file simply
    /// doesn't exist upstream (the local 404 is the right answer). `Err` is
    /// a hard failure (storage outage, exhausted verification retries).
    pub async fn ensure_artifact_cached(
        &self,
        state: &AppState,
        pkg: &str,
        filename: &str,
    ) -> Result<()> {
        let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        if state.storage.head_exists(&key).await? {
            return Ok(());
        }
        // Serialize concurrent fetches of the *same* artifact (distinct files
        // still download in parallel). The slot is held until this function
        // returns; a racer that loses the race re-checks below and finds the
        // file already cached instead of downloading its own copy.
        let _slot = self.acquire_download_slot(&key).await;
        if state.storage.head_exists(&key).await? {
            return Ok(());
        }
        let Some(found) = self.listing(state, pkg).await else {
            return Ok(());
        };
        let Some(file) = found.files.iter().find(|f| f.filename == filename) else {
            return Ok(()); // not upstream, or filtered out
        };

        // Claim before writing, exactly like sync: atomically, so a racing
        // first private upload can't merge worlds. Losing to a private claim
        // means this name is no longer ours to serve.
        let mut claimed_now = false;
        match origin::read_origin(state.storage.as_ref(), pkg).await? {
            Some(owner) if owner == origin::MIRROR => {}
            Some(_) => return Ok(()),
            None => {
                let (created, winner) =
                    origin::claim_origin(state.storage.as_ref(), pkg, origin::MIRROR).await?;
                if winner != origin::MIRROR {
                    return Ok(());
                }
                // Only the creator may later release this claim; a racer that
                // merely read back our peer's fresh MIRROR claim must not.
                claimed_now = created;
            }
        }

        info!(%pkg, %filename, upstream = %self.upstream, "proxy: caching artifact");
        let spool = match self.download_verified(state, pkg, file).await {
            Ok(spool) => spool,
            Err(e) => {
                state
                    .metrics
                    .proxy_artifact_errors
                    .fetch_add(1, Ordering::Relaxed);
                // A claim with nothing behind it would block the name forever.
                if claimed_now {
                    origin::release_empty_claim(state.storage.as_ref(), pkg).await;
                }
                return Err(e);
            }
        };

        // Intent before truth, commit after (see worker.rs): a crash between
        // the artifact landing and the commit marker heals via stale intent.
        let intent_nonce = crate::worker::mark_intent(state.storage.as_ref(), pkg)
            .await
            .ok();

        // Ordering invariant: artifact, then companion, then sidecar, then
        // commit marker — a listed-but-missing file is the only harmful state.
        state
            .storage
            .put_file_if_absent(&key, spool.path.path(), Some("application/octet-stream"))
            .await?;
        if filename.ends_with(".whl") && file.has_core_metadata() {
            // Best-effort, like sync: a missing companion only costs the
            // resolver a wheel download.
            if let Some(md) = self.fetch_metadata_url(pkg, &file.url).await {
                let _ = state
                    .storage
                    .put_bytes(
                        &metadata_key(&key),
                        md.to_vec(),
                        Some("text/plain; charset=utf-8"),
                    )
                    .await;
            }
        }
        if let Some(prov_url) = &file.provenance {
            // PEP 740 provenance, relayed verbatim alongside the artifact.
            // Best-effort like metadata: a missing companion only drops the
            // supply-chain signal, never the artifact.
            if let Some(prov) = self.fetch_provenance_url(pkg, prov_url).await {
                let _ = state
                    .storage
                    .put_bytes(
                        &provenance_key(&key),
                        prov.to_vec(),
                        Some("application/json"),
                    )
                    .await;
            }
        }
        let sidecar = Sidecar {
            // Upstream's digest, verified against the downloaded bytes.
            sha256: spool.sha256.clone(),
            size: spool.size,
            version: infer_version_from_filename(filename).unwrap_or_default(),
            // Upstream's true upload time: what keeps --exclude-newer honest.
            upload_time: file.upload_time.clone().unwrap_or_default(),
            requires_python: file.requires_python.clone(),
            yanked: file.yanked.clone(),
        };
        state
            .storage
            .put_bytes(
                &sidecar_key(&key),
                serde_json::to_vec(&sidecar)?,
                Some("application/json"),
            )
            .await?;
        crate::commit_marker(state, pkg, intent_nonce).await?;
        state
            .metrics
            .proxy_artifacts_cached
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// PEP 658 companion for a not-yet-cached wheel, fetched from upstream
    /// and served without storage writes. `None` falls back to a local 404.
    pub async fn fetch_metadata(
        &self,
        state: &AppState,
        pkg: &str,
        metadata_filename: &str,
    ) -> Option<bytes::Bytes> {
        let base = metadata_filename.strip_suffix(METADATA_SUFFIX)?;
        let found = self.listing(state, pkg).await?;
        let file = found.files.iter().find(|f| f.filename == base)?;
        if !file.has_core_metadata() {
            return None;
        }
        self.fetch_metadata_url(pkg, &file.url).await
    }

    async fn fetch_metadata_url(&self, pkg: &str, file_url: &str) -> Option<bytes::Bytes> {
        let url = match self.resolve_url(pkg, file_url) {
            Ok(url) => format!("{url}{METADATA_SUFFIX}"),
            Err(e) => {
                warn!(%pkg, error=?e, "proxy: unresolvable upstream file URL");
                return None;
            }
        };
        let resp = self
            .client
            .get(&url)
            .timeout(SMALL_FETCH_TIMEOUT)
            .send()
            .await
            .and_then(|r| r.error_for_status());
        match resp {
            Ok(resp) => read_capped(resp, crate::wheel::MAX_METADATA_BYTES, &url).await,
            Err(e) => {
                warn!(%url, error=?e, "proxy: upstream metadata fetch failed");
                None
            }
        }
    }

    /// PEP 740 provenance for a not-yet-cached file, fetched from upstream and
    /// served without storage writes. `None` falls back to a local 404.
    pub async fn fetch_provenance(
        &self,
        state: &AppState,
        pkg: &str,
        provenance_filename: &str,
    ) -> Option<bytes::Bytes> {
        let base = provenance_filename.strip_suffix(PROVENANCE_SUFFIX)?;
        let found = self.listing(state, pkg).await?;
        let file = found.files.iter().find(|f| f.filename == base)?;
        let prov_url = file.provenance.as_ref()?;
        self.fetch_provenance_url(pkg, prov_url).await
    }

    async fn fetch_provenance_url(&self, pkg: &str, prov_url: &str) -> Option<bytes::Bytes> {
        // The upstream provenance URL is authoritative (absolute on PyPI), but
        // resolve relative ones against the index page just like file URLs.
        let url = match self.resolve_url(pkg, prov_url) {
            Ok(url) => url,
            Err(e) => {
                warn!(%pkg, error=?e, "proxy: unresolvable upstream provenance URL");
                return None;
            }
        };
        let resp = self
            .client
            .get(url.clone())
            .timeout(SMALL_FETCH_TIMEOUT)
            .send()
            .await
            .and_then(|r| r.error_for_status());
        match resp {
            Ok(resp) => read_capped(resp, crate::wheel::MAX_METADATA_BYTES, url.as_str()).await,
            Err(e) => {
                warn!(%url, error=?e, "proxy: upstream provenance fetch failed");
                None
            }
        }
    }

    /// The filtered upstream listing for `pkg`, served from cache within
    /// [`LISTING_TTL`]. On upstream errors a stale entry is reused for one
    /// more TTL (already-resolved installs keep working through blips);
    /// with nothing to reuse the package is treated as missing for one TTL,
    /// so a dead upstream degrades to local-only instead of a per-request
    /// timeout.
    async fn listing(&self, state: &AppState, pkg: &str) -> Option<Arc<Found>> {
        if let Some(cached) = self.cached_listing(pkg, false) {
            return cached;
        }
        state
            .metrics
            .proxy_listing_fetches
            .fetch_add(1, Ordering::Relaxed);
        let listing = match self.fetch_listing(pkg).await {
            Ok(listing) => listing,
            Err(e) => {
                state
                    .metrics
                    .proxy_listing_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!(%pkg, upstream = %self.upstream, error=?e, "proxy: upstream listing fetch failed");
                if let Some(stale) = self.cached_listing(pkg, true) {
                    return stale;
                }
                Listing::Missing
            }
        };
        let result = match &listing {
            Listing::Found(found) => Some(found.clone()),
            Listing::Missing => None,
        };
        let mut map = self.listings.lock().expect("listing lock poisoned");
        if map.len() >= MAX_LISTINGS && !map.contains_key(pkg) {
            evict_listings(&mut map);
        }
        map.insert(
            pkg.to_string(),
            CacheEntry {
                listing,
                fetched: Instant::now(),
            },
        );
        result
    }

    /// Cached listing lookup. `revive` refreshes the entry's timestamp and
    /// ignores expiry — the stale-on-upstream-error path.
    fn cached_listing(&self, pkg: &str, revive: bool) -> Option<Option<Arc<Found>>> {
        let mut map = self.listings.lock().expect("listing lock poisoned");
        let entry = map.get_mut(pkg)?;
        if revive {
            entry.fetched = Instant::now();
        } else if entry.fetched.elapsed() >= LISTING_TTL {
            return None;
        }
        Some(match &entry.listing {
            Listing::Found(found) => Some(found.clone()),
            Listing::Missing => None,
        })
    }

    async fn fetch_listing(&self, pkg: &str) -> Result<Listing> {
        let Some(index) =
            simple::fetch_index(&self.client, &self.upstream, pkg, Some(SMALL_FETCH_TIMEOUT))
                .await?
        else {
            return Ok(Listing::Missing);
        };
        // Relay the upstream PEP 792 status verbatim (default active). An
        // upstream-quarantined project returns no files anyway, so the marker
        // rides along with a naturally empty listing.
        let status = index.project_status.clone().unwrap_or_default();
        let files: Vec<SimpleFile> = index
            .files
            .into_iter()
            // No digest, no service: every artifact we hand out is verifiable.
            .filter(|f| f.sha256().is_some())
            .filter(|f| matches_filters(f, &self.filter))
            // The scope's version axis: a pinned/ranged allowlist entry serves
            // only matching versions, exactly as `sync` mirrors only matching
            // versions. No scope → kept.
            .filter(|f| self.version_in_scope(pkg, &f.filename))
            .collect();
        let metas: Vec<FileMetadata> = files.iter().map(SimpleFile::as_file_metadata).collect();
        let render_metas: &[FileMetadata] = if status.status.blocks_downloads() {
            &[]
        } else {
            &metas
        };
        Ok(Listing::Found(Arc::new(Found {
            html: rendered(render::pep503_package_html(pkg, render_metas, &status)),
            json: rendered(render::pep691_package_json(pkg, render_metas, &status)),
            files,
        })))
    }

    /// Stream the artifact to the upload spool (hashing on the way) and
    /// verify it against the upstream digest; same retry budget as sync —
    /// a truncated body and a flaky CDN look identical.
    async fn download_verified(
        &self,
        state: &AppState,
        pkg: &str,
        file: &SimpleFile,
    ) -> Result<FinishedSpool> {
        let expected = file
            .sha256()
            .ok_or_else(|| anyhow!("no upstream sha256 for {}", file.filename))?;
        let url = self.resolve_url(pkg, &file.url)?;
        let mut last_err = None;
        for attempt in 1..=DOWNLOAD_ATTEMPTS {
            match self.download_once(state, &url, file).await {
                Ok(spool) if spool.sha256.eq_ignore_ascii_case(expected) => return Ok(spool),
                Ok(spool) => {
                    last_err = Some(anyhow!(
                        "sha256 mismatch for {} (expected {expected}, got {})",
                        file.filename,
                        spool.sha256
                    ));
                }
                Err(e) => last_err = Some(e),
            }
            if attempt < DOWNLOAD_ATTEMPTS {
                warn!(file=%file.filename, attempt, "proxy: download failed; retrying");
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
            }
        }
        Err(last_err.expect("at least one attempt"))
    }

    async fn download_once(
        &self,
        state: &AppState,
        url: &reqwest::Url,
        file: &SimpleFile,
    ) -> Result<FinishedSpool> {
        let resp = self
            .client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?;
        let mut spool = UploadSpool::new(&state.spool_dir).await?;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            spool.write_chunk(&chunk?).await?;
            // Mirror sync::download_once: abort a body that overruns its
            // upstream-declared size before it can fill the disk (the read
            // timeout bounds time, not size, and an overrun fails the sha
            // check anyway). No declared size → no cap, same as sync.
            if let Some(max) = file.size {
                if spool.size() > max {
                    bail!(
                        "{} overran its declared size ({} > {max} bytes)",
                        file.filename,
                        spool.size()
                    );
                }
            }
        }
        spool.finish().await
    }

    /// PEP 691 file URLs may be absolute or relative; relative ones resolve
    /// against the index page URL (RFC 3986), which `Url::join` implements.
    fn resolve_url(&self, pkg: &str, raw: &str) -> Result<reqwest::Url> {
        let base = reqwest::Url::parse(&format!("{}/simple/{pkg}/", self.upstream))?;
        Ok(base.join(raw)?)
    }
}

/// Read an upstream companion body into memory with a hard ceiling. The local
/// wheel extractor already bounds `.metadata` at 16 MiB; the passthrough/cache
/// paths must too, or a hostile/huge upstream `.metadata`/`.provenance` body
/// (the timeout bounds time, not size) OOMs the node. `None` on overflow or a
/// read error — both fall back to a local 404, which is the existing contract.
async fn read_capped(resp: reqwest::Response, max: u64, url: &str) -> Option<bytes::Bytes> {
    let declared = resp.content_length();
    if declared.is_some_and(|len| len > max) {
        warn!(%url, max, "proxy: upstream body exceeds cap (Content-Length)");
        return None;
    }
    let mut buf: Vec<u8> = Vec::with_capacity(declared.map_or(0, |l| l.min(max)) as usize);
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                warn!(%url, error=?e, "proxy: upstream body read failed");
                return None;
            }
        };
        if buf.len() as u64 + chunk.len() as u64 > max {
            warn!(%url, max, "proxy: upstream body exceeds cap");
            return None;
        }
        buf.extend_from_slice(&chunk);
    }
    Some(bytes::Bytes::from(buf))
}

/// Keep the listings cache bounded: first drop everything past its TTL (those
/// would be re-fetched anyway), and if that didn't free a slot, evict the
/// oldest entries down to half the cap so this stays amortized O(1) per insert.
fn evict_listings(map: &mut HashMap<String, CacheEntry>) {
    map.retain(|_, e| e.fetched.elapsed() < LISTING_TTL);
    if map.len() < MAX_LISTINGS {
        return;
    }
    let mut by_age: Vec<(String, Instant)> =
        map.iter().map(|(k, e)| (k.clone(), e.fetched)).collect();
    by_age.sort_by_key(|(_, fetched)| *fetched);
    for (k, _) in by_age.into_iter().take(MAX_LISTINGS / 2) {
        map.remove(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_and_absolute_upstream_urls_resolve() {
        let proxy = Proxy::new("https://pypi.org/", ResolvedFilter::default()).unwrap();
        assert_eq!(proxy.upstream(), "https://pypi.org");
        let abs = proxy
            .resolve_url("six", "https://files.pythonhosted.org/p/six.whl")
            .unwrap();
        assert_eq!(abs.as_str(), "https://files.pythonhosted.org/p/six.whl");
        let host_rel = proxy.resolve_url("six", "/files/six/six.whl").unwrap();
        assert_eq!(host_rel.as_str(), "https://pypi.org/files/six/six.whl");
        let page_rel = proxy.resolve_url("six", "six.whl").unwrap();
        assert_eq!(page_rel.as_str(), "https://pypi.org/simple/six/six.whl");
    }

    #[test]
    fn non_http_upstream_is_rejected() {
        let err = Proxy::new("ftp://mirror", ResolvedFilter::default())
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("http(s)"));
    }

    fn spec(name: &str, specifiers: Option<&str>) -> crate::sync::PackageSpec {
        crate::sync::PackageSpec {
            name: name.to_string(),
            specifiers: specifiers.map(|s| VersionSpecifiers::from_str(s).unwrap()),
        }
    }

    #[test]
    fn empty_scope_allows_every_name_and_version() {
        let proxy = Proxy::new("https://pypi.org", ResolvedFilter::default()).unwrap();
        assert!(proxy.name_in_scope("anything"));
        assert!(proxy.version_in_scope("anything", "anything-1.0.0.tar.gz"));
    }

    #[test]
    fn scope_gates_names_fail_closed() {
        let filter = ResolvedFilter {
            packages: vec![spec("requests", Some(">=2.20,<3")), spec("numpy", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", filter).unwrap();
        assert!(proxy.name_in_scope("requests"));
        assert!(proxy.name_in_scope("numpy"));
        assert!(
            !proxy.name_in_scope("flask"),
            "unapproved name must be denied"
        );
    }

    #[test]
    fn scope_gates_versions_like_sync() {
        let filter = ResolvedFilter {
            packages: vec![spec("requests", Some(">=2.20,<3")), spec("numpy", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", filter).unwrap();
        // Pinned name: only versions inside the range pass.
        assert!(proxy.version_in_scope("requests", "requests-2.31.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("requests", "requests-2.10.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("requests", "requests-3.0.0-py3-none-any.whl"));
        // Unparseable version under a constraint can't be proven to match → dropped.
        assert!(!proxy.version_in_scope("requests", "requests-garbage.whl"));
        // Bare (unpinned) name: every version passes, even an unparseable one.
        assert!(proxy.version_in_scope("numpy", "numpy-1.26.0-cp311-cp311-linux_x86_64.whl"));
        assert!(proxy.version_in_scope("numpy", "numpy-whatever.tar.gz"));
    }

    #[test]
    fn duplicate_entries_union_their_ranges() {
        // Two constrained entries for one name: a version matching either passes.
        let filter = ResolvedFilter {
            packages: vec![spec("foo", Some("==1.0")), spec("foo", Some("==3.0"))],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", filter).unwrap();
        assert!(proxy.version_in_scope("foo", "foo-1.0-py3-none-any.whl"));
        assert!(proxy.version_in_scope("foo", "foo-3.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("foo", "foo-2.0-py3-none-any.whl"));
    }
}
