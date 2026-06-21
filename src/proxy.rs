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
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use clap::Args;
use futures::StreamExt;
use reqwest::Client;
use tracing::{info, warn};

use crate::names::{infer_version_from_filename, matches_prefix};
use crate::origin;
use crate::render::{self, FileMetadata};
use crate::sidecar::{
    metadata_key, provenance_key, sidecar_key, Sidecar, METADATA_SUFFIX, PROVENANCE_SUFFIX,
};
use crate::simple::{self, SimpleFile};
use crate::sync::{matches_filters, parse_cutoff, parse_min_python, ResolvedFilter};
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

/// Filters gating what the proxy serves and caches — the `sync` filters under
/// a `--proxy-` prefix, with identical semantics (they gate what this server
/// *adds*; nothing cached is ever removed by a filter change).
#[derive(Debug, Clone, Default, Args)]
pub struct ProxyFilterArgs {
    /// Only proxy wheel files (.whl)
    #[arg(
        long = "proxy-only-wheels",
        env = "PYPIRON_PROXY_ONLY_WHEELS",
        conflicts_with = "only_sdists"
    )]
    pub only_wheels: bool,

    /// Only proxy source distributions (sdist)
    #[arg(long = "proxy-only-sdists", env = "PYPIRON_PROXY_ONLY_SDISTS")]
    pub only_sdists: bool,

    /// Include wheels whose python tag matches any of these (e.g. py3, cp311). Comma-separated or repeatable.
    #[arg(long = "proxy-python-tag", value_delimiter = ',', value_name = "TAG")]
    pub python_tag: Vec<String>,

    /// Include wheels whose ABI tag matches any of these (e.g. none, cp311). Comma-separated or repeatable.
    #[arg(long = "proxy-abi-tag", value_delimiter = ',', value_name = "TAG")]
    pub abi_tag: Vec<String>,

    /// Include wheels whose platform tag matches any of these (supports '*' wildcard).
    #[arg(long = "proxy-platform-tag", value_delimiter = ',', value_name = "TAG")]
    pub platform_tag: Vec<String>,

    /// Exclude wheels whose platform tag matches any of these (supports '*' wildcard).
    #[arg(
        long = "proxy-exclude-platform-tag",
        value_delimiter = ',',
        value_name = "TAG"
    )]
    pub exclude_platform_tag: Vec<String>,

    /// Only proxy files the upstream received before this cutoff. Accepts an
    /// RFC 3339 timestamp, a friendly duration ("30 days", "24 hours", "1 week"),
    /// or an ISO 8601 duration (P30D, PT24H), relative to now; no months/years.
    #[arg(
        long = "proxy-exclude-newer",
        env = "PYPIRON_PROXY_EXCLUDE_NEWER",
        value_name = "WHEN"
    )]
    pub exclude_newer: Option<String>,

    /// Only proxy files the upstream received at or after this cutoff. Same
    /// formats as --proxy-exclude-newer.
    #[arg(
        long = "proxy-exclude-older",
        env = "PYPIRON_PROXY_EXCLUDE_OLDER",
        value_name = "WHEN"
    )]
    pub exclude_older: Option<String>,

    /// Skip wheels built only for Python older than this floor (e.g. "3.10").
    /// Version-agnostic and abi3 wheels and all sdists are kept.
    #[arg(
        long = "proxy-min-python",
        env = "PYPIRON_PROXY_MIN_PYTHON",
        value_name = "X.Y"
    )]
    pub min_python: Option<String>,

    /// Skip PEP 440 dev releases (any version with a `.devN` segment).
    #[arg(long = "proxy-exclude-dev", env = "PYPIRON_PROXY_EXCLUDE_DEV")]
    pub exclude_dev: bool,

    /// Skip Windows artifacts: win* wheels and legacy .exe/.msi/.winXX installers.
    #[arg(long = "proxy-exclude-windows", env = "PYPIRON_PROXY_EXCLUDE_WINDOWS")]
    pub exclude_windows: bool,
}

impl ProxyFilterArgs {
    fn resolve(&self) -> Result<ResolvedFilter> {
        if self.only_wheels && self.only_sdists {
            // Would select nothing and "succeed" — a registry of 404s.
            bail!("proxy-only-wheels and proxy-only-sdists are mutually exclusive");
        }
        Ok(ResolvedFilter {
            only_wheels: self.only_wheels,
            only_sdists: self.only_sdists,
            python_tag: self.python_tag.clone(),
            abi_tag: self.abi_tag.clone(),
            platform_tag: self.platform_tag.clone(),
            exclude_platform_tag: self.exclude_platform_tag.clone(),
            exclude_newer: parse_cutoff("proxy-exclude-newer", self.exclude_newer.as_ref())?,
            exclude_older: parse_cutoff("proxy-exclude-older", self.exclude_older.as_ref())?,
            min_python: parse_min_python(self.min_python.as_deref())?,
            exclude_dev: self.exclude_dev,
            exclude_windows: self.exclude_windows,
        })
    }
}

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
    client: Client,
    listings: Mutex<HashMap<String, CacheEntry>>,
}

/// May this package be served from upstream at all? Private names and the
/// reserved prefix never fall through — that is the entire defense.
pub async fn eligible(state: &AppState, pkg: &str) -> Result<bool> {
    if let Some(prefix) = &state.private_prefix {
        if matches_prefix(pkg, prefix) {
            return Ok(false);
        }
    }
    match origin::read_origin(state.storage.as_ref(), pkg).await? {
        Some(owner) if owner == origin::PRIVATE => Ok(false),
        _ => Ok(true),
    }
}

impl Proxy {
    pub fn new(upstream: &str, filter: &ProxyFilterArgs) -> Result<Self> {
        let upstream = upstream.trim_end_matches('/').to_string();
        if !upstream.starts_with("http://") && !upstream.starts_with("https://") {
            bail!("--proxy-upstream must be an http(s) URL, got '{upstream}'");
        }
        Ok(Self {
            upstream,
            filter: filter.resolve()?,
            client: Client::builder()
                .user_agent("pypiron-proxy/0.1 (+https://github.com/brycedrennan/pypiron)")
                .connect_timeout(Duration::from_secs(10))
                // Inactivity timeout between reads, reset on each chunk: an
                // upstream that connects then stalls mid-stream can't hang a
                // client-facing request forever. Does NOT bound large downloads
                // that keep streaming. download_verified's retry loop turns the
                // resulting error into a clean retry.
                .read_timeout(Duration::from_secs(30))
                .build()?,
            listings: Mutex::new(HashMap::new()),
        })
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
            match self.download_once(state, &url).await {
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

    async fn download_once(&self, state: &AppState, url: &reqwest::Url) -> Result<FinishedSpool> {
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
        let proxy = Proxy::new("https://pypi.org/", &ProxyFilterArgs::default()).unwrap();
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
        let err = Proxy::new("ftp://mirror", &ProxyFilterArgs::default())
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("http(s)"));
    }
}
