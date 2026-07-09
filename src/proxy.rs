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
use std::net::{IpAddr, SocketAddr};
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
use crate::storage::Storage;
use crate::sync::{matches_mirror, ResolvedMirror};
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
    mirror: ResolvedMirror,
    /// The package scope as a fast name → version-constraints lookup, derived
    /// once from `mirror.include_packages`. `None` means no scope is configured (serve
    /// any non-private name — the open-proxy default). A present map is a
    /// fail-closed allowlist: a name absent from it never falls through, and a
    /// name's constraints gate which versions are served. A name may carry
    /// several constraints (duplicate list entries); a version passes if any
    /// allows it, matching `sync`'s union semantics.
    scope: Option<HashMap<String, Vec<Option<VersionSpecifiers>>>>,
    /// Package denylist as a fast name -> version-constraints lookup. `None`
    /// means no denylist. A bare entry (`None`) denies the whole project; a
    /// constrained entry denies only matching versions.
    deny: Option<HashMap<String, Vec<Option<VersionSpecifiers>>>>,
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
        let mut map = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
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

/// DNS resolver that refuses to hand back private, loopback, or link-local
/// addresses — the SSRF guard for the proxy's outbound fetches. Filtering at
/// resolve time (rather than validating a hostname up front) also closes the
/// DNS-rebind gap: reqwest connects to exactly the addresses returned here, on
/// the initial request and on every redirect hop. The configured upstream host
/// is exempt, so a self-hosted mirror on a private range still works.
struct SsrfGuardResolver {
    allow_host: Option<String>,
}

impl reqwest::dns::Resolve for SsrfGuardResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        let exempt = self.allow_host.as_deref() == Some(host.as_str());
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let filtered: Vec<SocketAddr> = addrs
                .filter(|addr| exempt || !is_forbidden_ip(&addr.ip()))
                .collect();
            if filtered.is_empty() {
                return Err(format!(
                    "refusing to connect to '{host}': resolves only to private/loopback addresses"
                )
                .into());
            }
            Ok(Box::new(filtered.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>)
        })
    }
}

/// Private, loopback, link-local, or otherwise non-routable — never a valid
/// target for an upstream fetch.
fn is_forbidden_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.octets()[0] == 0
                // Carrier-grade NAT (100.64.0.0/10).
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64)
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_forbidden_ip(&IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                // Unique-local fc00::/7.
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // Link-local fe80::/10.
                || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

/// May this package be served from upstream at all? Private names, the reserved
/// prefix, and (when a scope is configured) names outside the allowlist never
/// fall through — that is the entire defense.
pub async fn eligible(state: &AppState, storage: &dyn Storage, pkg: &str) -> Result<bool> {
    if let Some(prefix) = &state.private_prefix {
        if matches_prefix(pkg, prefix) {
            return Ok(false);
        }
    }
    // The package allowlist is fail-closed and pure (no I/O), so it gates before
    // the origin read — an unapproved name never even touches storage.
    if let Some(proxy) = &state.proxy {
        if proxy.name_fully_denied(pkg) {
            return Ok(false);
        }
        if !proxy.name_in_scope(pkg) {
            return Ok(false);
        }
    }
    match origin::read_origin(storage, pkg).await? {
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
    pub fn new(upstream: &str, mirror: ResolvedMirror, allow_insecure: bool) -> Result<Self> {
        let upstream = upstream.trim_end_matches('/').to_string();
        // Plaintext http:// lets a network MITM forge both the artifact bytes and
        // the sha256 we check them against (they arrive over the same channel), so
        // the hash stops being a control. Refuse it unless explicitly overridden.
        if upstream.starts_with("http://") {
            if !allow_insecure {
                bail!(
                    "--proxy-upstream is plaintext http://, which lets a network MITM forge \
                     artifact hashes; pass --allow-insecure-upstream to override, got '{upstream}'"
                );
            }
        } else if !upstream.starts_with("https://") {
            bail!("--proxy-upstream must be an https URL, got '{upstream}'");
        }
        // The upstream host is operator-chosen and trusted, so it is exempt from
        // the SSRF guard below — an internal/self-hosted mirror on a private range
        // must still work.
        let allow_host = reqwest::Url::parse(&upstream)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string));
        // Derive the request-time allowlist index once. Empty scope → None, so
        // name_in_scope() short-circuits to "allow all" (the open-proxy default).
        let scope = (!mirror.include_packages.is_empty()).then(|| {
            let mut map: HashMap<String, Vec<Option<VersionSpecifiers>>> = HashMap::new();
            for spec in &mirror.include_packages {
                map.entry(spec.name.clone())
                    .or_default()
                    .push(spec.specifiers.clone());
            }
            map
        });
        let deny = (!mirror.exclude_packages.is_empty()).then(|| {
            let mut map: HashMap<String, Vec<Option<VersionSpecifiers>>> = HashMap::new();
            for spec in &mirror.exclude_packages {
                map.entry(spec.name.clone())
                    .or_default()
                    .push(spec.specifiers.clone());
            }
            map
        });
        Ok(Self {
            upstream,
            mirror,
            scope,
            deny,
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
                // Refuse to connect to private/loopback/link-local addresses. A
                // malicious or MITM'd upstream listing can point a companion URL
                // (.metadata/.provenance — unlike artifacts, not hash-gated) at an
                // internal endpoint and read the response back through us.
                .dns_resolver(Arc::new(SsrfGuardResolver { allow_host }))
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
            .unwrap_or_else(|e| e.into_inner())
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

    pub fn name_fully_denied(&self, pkg: &str) -> bool {
        self.deny
            .as_ref()
            .and_then(|m| m.get(pkg))
            .is_some_and(|constraints| constraints.iter().any(Option::is_none))
    }

    /// Does this file satisfy the name's version constraints? Allowed when no
    /// scope is configured; otherwise the name's constraints gate the version.
    /// A name in scope is reached here only after [`name_in_scope`], so a miss
    /// in the map can't normally happen — treated fail-closed if it does.
    fn version_in_scope(&self, pkg: &str, filename: &str) -> bool {
        let allowed = match self.scope.as_ref() {
            None => true,
            Some(map) => map
                .get(pkg)
                .is_some_and(|constraints| version_allowed(constraints, filename)),
        };
        if !allowed {
            return false;
        }
        if let Some(constraints) = self.deny.as_ref().and_then(|m| m.get(pkg)) {
            if version_allowed(constraints, filename) {
                return false;
            }
        }
        true
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
        storage: &dyn Storage,
        pkg: &str,
        filename: &str,
    ) -> Result<()> {
        let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        if storage.head_exists(&key).await? {
            return Ok(());
        }
        // Serialize concurrent fetches of the *same* artifact (distinct files
        // still download in parallel). The slot is held until this function
        // returns; a racer that loses the race re-checks below and finds the
        // file already cached instead of downloading its own copy.
        let _slot = self.acquire_download_slot(&key).await;
        if storage.head_exists(&key).await? {
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
        match origin::read_origin(storage, pkg).await? {
            Some(owner) if owner == origin::MIRROR => {}
            Some(_) => return Ok(()),
            None => {
                let (created, winner) = origin::claim_origin(storage, pkg, origin::MIRROR).await?;
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
                    origin::release_empty_claim(storage, pkg).await;
                }
                return Err(e);
            }
        };

        // Intent before truth, commit after (see worker.rs): a crash between
        // the artifact landing and the commit marker heals via stale intent.
        let intent_nonce = crate::worker::mark_intent(storage, pkg).await.ok();

        // Ordering invariant: artifact, then companion, then sidecar, then
        // commit marker — a listed-but-missing file is the only harmful state.
        storage
            .put_file_if_absent(&key, spool.path.path(), Some("application/octet-stream"))
            .await?;
        if filename.ends_with(".whl") && file.has_core_metadata() {
            // Best-effort, like sync: a missing companion only costs the
            // resolver a wheel download.
            if let Some(md) = self.fetch_metadata_url(pkg, &file.url).await {
                let _ = storage
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
                let _ = storage
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
        storage
            .put_bytes(
                &sidecar_key(&key),
                serde_json::to_vec(&sidecar)?,
                Some("application/json"),
            )
            .await?;
        crate::commit_marker(state, storage, pkg, intent_nonce).await?;
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
        let mut map = self.listings.lock().unwrap_or_else(|e| e.into_inner());
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
        let mut map = self.listings.lock().unwrap_or_else(|e| e.into_inner());
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
            .filter(|f| matches_mirror(f, &self.mirror))
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
        let proxy = Proxy::new("https://pypi.org/", ResolvedMirror::default(), false).unwrap();
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
        let err = Proxy::new("ftp://mirror", ResolvedMirror::default(), false)
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("https"));
    }

    #[test]
    fn plaintext_http_upstream_needs_opt_in() {
        // Off by default: http:// lets a MITM forge the hashes we verify against.
        let err = Proxy::new("http://mirror.internal", ResolvedMirror::default(), false)
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("allow-insecure-upstream"));
        // Explicit opt-in accepts it.
        Proxy::new("http://mirror.internal", ResolvedMirror::default(), true).unwrap();
    }

    #[test]
    fn ssrf_guard_blocks_private_addresses() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        for ip in [
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)),
        ] {
            assert!(is_forbidden_ip(&ip), "{ip} should be blocked");
        }
        for ip in [
            IpAddr::V4(Ipv4Addr::new(151, 101, 0, 223)), // files.pythonhosted.org
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            IpAddr::V6(Ipv6Addr::new(0x2606, 0x4700, 0, 0, 0, 0, 0, 1)),
        ] {
            assert!(!is_forbidden_ip(&ip), "{ip} should be allowed");
        }
    }

    fn spec(name: &str, specifiers: Option<&str>) -> crate::sync::PackageSpec {
        crate::sync::PackageSpec {
            name: name.to_string(),
            specifiers: specifiers.map(|s| VersionSpecifiers::from_str(s).unwrap()),
        }
    }

    #[test]
    fn empty_scope_allows_every_name_and_version() {
        let proxy = Proxy::new("https://pypi.org", ResolvedMirror::default(), false).unwrap();
        assert!(proxy.name_in_scope("anything"));
        assert!(proxy.version_in_scope("anything", "anything-1.0.0.tar.gz"));
    }

    #[test]
    fn scope_gates_names_fail_closed() {
        let filter = ResolvedMirror {
            include_packages: vec![spec("requests", Some(">=2.20,<3")), spec("numpy", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", filter, false).unwrap();
        assert!(proxy.name_in_scope("requests"));
        assert!(proxy.name_in_scope("numpy"));
        assert!(
            !proxy.name_in_scope("flask"),
            "unapproved name must be denied"
        );
    }

    #[test]
    fn scope_gates_versions_like_sync() {
        let filter = ResolvedMirror {
            include_packages: vec![spec("requests", Some(">=2.20,<3")), spec("numpy", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", filter, false).unwrap();
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
        let mirror = ResolvedMirror {
            include_packages: vec![spec("foo", Some("==1.0")), spec("foo", Some("==3.0"))],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false).unwrap();
        assert!(proxy.version_in_scope("foo", "foo-1.0-py3-none-any.whl"));
        assert!(proxy.version_in_scope("foo", "foo-3.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("foo", "foo-2.0-py3-none-any.whl"));
    }

    #[test]
    fn empty_scope_with_bare_deny_excludes_that_name_only() {
        let mirror = ResolvedMirror {
            exclude_packages: vec![spec("blocked", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false).unwrap();
        assert!(proxy.name_in_scope("blocked"));
        assert!(proxy.name_fully_denied("blocked"));
        assert!(!proxy.version_in_scope("blocked", "blocked-1.0-py3-none-any.whl"));
        assert!(!proxy.name_fully_denied("allowed"));
        assert!(proxy.version_in_scope("allowed", "allowed-1.0-py3-none-any.whl"));
    }

    #[test]
    fn version_pinned_deny_drops_old_versions_only() {
        let mirror = ResolvedMirror {
            exclude_packages: vec![spec("demo", Some("<2"))],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false).unwrap();
        assert!(!proxy.name_fully_denied("demo"));
        assert!(!proxy.version_in_scope("demo", "demo-1.9-py3-none-any.whl"));
        assert!(proxy.version_in_scope("demo", "demo-2.0-py3-none-any.whl"));
        assert!(proxy.version_in_scope("demo", "demo-garbage.whl"));
    }

    #[test]
    fn deny_wins_over_allow() {
        let mirror = ResolvedMirror {
            include_packages: vec![spec("demo", None), spec("pinned", Some(">=1"))],
            exclude_packages: vec![spec("demo", None), spec("pinned", Some("<2"))],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false).unwrap();
        assert!(proxy.name_in_scope("demo"));
        assert!(proxy.name_fully_denied("demo"));
        assert!(!proxy.version_in_scope("demo", "demo-3.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("pinned", "pinned-1.5-py3-none-any.whl"));
        assert!(proxy.version_in_scope("pinned", "pinned-2.0-py3-none-any.whl"));
    }
}
