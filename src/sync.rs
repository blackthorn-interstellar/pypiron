//! Mirror packages from PyPI into this registry.
//!
//! The recommended mode is mirror-over-HTTP (`--to <server>`): each file is
//! POSTed to the server's `/legacy/` with `mirror=true` plus PyPI's true
//! `upload-time` and yank state, authenticated against the server's admin
//! credential, and the server owns every storage write. Sync needs a URL and
//! the admin credential — nothing about the server's storage. Without
//! `--to`, sync writes directly to storage with the same code the server uses.
//!
//! Filters (`--only-wheels`, tag filters, `--exclude-newer`/`--exclude-older`,
//! PEP 440 specifiers in the package list) gate only what a run *adds*;
//! nothing already mirrored is ever removed. Options layer as
//! CLI/env > pypiron.toml > defaults.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use futures::stream::{self, StreamExt};
use pep440_rs::{Version, VersionSpecifiers};
use reqwest::{multipart, Client};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::str::FromStr;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::fs;
use tracing::{error, info, warn};

use crate::config::{self, SyncConfig};
use crate::names::{
    infer_version_from_filename, is_normalized, matches_prefix, normalize_pkg_name,
};
use crate::origin;
use crate::sidecar::{metadata_key, provenance_key, sidecar_key, Sidecar, Yanked};
use crate::simple::{self, SimpleFile};
use crate::storage::{Storage, StorageArgs};
use crate::worker::mark_dirty;
use crate::PACKAGES_PREFIX;

#[derive(Debug, Clone, Args)]
pub struct SyncArgs {
    /// Path to a pypiron.toml (defaults to ./pypiron.toml when present).
    /// CLI and env values take precedence over the file.
    #[arg(long, env = "PYPIRON_CONFIG")]
    pub config: Option<PathBuf>,

    /// Text file of packages to mirror, one per line: a name with optional
    /// PEP 440 specifiers (e.g. "requests>=2.20,<3").
    #[arg(long, env = "PYPIRON_PACKAGES_LIST")]
    pub packages_list: Option<PathBuf>,

    /// Source index base (default: https://pypi.org). Read over the PEP 691
    /// Simple API (`/simple/<name>/`), so any PEP 691 index works — PyPI,
    /// another pypiron, etc.
    #[arg(long = "from", env = "PYPIRON_SYNC_FROM")]
    pub src_base: Option<String>,

    /// Destination PypIron base URL: mirror over HTTP via its /legacy/
    /// (recommended; authenticate with the server's admin credential). Omit
    /// to write directly to storage.
    #[arg(long = "to", env = "PYPIRON_SYNC_TO")]
    pub dst_base: Option<String>,

    /// Basic auth username for the HTTP-mode destination (optional).
    #[arg(long, env = "PYPIRON_SYNC_USERNAME")]
    pub username: Option<String>,

    /// Basic auth password for the HTTP-mode destination (optional).
    #[arg(long, env = "PYPIRON_SYNC_PASSWORD")]
    pub password: Option<String>,

    /// Refuse to mirror names inside this private namespace (PEP 503-normalized)
    #[arg(long, env = "PYPIRON_PRIVATE_PREFIX")]
    pub private_prefix: Option<String>,

    /// Parallel downloads/uploads within one package (default 4).
    #[arg(long, env = "PYPIRON_SYNC_CONCURRENCY")]
    pub concurrency: Option<usize>,

    /// Packages synced in parallel (default 8). The long tail of any real
    /// mirror is hundreds of thousands of 2-file packages; per-file
    /// concurrency alone leaves throughput gated on serial per-package
    /// round-trips.
    #[arg(long, env = "PYPIRON_SYNC_PACKAGE_CONCURRENCY")]
    pub package_concurrency: Option<usize>,

    /// Print actions without downloading/uploading.
    #[arg(long)]
    pub dry_run: bool,

    /// Filtering flags (wheel/python/abi/platform/upload-time).
    #[command(flatten)]
    pub filter: FilterArgs,

    /// Storage configuration for direct mode (same flags as `serve`).
    #[command(flatten)]
    pub storage: StorageArgs,
}

#[derive(Debug, Clone, Args)]
pub struct FilterArgs {
    /// Only mirror wheel files (.whl)
    #[arg(long, env = "PYPIRON_SYNC_ONLY_WHEELS", conflicts_with = "only_sdists")]
    pub only_wheels: bool,

    /// Only mirror source distributions (sdist)
    #[arg(long, env = "PYPIRON_SYNC_ONLY_SDISTS")]
    pub only_sdists: bool,

    /// Include wheels whose python tag matches any of these (e.g. py3, cp311). Comma-separated or repeatable.
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub python_tag: Vec<String>,

    /// Include wheels whose ABI tag matches any of these (e.g. none, cp311). Comma-separated or repeatable.
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub abi_tag: Vec<String>,

    /// Include wheels whose platform tag matches any of these (e.g. any, manylinux2014_x86_64, macosx_*_arm64, win_amd64). Supports '*' wildcard.
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub platform_tag: Vec<String>,

    /// Exclude wheels whose platform tag matches any of these (supports '*' wildcard).
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub exclude_platform_tag: Vec<String>,

    /// Only mirror files PyPI received before this RFC 3339 timestamp
    /// (the mirroring twin of uv's --exclude-newer).
    #[arg(long, env = "PYPIRON_SYNC_EXCLUDE_NEWER", value_name = "TIMESTAMP")]
    pub exclude_newer: Option<String>,

    /// Only mirror files PyPI received at or after this RFC 3339 timestamp.
    #[arg(long, env = "PYPIRON_SYNC_EXCLUDE_OLDER", value_name = "TIMESTAMP")]
    pub exclude_older: Option<String>,
}

/// One package to mirror, with optional PEP 440 version constraints.
#[derive(Debug, Clone)]
struct PackageSpec {
    name: String,
    specifiers: Option<VersionSpecifiers>,
}

/// Everything resolved: CLI/env over pypiron.toml over defaults, all inputs
/// parsed and validated up front.
struct Resolved {
    specs: Vec<PackageSpec>,
    src_base: String,
    dst_base: Option<String>,
    username: Option<String>,
    password: Option<String>,
    private_prefix: Option<String>,
    concurrency: usize,
    package_concurrency: usize,
    dry_run: bool,
    filter: ResolvedFilter,
}

pub(crate) struct ResolvedFilter {
    pub(crate) only_wheels: bool,
    pub(crate) only_sdists: bool,
    pub(crate) python_tag: Vec<String>,
    pub(crate) abi_tag: Vec<String>,
    pub(crate) platform_tag: Vec<String>,
    pub(crate) exclude_platform_tag: Vec<String>,
    pub(crate) exclude_newer: Option<OffsetDateTime>,
    pub(crate) exclude_older: Option<OffsetDateTime>,
}

impl Resolved {
    async fn merge(args: &SyncArgs, cfg: SyncConfig) -> Result<Self> {
        // Package set follows CLI > file: an explicit --packages-list replaces
        // the file's list entirely (both its `packages-list` and inline
        // `packages`). With no CLI list, the file's path and inline array
        // combine.
        let mut lines: Vec<String> = Vec::new();
        if let Some(path) = &args.packages_list {
            let text = fs::read_to_string(path)
                .await
                .with_context(|| format!("reading {}", path.display()))?;
            lines.extend(text.lines().map(str::to_string));
        } else {
            if let Some(path) = &cfg.packages_list {
                let text = fs::read_to_string(path)
                    .await
                    .with_context(|| format!("reading {}", path.display()))?;
                lines.extend(text.lines().map(str::to_string));
            }
            lines.extend(cfg.packages.unwrap_or_default());
        }

        let mut specs = Vec::new();
        for (lineno, raw) in lines.iter().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            specs.push(
                parse_spec_line(line)
                    .with_context(|| format!("package entry {} ('{line}')", lineno + 1))?,
            );
        }
        if specs.is_empty() {
            bail!(
                "no packages to sync: provide --packages-list or [sync].packages in pypiron.toml"
            );
        }

        let filter = &args.filter;
        let only_wheels = filter.only_wheels || cfg.only_wheels.unwrap_or(false);
        let only_sdists = filter.only_sdists || cfg.only_sdists.unwrap_or(false);
        if only_wheels && only_sdists {
            // Would select nothing and "succeed" — a silent empty mirror.
            bail!("only-wheels and only-sdists are mutually exclusive");
        }

        let dst_base = args.dst_base.clone().or(cfg.to);
        // CLI/env > file says storage flags win, but in HTTP mode they have no
        // sink to apply to — flag it rather than silently ignore --data-dir.
        if dst_base.is_some()
            && (args.storage.data_dir.is_some() || args.storage.s3_bucket.is_some())
        {
            warn!("mirror-over-HTTP mode (--to/[sync].to): --data-dir/--s3-bucket are ignored");
        }

        Ok(Self {
            specs,
            src_base: args
                .src_base
                .clone()
                .or(cfg.from)
                .unwrap_or_else(|| "https://pypi.org".to_string()),
            dst_base,
            username: args.username.clone().or(cfg.username),
            password: args.password.clone().or(cfg.password),
            private_prefix: args.private_prefix.clone().or(cfg.private_prefix),
            concurrency: args.concurrency.or(cfg.concurrency).unwrap_or(4).max(1),
            package_concurrency: args
                .package_concurrency
                .or(cfg.package_concurrency)
                .unwrap_or(8)
                .max(1),
            dry_run: args.dry_run,
            filter: ResolvedFilter {
                only_wheels,
                only_sdists,
                python_tag: pick_vec(&filter.python_tag, cfg.python_tag),
                abi_tag: pick_vec(&filter.abi_tag, cfg.abi_tag),
                platform_tag: pick_vec(&filter.platform_tag, cfg.platform_tag),
                exclude_platform_tag: pick_vec(
                    &filter.exclude_platform_tag,
                    cfg.exclude_platform_tag,
                ),
                exclude_newer: parse_cutoff(
                    "exclude-newer",
                    filter.exclude_newer.as_ref().or(cfg.exclude_newer.as_ref()),
                )?,
                exclude_older: parse_cutoff(
                    "exclude-older",
                    filter.exclude_older.as_ref().or(cfg.exclude_older.as_ref()),
                )?,
            },
        })
    }
}

/// CLI tags win when any were passed; otherwise the config file's.
fn pick_vec(cli: &[String], cfg: Option<Vec<String>>) -> Vec<String> {
    if cli.is_empty() {
        cfg.unwrap_or_default()
    } else {
        cli.to_vec()
    }
}

pub(crate) fn parse_cutoff(what: &str, value: Option<&String>) -> Result<Option<OffsetDateTime>> {
    let Some(value) = value.filter(|v| !v.trim().is_empty()) else {
        return Ok(None);
    };
    OffsetDateTime::parse(value, &Rfc3339)
        .map(Some)
        .map_err(|e| anyhow!("{what} is not RFC 3339 ('{value}'): {e}"))
}

/// `name` with optional PEP 440 specifiers: `requests`, `six==1.16.0`,
/// `requests>=2.20,<3`.
fn parse_spec_line(line: &str) -> Result<PackageSpec> {
    let split = line
        .find(['<', '>', '=', '!', '~', ' '])
        .unwrap_or(line.len());
    let (raw_name, raw_spec) = line.split_at(split);
    let name = normalize_pkg_name(raw_name.trim());
    if !is_normalized(&name) {
        bail!("invalid package name '{raw_name}'");
    }
    let raw_spec = raw_spec.trim();
    let specifiers = if raw_spec.is_empty() {
        None
    } else {
        Some(
            VersionSpecifiers::from_str(raw_spec)
                .map_err(|e| anyhow!("invalid version specifiers '{raw_spec}': {e}"))?,
        )
    };
    Ok(PackageSpec { name, specifiers })
}

/// A file selected for mirroring. `version` is inferred from the filename (the
/// Simple API doesn't bind files to versions); `None` means it wasn't parseable.
struct Selected {
    version: Option<String>,
    file: SimpleFile,
}

enum Sink {
    Storage(std::sync::Arc<dyn Storage>),
    Http(String),
}

pub async fn run_sync(args: SyncArgs) -> Result<()> {
    let cfg = config::load(args.config.as_deref())?.sync;
    let resolved = Resolved::merge(&args, cfg).await?;

    let client = Client::builder()
        .user_agent("pypiron-sync/0.1 (+https://github.com/brycedrennan/pypiron)")
        // Bound the handshake and any mid-stream stall so a dead/dribbling
        // upstream fails a sync task cleanly (the retry loop absorbs it) instead
        // of hanging forever. read_timeout is per-read and resets on each chunk,
        // so it never bounds a large artifact that keeps streaming.
        .connect_timeout(std::time::Duration::from_secs(10))
        .read_timeout(std::time::Duration::from_secs(30))
        .build()?;

    let sink = match &resolved.dst_base {
        Some(dst) => {
            let endpoint = normalize_legacy_endpoint(dst);
            info!("mirror-over-HTTP mode: uploading to {endpoint}");
            Sink::Http(endpoint)
        }
        None => {
            info!("direct-storage mode");
            Sink::Storage(args.storage.build().await?)
        }
    };

    // Packages in parallel (chunked join_all — same pattern as the worker
    // sweep), files within each package in parallel below. The long tail of a
    // mirror is small packages, so serial-per-package was the throughput cap.
    let mut failures = 0usize;
    for chunk in resolved.specs.chunks(resolved.package_concurrency) {
        let results = futures::future::join_all(
            chunk
                .iter()
                .map(|spec| sync_one_package(&client, &resolved, &sink, spec)),
        )
        .await;
        for (spec, result) in chunk.iter().zip(results) {
            if let Err(e) = result {
                error!(package=%spec.name, error=?e, "package sync failed");
                failures += 1;
            }
        }
    }
    if failures > 0 {
        bail!("{failures} package(s) failed to sync");
    }
    Ok(())
}

async fn sync_one_package(
    client: &Client,
    resolved: &Resolved,
    sink: &Sink,
    spec: &PackageSpec,
) -> Result<()> {
    let pkg = spec.name.as_str();

    // Policy gates come before any network traffic. Over HTTP the server
    // enforces all of this again — defense in both places.
    if let Some(prefix) = &resolved.private_prefix {
        let prefix = normalize_pkg_name(prefix);
        if matches_prefix(pkg, &prefix) {
            bail!("'{pkg}' is inside the private namespace '{prefix}'; refusing to mirror");
        }
    }
    if let Sink::Storage(storage) = sink {
        match origin::read_origin(storage.as_ref(), pkg).await?.as_deref() {
            Some(origin::MIRROR) | None => {}
            Some(other) => {
                bail!("'{pkg}' is {other}-owned; refusing to mirror over it")
            }
        }
    }

    let selected = fetch_selected_files(client, resolved, spec).await?;
    info!("Syncing {pkg} ({} matching files selected)", selected.len());

    if resolved.dry_run {
        for s in &selected {
            println!("[dry-run] would copy {} ({})", s.file.filename, s.file.url);
        }
        return Ok(());
    }

    let mut claimed_now = false;
    if let Sink::Storage(storage) = sink {
        // Claim only when actually writing — atomically, so a racing first
        // private upload can't merge origins.
        if origin::read_origin(storage.as_ref(), pkg).await?.is_none() {
            let (created, winner) =
                origin::claim_origin(storage.as_ref(), pkg, origin::MIRROR).await?;
            if winner != origin::MIRROR {
                bail!("'{pkg}' is {winner}-owned; refusing to mirror over it");
            }
            // Only release a claim we actually created — never a racing peer's.
            claimed_now = created;
        }
    }

    // Intent before the batch of truth writes; commit (paired) after. A sync
    // process killed mid-package heals via the stale intent.
    let intent_nonce = match sink {
        Sink::Storage(storage) => crate::worker::mark_intent(storage.as_ref(), pkg).await.ok(),
        Sink::Http(_) => None,
    };

    let results: Vec<Result<bool>> = stream::iter(selected)
        .map(|s| async move {
            match sink {
                Sink::Storage(storage) => {
                    mirror_to_storage(client, storage.as_ref(), pkg, &s).await
                }
                Sink::Http(endpoint) => upload_via_http(client, resolved, endpoint, pkg, &s).await,
            }
        })
        .buffer_unordered(resolved.concurrency)
        .collect()
        .await;

    let mut wrote = false;
    let mut errors = 0usize;
    for r in &results {
        match r {
            Ok(true) => wrote = true,
            Ok(false) => {}
            Err(e) => {
                error!(package=%pkg, error=?e, "file failed");
                errors += 1;
            }
        }
    }

    if wrote {
        if let Sink::Storage(storage) = sink {
            match &intent_nonce {
                Some(nonce) => crate::worker::mark_commit(storage.as_ref(), pkg, nonce).await?,
                None => mark_dirty(storage.as_ref(), pkg).await?,
            }
        }
    }
    if errors > 0 {
        // A claim with nothing behind it would block the name forever.
        if claimed_now && !wrote {
            if let Sink::Storage(storage) = sink {
                origin::release_empty_claim(storage.as_ref(), pkg).await;
            }
        }
        bail!("{errors} file(s) failed for '{pkg}'");
    }
    Ok(())
}

async fn fetch_selected_files(
    client: &Client,
    resolved: &Resolved,
    spec: &PackageSpec,
) -> Result<Vec<Selected>> {
    let base_url = format!(
        "{}/simple/{}/",
        resolved.src_base.trim_end_matches('/'),
        spec.name
    );
    let index = simple::fetch_index(client, &resolved.src_base, &spec.name, None)
        .await?
        .ok_or_else(|| anyhow!("Package not found on source: {}", spec.name))?;

    // PEP 691 file URLs may be relative; resolve them (and provenance URLs)
    // against the index page so a non-PyPI source — another pypiron, whose
    // listings are root-relative — works, not just PyPI's absolute CDN links.
    let base = reqwest::Url::parse(&base_url).ok();
    let resolve = |raw: &str| -> String {
        base.as_ref()
            .and_then(|b| b.join(raw).ok())
            .map(|u| u.to_string())
            .unwrap_or_else(|| raw.to_string())
    };

    let mut selected = Vec::new();
    for mut file in index.files {
        // No digest, no service: every artifact we hand out must be verifiable.
        if file.sha256().is_none() {
            continue;
        }
        if !matches_filters(&file, &resolved.filter) {
            continue;
        }
        let version = infer_version_from_filename(&file.filename);
        if let Some(specifiers) = &spec.specifiers {
            // A specifier gates by version, which the Simple API doesn't carry —
            // we infer it from the filename. A file whose version can't be
            // parsed can't be proven to match, so it's skipped (the same
            // conservative rule the release-keyed API applied to junk versions).
            let matched = version
                .as_deref()
                .and_then(|v| Version::from_str(v).ok())
                .is_some_and(|v| specifiers.contains(&v));
            if !matched {
                continue;
            }
        }
        file.url = resolve(&file.url);
        file.provenance = file.provenance.as_deref().map(&resolve);
        selected.push(Selected { version, file });
    }
    if selected.is_empty() {
        warn!("No matching files for package '{}'", spec.name);
    }
    Ok(selected)
}

/// Best-effort provenance fetch — supplemental, never fails the file.
async fn download_provenance(client: &Client, url: &str) -> Option<Vec<u8>> {
    match client
        .get(url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
    {
        Ok(resp) => match resp.bytes().await {
            Ok(bytes) => Some(bytes.to_vec()),
            Err(e) => {
                warn!(%url, error=?e, "sync: provenance body read failed");
                None
            }
        },
        Err(e) => {
            warn!(%url, error=?e, "sync: provenance fetch failed");
            None
        }
    }
}

/// Mirror one file into storage. Returns false if it was already present.
/// Writes follow the ordering invariant: artifact, metadata, sidecar; the
/// caller drops the package's dirty marker after the batch.
async fn mirror_to_storage(
    client: &Client,
    storage: &dyn Storage,
    pkg: &str,
    s: &Selected,
) -> Result<bool> {
    let key = format!("{PACKAGES_PREFIX}{pkg}/{}", s.file.filename);
    let artifact_exists = storage.head_exists(&key).await?;
    let sidecar_exists = storage.head_exists(&sidecar_key(&key)).await?;
    if artifact_exists && sidecar_exists {
        return Ok(false);
    }
    if artifact_exists {
        // A crash between artifact and sidecar writes left a half-mirrored
        // file; heal the sidecar from PyPI metadata without re-downloading.
        write_mirror_sidecar(storage, &key, s, s.file.size.unwrap_or(0)).await?;
        return Ok(true);
    }

    info!("  - mirroring {}", s.file.filename);
    let bytes = download_verified(client, &s.file).await?;
    let size = bytes.len() as u64;

    // Conditional create: a racing syncer losing here is harmless, the
    // sidecar write below is deterministic for both.
    storage
        .put_if_absent(&key, bytes, Some("application/octet-stream"))
        .await?;

    // Best-effort PEP 658: the source serves metadata at <file-url>.metadata.
    // The Simple API tells us which wheels actually have a companion, so we only
    // fetch when one exists instead of probing (and 404ing) on every wheel.
    if s.file.filename.ends_with(".whl") && s.file.has_core_metadata() {
        if let Ok(resp) = client.get(format!("{}.metadata", s.file.url)).send().await {
            if resp.status().is_success() {
                if let Ok(md) = resp.bytes().await {
                    let _ = storage
                        .put_bytes(
                            &metadata_key(&key),
                            md.to_vec(),
                            Some("text/plain; charset=utf-8"),
                        )
                        .await;
                }
            }
        }
    }

    // Best-effort PEP 740: relay the provenance object verbatim next to the
    // artifact so an offline consumer can still verify the original publisher.
    if let Some(prov_url) = &s.file.provenance {
        if let Some(prov) = download_provenance(client, prov_url).await {
            let _ = storage
                .put_bytes(&provenance_key(&key), prov, Some("application/json"))
                .await;
        }
    }

    write_mirror_sidecar(storage, &key, s, size).await?;
    Ok(true)
}

/// Sidecar carrying PyPI's metadata verbatim — digest, true upload time,
/// requires-python, yank state.
async fn write_mirror_sidecar(
    storage: &dyn Storage,
    key: &str,
    s: &Selected,
    size: u64,
) -> Result<()> {
    let sc = Sidecar {
        // The source's digest, verified against the downloaded bytes — not re-derived.
        sha256: s.file.sha256().unwrap_or_default().to_string(),
        size,
        version: s.version.clone().unwrap_or_default(),
        // The source's true upload time (PEP 700): the whole point of mirroring.
        upload_time: s.file.upload_time.clone().unwrap_or_default(),
        requires_python: s.file.requires_python.clone(),
        yanked: s.file.yanked.clone(),
    };
    storage
        .put_bytes(
            &sidecar_key(key),
            serde_json::to_vec(&sc)?,
            Some("application/json"),
        )
        .await?;
    Ok(())
}

/// How many times a single file download is attempted before the package is
/// marked failed. At mirror scale, transient CDN errors are a statistical
/// certainty — one 503 in 7,714 files failed an entire sync run before this
/// existed. Hash mismatches retry too: a truncated body looks identical.
const DOWNLOAD_ATTEMPTS: u32 = 3;

async fn download_verified(client: &Client, file: &SimpleFile) -> Result<Vec<u8>> {
    let mut last_err = None;
    for attempt in 1..=DOWNLOAD_ATTEMPTS {
        match download_once(client, file).await {
            Ok(bytes) => return Ok(bytes),
            Err(e) => {
                if attempt < DOWNLOAD_ATTEMPTS {
                    warn!(file=%file.filename, error=?e, attempt, "download failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.expect("at least one attempt"))
}

async fn download_once(client: &Client, file: &SimpleFile) -> Result<Vec<u8>> {
    let expected = file
        .sha256()
        .ok_or_else(|| anyhow!("no sha256 for {}", file.filename))?;
    let resp = client.get(&file.url).send().await?.error_for_status()?;
    let bytes = resp.bytes().await?.to_vec();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let got = format!("{:x}", hasher.finalize());
    if !got.eq_ignore_ascii_case(expected) {
        bail!(
            "sha256 mismatch for {} (expected {expected}, got {got})",
            file.filename,
        );
    }
    Ok(bytes)
}

/// HTTP mode: push through the remote `/legacy/` as a mirror upload, carrying
/// PyPI's metadata verbatim — the server (authenticated as admin) owns the
/// storage writes. Returns true on success; the remote's 409 on an existing
/// file means already-present (false).
async fn upload_via_http(
    client: &Client,
    resolved: &Resolved,
    endpoint: &str,
    pkg: &str,
    s: &Selected,
) -> Result<bool> {
    let bytes = download_verified(client, &s.file).await?;

    info!("  - uploading {}", s.file.filename);
    let part = multipart::Part::bytes(bytes)
        .file_name(s.file.filename.clone())
        .mime_str("application/octet-stream")?;

    let (yanked, yanked_reason) = match &s.file.yanked {
        Yanked::Flag(f) => (*f, None),
        Yanked::Reason(r) => (true, Some(r.clone())),
    };
    let mut form = multipart::Form::new()
        .text(":action", "file_upload")
        .text("protocol_version", "1")
        .text("mirror", "true")
        .text("name", pkg.to_string())
        .text(
            "sha256_digest",
            s.file.sha256().unwrap_or_default().to_string(),
        )
        .text("yanked", if yanked { "true" } else { "false" })
        .part("content", part);
    // The Simple API doesn't bind files to versions; send the filename-inferred
    // one when we have it, else let the server infer it the same way.
    if let Some(v) = &s.version {
        form = form.text("version", v.clone());
    }
    if let Some(ts) = &s.file.upload_time {
        form = form.text("upload_time", ts.clone());
    }
    if let Some(reason) = &yanked_reason {
        if !reason.trim().is_empty() {
            form = form.text("yanked_reason", reason.trim().to_string());
        }
    }
    if let Some(rp) = &s.file.requires_python {
        form = form.text("requires_python", rp.clone());
    }
    // PEP 740: forward the provenance object verbatim; the receiving server
    // stores it as the `.provenance` companion. Best-effort and UTF-8 (the
    // object is JSON), so a fetch failure just omits the supply-chain signal.
    if let Some(prov_url) = &s.file.provenance {
        if let Some(prov) = download_provenance(client, prov_url).await {
            if let Ok(text) = String::from_utf8(prov) {
                form = form.text("provenance", text);
            }
        }
    }

    let mut req = client.post(endpoint).multipart(form);
    if let (Some(u), Some(p)) = (resolved.username.as_ref(), resolved.password.as_ref()) {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    if resp.status() == reqwest::StatusCode::CONFLICT {
        return Ok(false);
    }
    if !resp.status().is_success() {
        let code = resp.status();
        let body = resp.text().await.unwrap_or_else(|_| "<no body>".into());
        bail!("upload failed [{code}]: {body}");
    }
    Ok(true)
}

pub(crate) fn matches_filters(file: &SimpleFile, f: &ResolvedFilter) -> bool {
    let fname = file.filename.to_ascii_lowercase();
    let is_wheel = fname.ends_with(".whl");

    if f.only_wheels && !is_wheel {
        return false;
    }
    if f.only_sdists && is_wheel {
        return false;
    }

    // Upload-time bounds. With a bound set, a file without a parseable
    // timestamp is excluded — same rule uv applies to --exclude-newer.
    if f.exclude_newer.is_some() || f.exclude_older.is_some() {
        let uploaded = file
            .upload_time
            .as_deref()
            .and_then(|ts| OffsetDateTime::parse(ts, &Rfc3339).ok());
        let Some(uploaded) = uploaded else {
            return false;
        };
        if f.exclude_newer.is_some_and(|cutoff| uploaded >= cutoff) {
            return false;
        }
        if f.exclude_older.is_some_and(|cutoff| uploaded < cutoff) {
            return false;
        }
    }

    // Only *inclusion* filters gate non-wheels (sdists have no tags). An
    // exclusion-only filter (e.g. --exclude-platform-tag win*) must not silently
    // drop every sdist — an sdist can't match a platform exclusion.
    let has_inclusion_filters =
        !(f.python_tag.is_empty() && f.abi_tag.is_empty() && f.platform_tag.is_empty());

    if !is_wheel {
        return !has_inclusion_filters;
    }

    let tags = match parse_wheel_tags(&file.filename) {
        Some(t) => t,
        None => {
            warn!(filename=%file.filename, "Could not parse wheel tags; skipping if inclusion filters present");
            return !has_inclusion_filters;
        }
    };

    // Exclusions first
    if !f.exclude_platform_tag.is_empty()
        && tokens_match_any(&tags.platform, &f.exclude_platform_tag)
    {
        return false;
    }

    if !f.python_tag.is_empty() && !tokens_match_any(&tags.python, &f.python_tag) {
        return false;
    }
    if !f.abi_tag.is_empty() && !tokens_match_any(&tags.abi, &f.abi_tag) {
        return false;
    }
    if !f.platform_tag.is_empty() && !tokens_match_any(&tags.platform, &f.platform_tag) {
        return false;
    }

    true
}

#[derive(Debug, Clone)]
pub(crate) struct WheelTags {
    pub(crate) python: Vec<String>,
    pub(crate) abi: Vec<String>,
    pub(crate) platform: Vec<String>,
}

fn tokens_match_any(tokens: &[String], filters: &[String]) -> bool {
    let tokens_lc: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();
    for f in filters {
        let pat = f.to_ascii_lowercase();
        for t in &tokens_lc {
            if tag_matches(t, &pat) {
                return true;
            }
        }
    }
    false
}

/// Supports exact match or glob-like '*' anywhere in the filter (matches ordered substrings).
fn tag_matches(tag: &str, filter: &str) -> bool {
    if filter == "*" {
        return true;
    }
    if !filter.contains('*') {
        return tag == filter;
    }
    glob_like_contains(tag, filter)
}

/// Simple glob-like matching: '*' matches any substring, parts must appear in order.
fn glob_like_contains(haystack: &str, pattern: &str) -> bool {
    let mut rest = haystack;
    for part in pattern.split('*') {
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = rest.find(part) {
            rest = &rest[idx + part.len()..];
        } else {
            return false;
        }
    }
    true
}

/// Parse wheel filename into (python, abi, platform) tags.
/// Uses the last 3 dash-separated fields before ".whl" per PEP 427.
pub(crate) fn parse_wheel_tags(filename: &str) -> Option<WheelTags> {
    if !filename.ends_with(".whl") {
        return None;
    }
    let stem = filename.strip_suffix(".whl")?;
    let parts: Vec<&str> = stem.split('-').collect();
    if parts.len() < 5 {
        // name, version, [build?], py, abi, platform  -> min 5 fields (without build)
        return None;
    }
    let dotted = |field: &str| field.split('.').map(str::to_string).collect::<Vec<_>>();
    Some(WheelTags {
        python: dotted(parts[parts.len() - 3]),
        abi: dotted(parts[parts.len() - 2]),
        platform: dotted(parts[parts.len() - 1]),
    })
}

fn normalize_legacy_endpoint(dst_base: &str) -> String {
    let trimmed = dst_base.trim_end_matches('/');
    if trimmed.ends_with("/legacy") {
        format!("{trimmed}/")
    } else {
        format!("{trimmed}/legacy/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_spec_lines() {
        let plain = parse_spec_line("requests").unwrap();
        assert_eq!(plain.name, "requests");
        assert!(plain.specifiers.is_none());

        let pinned = parse_spec_line("Six==1.16.0").unwrap();
        assert_eq!(pinned.name, "six");
        let v = Version::from_str("1.16.0").unwrap();
        assert!(pinned.specifiers.unwrap().contains(&v));

        let ranged = parse_spec_line("requests>=2.20,<3").unwrap();
        let specs = ranged.specifiers.unwrap();
        assert!(specs.contains(&Version::from_str("2.28.1").unwrap()));
        assert!(!specs.contains(&Version::from_str("3.0.0").unwrap()));

        assert!(parse_spec_line("foo/bar").is_err());
        assert!(parse_spec_line("requests >= 2.20").is_ok());
    }

    fn simple_file(upload_time: Option<&str>) -> SimpleFile {
        SimpleFile {
            filename: "six-1.16.0-py2.py3-none-any.whl".into(),
            url: String::new(),
            hashes: Default::default(),
            size: None,
            upload_time: upload_time.map(str::to_string),
            requires_python: None,
            yanked: Yanked::Flag(false),
            core_metadata: None,
            dist_info_metadata: None,
            provenance: None,
        }
    }

    fn time_filter(newer: Option<&str>, older: Option<&str>) -> ResolvedFilter {
        let parse = |v: Option<&str>| v.map(|s| OffsetDateTime::parse(s, &Rfc3339).unwrap());
        ResolvedFilter {
            only_wheels: false,
            only_sdists: false,
            python_tag: vec![],
            abi_tag: vec![],
            platform_tag: vec![],
            exclude_platform_tag: vec![],
            exclude_newer: parse(newer),
            exclude_older: parse(older),
        }
    }

    #[test]
    fn wheel_tags_parse_and_reject_real_shapes() {
        // Standard, compound python tag, and build-tag wheels parse.
        let t = parse_wheel_tags("six-1.16.0-py2.py3-none-any.whl").unwrap();
        assert_eq!(t.python, ["py2", "py3"]);
        assert_eq!(t.abi, ["none"]);
        assert_eq!(t.platform, ["any"]);
        let t = parse_wheel_tags("demo-1.0-1-cp311-cp311-manylinux_2_17_x86_64.whl").unwrap();
        assert_eq!(t.python, ["cp311"]);

        // Real malformed uploads (126 of 9.94M wheels): missing version or
        // tag fields. Must be None, not a panic or a bogus parse.
        assert!(parse_wheel_tags("JHVIT-0.0.1-py3-any.whl").is_none());
        assert!(parse_wheel_tags("CLUEstering-1.0.2-none-any.whl").is_none());
        assert!(parse_wheel_tags("GoldenFace1.1-py3-none-any.whl").is_none());
        assert!(parse_wheel_tags("not-a-wheel-1.0.tar.gz").is_none());
    }

    #[test]
    fn upload_time_bounds_filter() {
        let old = simple_file(Some("2015-10-07T13:41:23Z"));
        let new = simple_file(Some("2024-12-04T17:35:26Z"));
        let unknown = simple_file(None);

        let before_2016 = time_filter(Some("2016-01-01T00:00:00Z"), None);
        assert!(matches_filters(&old, &before_2016));
        assert!(!matches_filters(&new, &before_2016));
        assert!(!matches_filters(&unknown, &before_2016));

        let since_2016 = time_filter(None, Some("2016-01-01T00:00:00Z"));
        assert!(!matches_filters(&old, &since_2016));
        assert!(matches_filters(&new, &since_2016));

        let unbounded = time_filter(None, None);
        assert!(matches_filters(&unknown, &unbounded));
    }
}
