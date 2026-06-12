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
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::fs;
use tracing::{error, info, warn};

use crate::config::{self, SyncConfig};
use crate::names::{is_normalized, matches_prefix, normalize_pkg_name};
use crate::origin;
use crate::sidecar::{metadata_key, sidecar_key, Sidecar, Yanked};
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

    /// Source PyPI base (default: https://pypi.org). We call /pypi/<name>/json here.
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

    /// Parallel downloads/uploads (default 4).
    #[arg(long, env = "PYPIRON_SYNC_CONCURRENCY")]
    pub concurrency: Option<usize>,

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
    dry_run: bool,
    filter: ResolvedFilter,
}

struct ResolvedFilter {
    only_wheels: bool,
    only_sdists: bool,
    python_tag: Vec<String>,
    abi_tag: Vec<String>,
    platform_tag: Vec<String>,
    exclude_platform_tag: Vec<String>,
    exclude_newer: Option<OffsetDateTime>,
    exclude_older: Option<OffsetDateTime>,
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

fn parse_cutoff(what: &str, value: Option<&String>) -> Result<Option<OffsetDateTime>> {
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

#[derive(Debug, Deserialize)]
struct PyPiJson {
    releases: HashMap<String, Vec<PyPiFile>>,
}

#[derive(Debug, Clone, Deserialize)]
struct PyPiFile {
    filename: String,
    url: String,
    digests: PyPiDigests,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    packagetype: Option<String>,
    #[serde(default)]
    upload_time_iso_8601: Option<String>,
    #[serde(default)]
    requires_python: Option<String>,
    #[serde(default)]
    yanked: bool,
    #[serde(default)]
    yanked_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct PyPiDigests {
    sha256: String,
}

/// A file selected for mirroring, with its release version.
struct Selected {
    version: String,
    file: PyPiFile,
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

    let mut failures = 0usize;
    for spec in &resolved.specs {
        if let Err(e) = sync_one_package(&client, &resolved, &sink, spec).await {
            error!(package=%spec.name, error=?e, "package sync failed");
            failures += 1;
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
            let winner = origin::claim_origin(storage.as_ref(), pkg, origin::MIRROR).await?;
            if winner != origin::MIRROR {
                bail!("'{pkg}' is {winner}-owned; refusing to mirror over it");
            }
            claimed_now = true;
        }
    }

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
            mark_dirty(storage.as_ref(), pkg).await?;
        }
    }
    if errors > 0 {
        // A claim with nothing behind it would block the name forever.
        if claimed_now && !wrote {
            if let Sink::Storage(storage) = sink {
                release_empty_claim(storage.as_ref(), pkg).await;
            }
        }
        bail!("{errors} file(s) failed for '{pkg}'");
    }
    Ok(())
}

/// Remove our orphan `.origin` claim if the package holds no artifacts.
async fn release_empty_claim(storage: &dyn Storage, pkg: &str) {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    match storage.list_dir_entries(&prefix).await {
        Ok(entries) => {
            let has_artifact = entries.iter().any(|e| {
                e.key
                    .strip_prefix(&prefix)
                    .map(crate::sidecar::is_artifact)
                    .unwrap_or(false)
            });
            if !has_artifact {
                let _ = storage.delete_keys(&[origin::origin_key(pkg)]).await;
            }
        }
        Err(e) => warn!(package=%pkg, error=?e, "could not check for orphan claim"),
    }
}

async fn fetch_selected_files(
    client: &Client,
    resolved: &Resolved,
    spec: &PackageSpec,
) -> Result<Vec<Selected>> {
    let url = format!(
        "{}/pypi/{}/json",
        resolved.src_base.trim_end_matches('/'),
        spec.name
    );
    let resp = client.get(url).send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(anyhow!("Package not found on source: {}", spec.name));
    }
    let json: PyPiJson = resp.error_for_status()?.json().await?;

    let mut selected = Vec::new();
    for (version, files) in json.releases {
        if let Some(specifiers) = &spec.specifiers {
            // Unparseable (pre-PEP-440 junk) versions never match a constraint.
            match Version::from_str(&version) {
                Ok(v) if specifiers.contains(&v) => {}
                _ => continue,
            }
        }
        for file in files {
            if matches_filters(&file, &resolved.filter) {
                selected.push(Selected {
                    version: version.clone(),
                    file,
                });
            }
        }
    }
    if selected.is_empty() {
        warn!("No matching files for package '{}'", spec.name);
    }
    Ok(selected)
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

    // Best-effort PEP 658: PyPI serves metadata at <file-url>.metadata.
    if s.file.filename.ends_with(".whl") {
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
    let yanked = match (&s.file.yanked_reason, s.file.yanked) {
        (Some(reason), _) if !reason.trim().is_empty() => Yanked::Reason(reason.trim().into()),
        (_, flag) => Yanked::Flag(flag),
    };
    let sc = Sidecar {
        // PyPI's digest, verified against the downloaded bytes — not re-derived.
        sha256: s.file.digests.sha256.clone(),
        size,
        version: s.version.clone(),
        // PyPI's true upload time: the whole point of mirroring.
        upload_time: s.file.upload_time_iso_8601.clone().unwrap_or_default(),
        requires_python: s.file.requires_python.clone(),
        yanked,
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

async fn download_verified(client: &Client, file: &PyPiFile) -> Result<Vec<u8>> {
    let resp = client.get(&file.url).send().await?.error_for_status()?;
    let bytes = resp.bytes().await?.to_vec();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let got = format!("{:x}", hasher.finalize());
    if !got.eq_ignore_ascii_case(&file.digests.sha256) {
        bail!(
            "sha256 mismatch for {} (expected {}, got {got})",
            file.filename,
            file.digests.sha256
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

    let mut form = multipart::Form::new()
        .text(":action", "file_upload")
        .text("protocol_version", "1")
        .text("mirror", "true")
        .text("name", pkg.to_string())
        .text("version", s.version.clone())
        .text("sha256_digest", s.file.digests.sha256.clone())
        .text("yanked", if s.file.yanked { "true" } else { "false" })
        .part("content", part);
    if let Some(ts) = &s.file.upload_time_iso_8601 {
        form = form.text("upload_time", ts.clone());
    }
    if let Some(reason) = &s.file.yanked_reason {
        if !reason.trim().is_empty() {
            form = form.text("yanked_reason", reason.trim().to_string());
        }
    }
    if let Some(rp) = &s.file.requires_python {
        form = form.text("requires_python", rp.clone());
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

fn matches_filters(file: &PyPiFile, f: &ResolvedFilter) -> bool {
    let fname = file.filename.to_ascii_lowercase();
    let is_wheel =
        fname.ends_with(".whl") || matches!(file.packagetype.as_deref(), Some("bdist_wheel"));

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
            .upload_time_iso_8601
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
struct WheelTags {
    python: Vec<String>,
    abi: Vec<String>,
    platform: Vec<String>,
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
fn parse_wheel_tags(filename: &str) -> Option<WheelTags> {
    if !filename.ends_with(".whl") {
        return None;
    }
    let stem = filename.strip_suffix(".whl")?;
    let parts: Vec<&str> = stem.split('-').collect();
    if parts.len() < 5 {
        // name, version, [build?], py, abi, platform  -> min 5 fields (without build)
        return None;
    }
    let py = parts[parts.len() - 3]
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let abi = parts[parts.len() - 2]
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let plat = parts[parts.len() - 1]
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Some(WheelTags {
        python: py,
        abi,
        platform: plat,
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

    fn pypi_file(upload_time: Option<&str>) -> PyPiFile {
        PyPiFile {
            filename: "six-1.16.0-py2.py3-none-any.whl".into(),
            url: String::new(),
            digests: PyPiDigests {
                sha256: String::new(),
            },
            size: None,
            packagetype: None,
            upload_time_iso_8601: upload_time.map(str::to_string),
            requires_python: None,
            yanked: false,
            yanked_reason: None,
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
    fn upload_time_bounds_filter() {
        let old = pypi_file(Some("2015-10-07T13:41:23Z"));
        let new = pypi_file(Some("2024-12-04T17:35:26Z"));
        let unknown = pypi_file(None);

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
