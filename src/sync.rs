use anyhow::{anyhow, Context, Result};
use clap::Args;
use futures::stream::{self, StreamExt};
use reqwest::{multipart, Client};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Args)]
pub struct SyncArgs {
    /// Path to a text file containing packages to mirror (one per line). Only names are allowed (no versions).
    #[arg(long, env = "PYPIRON_PACKAGES_LIST")]
    pub packages_list: PathBuf,

    /// Source PyPI base (default: https://pypi.org). We call /pypi/<name>/json here.
    #[arg(long = "from", env = "PYPIRON_SYNC_FROM", default_value = "https://pypi.org")]
    pub src_base: String,

    /// Destination PypIron base URL (we'll POST to <to>/legacy/).
    #[arg(long = "to", env = "PYPIRON_SYNC_TO")]
    pub dst_base: String,

    /// Basic auth username for destination (optional).
    #[arg(long, env = "PYPIRON_SYNC_USERNAME")]
    pub username: Option<String>,

    /// Basic auth password for destination (optional).
    #[arg(long, env = "PYPIRON_SYNC_PASSWORD")]
    pub password: Option<String>,

    /// Parallel downloads/uploads.
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,

    /// Print actions without downloading/uploading.
    #[arg(long)]
    pub dry_run: bool,

    /// Filtering flags (wheel/python/abi/platform).
    #[command(flatten)]
    pub filter: FilterArgs,
}

#[derive(Debug, Clone, Args)]
pub struct FilterArgs {
    /// Only mirror wheel files (.whl)
    #[arg(long)]
    pub only_wheels: bool,

    /// Only mirror source distributions (sdist)
    #[arg(long)]
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
}

#[derive(Debug, Clone)]
struct PackageSpec {
    name: String,
}

#[derive(Debug, Deserialize)]
struct PyPiJson {
    #[allow(dead_code)]
    info: PyPiInfo,
    releases: HashMap<String, Vec<PyPiFile>>,
}

#[derive(Debug, Deserialize)]
struct PyPiInfo {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    version: String,
}

#[derive(Debug, Clone, Deserialize)]
struct PyPiFile {
    filename: String,
    url: String,
    digests: PyPiDigests,

    // Useful hints; presence may vary.
    #[serde(default)]
    packagetype: Option<String>,
    #[serde(default)]
    python_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct PyPiDigests {
    sha256: String,
}

#[derive(Debug, Clone)]
struct WheelTags {
    python: Vec<String>,
    abi: Vec<String>,
    platform: Vec<String>,
}

pub async fn run_sync(args: SyncArgs) -> Result<()> {
    let client = Client::builder()
        .user_agent("pypiron-sync/0.1 (+https://github.com/brycedrennan/pypiron)")
        .build()?;

    let specs = read_specs(&args.packages_list).await?;
    if specs.is_empty() {
        return Err(anyhow!(
            "No packages found in {}",
            args.packages_list.display()
        ));
    }

    let dst_legacy = normalize_legacy_endpoint(&args.dst_base);
    info!("Destination legacy upload endpoint: {}", dst_legacy);

    for spec in specs {
        if let Err(e) = sync_one_package(&client, &args, &dst_legacy, spec).await {
            error!(error=?e, "Failed to sync a package");
        }
    }

    Ok(())
}

async fn sync_one_package(
    client: &Client,
    args: &SyncArgs,
    dst_legacy: &str,
    spec: PackageSpec,
) -> Result<()> {
    let files = fetch_all_files_for_package(client, &args.src_base, &spec.name).await?;
    let selected: Vec<PyPiFile> = files
        .into_iter()
        .filter(|f| matches_filters(f, &args.filter))
        .collect();

    info!(
        "Syncing {} ({} matching files selected)",
        spec.name,
        selected.len()
    );

    if args.dry_run {
        for f in &selected {
            println!("[dry-run] would copy {} ({})", f.filename, f.url);
        }
        return Ok(());
    }

    stream::iter(selected.into_iter())
        .map(|f| {
            let client = client.clone();
            let args = args.clone();
            let dst_legacy = dst_legacy.to_string();
            async move {
                if let Err(e) = download_verify_and_upload(&client, &dst_legacy, &args, f).await {
                    error!(error=?e, "file failed");
                }
            }
        })
        .buffer_unordered(args.concurrency.max(1))
        .collect::<Vec<_>>()
        .await;

    Ok(())
}

async fn fetch_all_files_for_package(client: &Client, src_base: &str, pkg: &str) -> Result<Vec<PyPiFile>> {
    let url = format!("{}/pypi/{}/json", src_base.trim_end_matches('/'), pkg);
    let resp = client.get(url).send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(anyhow!("Package not found on source: {}", pkg));
    }
    let json: PyPiJson = resp.error_for_status()?.json().await?;

    let mut all = Vec::new();
    for (_ver, files) in json.releases {
        for f in files {
            all.push(f);
        }
    }
    if all.is_empty() {
        warn!("No files found for package '{}'", pkg);
    }
    Ok(all)
}

async fn download_verify_and_upload(
    client: &Client,
    dst_legacy: &str,
    args: &SyncArgs,
    file: PyPiFile,
) -> Result<()> {
    info!("  - downloading {}", file.filename);
    let resp = client.get(&file.url).send().await?.error_for_status()?;
    let bytes = resp.bytes().await?.to_vec();

    // Verify SHA256
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let got = format!("{:x}", hasher.finalize());
    if got != file.digests.sha256 {
        warn!(
            "SHA256 mismatch for {} (expected {}, got {}) — skipping upload",
            file.filename, file.digests.sha256, got
        );
        return Err(anyhow!("sha256 mismatch for {}", file.filename));
    }

    info!("  - uploading {}", file.filename);
    let part = multipart::Part::bytes(bytes)
        .file_name(file.filename.clone())
        .mime_str("application/octet-stream")?;

    let form = multipart::Form::new()
        .text("filename", file.filename.clone())
        .part("content", part);

    let mut req = client.post(dst_legacy).multipart(form);
    if let (Some(u), Some(p)) = (args.username.as_ref(), args.password.as_ref()) {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        let code = resp.status();
        let body = resp.text().await.unwrap_or_else(|_| "<no body>".into());
        return Err(anyhow!("upload failed [{}]: {}", code, body));
    }
    Ok(())
}

fn matches_filters(file: &PyPiFile, f: &FilterArgs) -> bool {
    let fname = file.filename.to_ascii_lowercase();
    let is_wheel = fname.ends_with(".whl")
        || matches!(file.packagetype.as_deref(), Some("bdist_wheel"));

    if f.only_wheels && !is_wheel {
        return false;
    }
    if f.only_sdists && is_wheel {
        return false;
    }

    let has_tag_filters = !(f.python_tag.is_empty()
        && f.abi_tag.is_empty()
        && f.platform_tag.is_empty()
        && f.exclude_platform_tag.is_empty());

    if !is_wheel {
        // Non-wheel (e.g., sdist). If tag filters provided, sdists won't match.
        return !has_tag_filters;
    }

    let tags = match parse_wheel_tags(&file.filename) {
        Some(t) => t,
        None => {
            warn!(filename=%file.filename, "Could not parse wheel tags; skipping if filters present");
            return !has_tag_filters;
        }
    };

    // Exclusions first
    if !f.exclude_platform_tag.is_empty() && tokens_match_any(&tags.platform, &f.exclude_platform_tag) {
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
    let mut first_part = true;
    for part in pattern.split('*') {
        if part.is_empty() {
            first_part = false;
            continue;
        }
        if let Some(idx) = rest.find(part) {
            rest = &rest[idx + part.len()..];
        } else {
            return false;
        }
        first_part = false;
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
    let py = parts[parts.len() - 3].split('.').map(|s| s.to_string()).collect::<Vec<_>>();
    let abi = parts[parts.len() - 2].split('.').map(|s| s.to_string()).collect::<Vec<_>>();
    let plat = parts[parts.len() - 1].split('.').map(|s| s.to_string()).collect::<Vec<_>>();
    Some(WheelTags {
        python: py,
        abi: abi,
        platform: plat,
    })
}

async fn read_specs(path: &PathBuf) -> Result<Vec<PackageSpec>> {
    let text = fs::read_to_string(path)
        .await
        .with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let spec = parse_spec_line(line)
            .with_context(|| format!("line {}: {}", lineno + 1, raw))?;
        out.push(spec);
    }
    Ok(out)
}

fn parse_spec_line(line: &str) -> Result<PackageSpec> {
    if line.contains("==") {
        return Err(anyhow!(
            "Versions are not allowed in the packages list (found '{}'). \
             Put only the package name (one per line) to mirror ALL releases.",
            line
        ));
    }
    Ok(PackageSpec {
        name: normalize_pkg_name(line.trim()),
    })
}

/// PEP 503 normalization (lowercase; replace runs of [-_.] with single '-').
fn normalize_pkg_name(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut last_dash = false;
    for ch in lower.chars() {
        let is_sep = ch == '-' || ch == '_' || ch == '.';
        if is_sep {
            if !last_dash {
                out.push('-');
                last_dash = true;
            }
        } else {
            out.push(ch);
            last_dash = false;
        }
    }
    out.trim_matches('-').to_string()
}

fn normalize_legacy_endpoint(dst_base: &str) -> String {
    let trimmed = dst_base.trim_end_matches('/');
    if trimmed.ends_with("/legacy") {
        format!("{trimmed}/")
    } else {
        format!("{trimmed}/legacy/")
    }
}