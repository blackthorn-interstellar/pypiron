//! Mirror packages from PyPI into a pypiron server, over HTTP.
//!
//! Sync is a client: each selected file is POSTed to the destination server's
//! `/legacy/` with `mirror=true` plus PyPI's true `upload-time` and yank state,
//! authenticated against the server's admin credential. The server owns every
//! storage write — sync needs a URL (`--to`) and the admin credential, nothing
//! about the server's storage backend.
//!
//! Filters (`--only-wheels`, tag filters, `--exclude-newer`/`--exclude-older`,
//! PEP 440 specifiers in the package list) gate only what a run *adds* — an
//! artifact, once mirrored, is never deleted. A re-sync does, however,
//! *reconcile* the mutable metadata of files it already has: yank state is
//! brought in line with upstream (set, cleared, or its reason updated, via the
//! server's yank endpoint), and a file gone from upstream is flagged yanked
//! `removed upstream` (kept downloadable, but installers skip it). PEP 792
//! project status is relayed the same way, through the server's status endpoint.
//!
//! To make "reconcile every run" cheap, each project is fetched conditionally:
//! the last upstream ETag is remembered (server-side, in `_sync/cursors.json`)
//! and replayed as `If-None-Match`, so an unchanged upstream answers `304` and
//! the whole project is skipped. `--full` ignores the memo and reconciles
//! everything — run it periodically as the self-heal. Options layer as
//! CLI/env > pypiron.toml > defaults.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use futures::stream::{self, StreamExt};
use pep440_rs::{Version, VersionSpecifiers};
use reqwest::{multipart, Client};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};
use tokio::fs;
use tracing::{error, info, warn};

use crate::config::{self, SyncConfig};
use crate::names::{
    checked_pkg_name, infer_version_from_filename, matches_prefix, normalize_pkg_name,
    parse_wheel_tags,
};
use crate::render::SIMPLE_JSON_CONTENT_TYPE;
use crate::sidecar::Yanked;
use crate::simple::{self, IndexFetch, SimpleFile, SimpleIndex};
use crate::status::ProjectStatusDoc;

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

    /// A single package to mirror, same line syntax as a packages-list entry
    /// (name with optional PEP 440 specifiers); repeatable. When any CLI
    /// package is given (`--pkg` and/or `--packages-list`), the CLI set fully
    /// replaces the config file's `[sync].packages`/`packages-list`.
    #[arg(long = "pkg", value_name = "SPEC")]
    pub pkg: Vec<String>,

    /// Source index base (default: https://pypi.org). Read over the PEP 691
    /// Simple API (`/simple/<name>/`), so any PEP 691 index works — PyPI,
    /// another pypiron, etc.
    #[arg(long = "from", env = "PYPIRON_SYNC_FROM")]
    pub src_base: Option<String>,

    /// Destination pypiron base URL. Sync mirrors over HTTP: each file is POSTed
    /// to the server's `/legacy/`, authenticated with the server's admin
    /// credential. Required (here or as `[sync].to`).
    #[arg(long = "to", env = "PYPIRON_SYNC_TO")]
    pub dst_base: Option<String>,

    /// Basic auth username for the destination (the admin credential).
    #[arg(long, env = "PYPIRON_SYNC_USERNAME")]
    pub username: Option<String>,

    /// Basic auth password for the destination (the admin credential).
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

    /// Ignore the conditional-fetch memo: re-fetch every project unconditionally
    /// and fully reconcile (yank/status/removed) what is already mirrored. Run
    /// periodically as the self-heal — a normal run only reconciles projects
    /// whose upstream listing actually changed.
    #[arg(long, env = "PYPIRON_SYNC_FULL")]
    pub full: bool,

    /// Filtering flags (wheel/python/abi/platform/upload-time).
    #[command(flatten)]
    pub filter: FilterArgs,
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

    /// Only mirror files PyPI received before this cutoff (the mirroring twin of
    /// uv's --exclude-newer). Accepts an RFC 3339 timestamp, a friendly duration
    /// ("30 days", "24 hours", "1 week"), or an ISO 8601 duration (P30D, PT24H);
    /// a duration is relative to now. Calendar months/years are not allowed.
    #[arg(long, env = "PYPIRON_SYNC_EXCLUDE_NEWER", value_name = "WHEN")]
    pub exclude_newer: Option<String>,

    /// Only mirror files PyPI received at or after this cutoff. Same formats as
    /// --exclude-newer (RFC 3339 timestamp, or a duration ago like "30 days" /
    /// P30D).
    #[arg(long, env = "PYPIRON_SYNC_EXCLUDE_OLDER", value_name = "WHEN")]
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
    dst_base: String,
    username: Option<String>,
    password: Option<String>,
    private_prefix: Option<String>,
    concurrency: usize,
    package_concurrency: usize,
    dry_run: bool,
    full: bool,
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
        // Package set follows CLI > file: any CLI package source (`--pkg` or
        // `--packages-list`) replaces the file's set entirely (both its
        // `packages-list` and inline `packages`). With no CLI packages, the
        // file's path and inline array combine.
        let mut lines: Vec<String> = Vec::new();
        if args.packages_list.is_some() || !args.pkg.is_empty() {
            if let Some(path) = &args.packages_list {
                let text = fs::read_to_string(path)
                    .await
                    .with_context(|| format!("reading {}", path.display()))?;
                lines.extend(text.lines().map(str::to_string));
            }
            lines.extend(args.pkg.iter().cloned());
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
                "no packages to sync: provide --pkg/--packages-list or [sync].packages in pypiron.toml"
            );
        }

        let filter = &args.filter;
        let only_wheels = filter.only_wheels || cfg.only_wheels.unwrap_or(false);
        let only_sdists = filter.only_sdists || cfg.only_sdists.unwrap_or(false);
        if only_wheels && only_sdists {
            // Would select nothing and "succeed" — a silent empty mirror.
            bail!("only-wheels and only-sdists are mutually exclusive");
        }

        // Sync mirrors over HTTP; a destination is mandatory.
        let dst_base = ensure_http_scheme(args.dst_base.clone().or(cfg.to).ok_or_else(|| {
            anyhow!(
                "no destination: pass --to <server> (or set [sync].to) — sync mirrors over HTTP"
            )
        })?);

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
            full: args.full,
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

/// Parse a "cutoff" value (CLI/env/config) into an absolute instant, matching
/// uv's `--exclude-newer` grammar: an RFC 3339 timestamp, a "friendly" duration
/// (`30 days`, `24 hours`, `1 week`), or an ISO 8601 duration (`P30D`, `PT24H`).
/// A duration is taken relative to now and resolved as a fixed number of seconds
/// — a day is 24 hours, DST is ignored. Calendar months and years are rejected:
/// with no fixed length they can't be reduced to a number of seconds.
pub(crate) fn parse_cutoff(what: &str, value: Option<&String>) -> Result<Option<OffsetDateTime>> {
    let Some(value) = value.map(|v| v.trim()).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    // An absolute RFC 3339 timestamp wins.
    if let Ok(ts) = OffsetDateTime::parse(value, &Rfc3339) {
        return Ok(Some(ts));
    }
    // Otherwise a relative duration, resolved against now.
    if let Some(secs) = parse_duration_secs(value) {
        return Ok(Some(OffsetDateTime::now_utc() - Duration::seconds(secs)));
    }
    bail!(
        "{what} '{value}' is not a valid cutoff: use an RFC 3339 timestamp \
         (e.g. 2026-01-01T00:00:00Z), a friendly duration (e.g. \"30 days\", \"24 hours\", \
         \"1 week\"), or an ISO 8601 duration (e.g. P30D, PT24H). Calendar months and years \
         are not allowed."
    );
}

/// Total seconds in a duration string — friendly (`30 days`) or ISO 8601
/// (`P30D`) — or `None` if it isn't a supported duration. Only fixed-length
/// units (second, minute, hour, day = 24 h, week = 7 d) are accepted; months and
/// years are rejected.
fn parse_duration_secs(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.starts_with(['P', 'p']) {
        parse_iso8601_duration_secs(s)
    } else {
        parse_friendly_duration_secs(s)
    }
}

/// Seconds per fixed-length unit; `None` for months/years and anything unknown.
fn unit_seconds(unit: &str) -> Option<i64> {
    Some(match unit {
        "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600,
        "d" | "day" | "days" => 86_400,
        "w" | "wk" | "wks" | "week" | "weeks" => 604_800,
        _ => return None,
    })
}

/// `30 days`, `24 hours`, `1 week`, `1h30m`, `2 days 5 hours`. Each term is an
/// integer count followed by a unit; whitespace and commas separate terms. Empty
/// input, a bare number, a fraction, or any unknown/calendar unit → `None`.
fn parse_friendly_duration_secs(s: &str) -> Option<i64> {
    let mut total: i64 = 0;
    let mut saw_term = false;
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() || c == ',' {
            chars.next();
            continue;
        }
        let mut num: i64 = 0;
        let mut saw_digit = false;
        while let Some(d) = chars.peek().and_then(|c| c.to_digit(10)) {
            num = num.checked_mul(10)?.checked_add(i64::from(d))?;
            saw_digit = true;
            chars.next();
        }
        if !saw_digit {
            return None;
        }
        // An optional space between the count and its unit ("30 days").
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let mut unit = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_alphabetic() {
                unit.push(c.to_ascii_lowercase());
                chars.next();
            } else {
                break;
            }
        }
        let secs = unit_seconds(&unit)?;
        total = total.checked_add(num.checked_mul(secs)?)?;
        saw_term = true;
    }
    saw_term.then_some(total)
}

/// `P30D`, `PT24H`, `P1W`, `P1DT2H30M`. Date part: weeks and days; time part
/// (after `T`): hours, minutes, seconds. Years (`Y`) and months (`M` before
/// `T`) are rejected; integers only. `None` on anything malformed.
fn parse_iso8601_duration_secs(s: &str) -> Option<i64> {
    let rest = s.strip_prefix(['P', 'p'])?;
    if rest.is_empty() {
        return None;
    }
    let (date_part, time_part) = match rest.split_once(['T', 't']) {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    let mut total: i64 = 0;
    let mut saw_any = false;
    for (num, unit) in iso_terms(date_part)? {
        let secs = match unit.to_ascii_uppercase() {
            'D' => 86_400,
            'W' => 604_800,
            _ => return None, // Y/M (calendar) and anything else
        };
        total = total.checked_add(num.checked_mul(secs)?)?;
        saw_any = true;
    }
    if let Some(time_part) = time_part {
        if time_part.is_empty() {
            return None; // a dangling `T` with no time terms
        }
        for (num, unit) in iso_terms(time_part)? {
            let secs = match unit.to_ascii_uppercase() {
                'H' => 3_600,
                'M' => 60,
                'S' => 1,
                _ => return None,
            };
            total = total.checked_add(num.checked_mul(secs)?)?;
            saw_any = true;
        }
    }
    saw_any.then_some(total)
}

/// Split an ISO 8601 component run into (integer, unit-letter) pairs; `None`
/// unless it's a clean sequence of digits-then-letter.
fn iso_terms(part: &str) -> Option<Vec<(i64, char)>> {
    let mut terms = Vec::new();
    let mut chars = part.chars().peekable();
    while chars.peek().is_some() {
        let mut num: i64 = 0;
        let mut saw_digit = false;
        while let Some(d) = chars.peek().and_then(|c| c.to_digit(10)) {
            num = num.checked_mul(10)?.checked_add(i64::from(d))?;
            saw_digit = true;
            chars.next();
        }
        if !saw_digit {
            return None;
        }
        let unit = chars.next()?;
        if !unit.is_ascii_alphabetic() {
            return None;
        }
        terms.push((num, unit));
    }
    Some(terms)
}

/// `name` with optional PEP 440 specifiers: `requests`, `six==1.16.0`,
/// `requests>=2.20,<3`.
fn parse_spec_line(line: &str) -> Result<PackageSpec> {
    let split = line
        .find(['<', '>', '=', '!', '~', ' '])
        .unwrap_or(line.len());
    let (raw_name, raw_spec) = line.split_at(split);
    let Some(name) = checked_pkg_name(raw_name.trim()) else {
        bail!("invalid package name '{raw_name}'");
    };
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

/// Yank reason stamped on a file that has disappeared from upstream. The bytes
/// stay downloadable (we never delete a mirrored artifact); installers skip it.
const REMOVED_UPSTREAM: &str = "removed upstream";

/// Storage key of the sync-cursor blob — the server-side memo of the last
/// upstream ETag each project synced at, replayed as `If-None-Match`. Lives
/// outside `packages/`/`simple/`, so the worker's membership and index builds
/// never see it; disposable — a lost blob just means the next run full-fetches.
pub(crate) const CURSORS_KEY: &str = "_sync/cursors.json";

/// One project's conditional-fetch memo.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CursorEntry {
    /// The upstream listing ETag last synced (opaque, equality-only).
    etag: String,
    /// Hash of the run config that produced it ([`config_key`]); a mismatch
    /// invalidates the ETag, since a changed filter/specifier/source may select
    /// files the cached listing already contained but we skipped.
    config: String,
    /// PyPI's `X-PyPI-Last-Serial`, when present — diagnostics only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    serial: Option<u64>,
}

/// project name -> its cursor.
type Cursors = HashMap<String, CursorEntry>;

/// A stable hash of everything that changes which files a run *selects*: the
/// source, the resolved filters, and this project's version specifiers. Stored
/// beside the ETag so a config change forces a full fetch instead of trusting a
/// 304 against a listing that was filtered differently. Tag vectors are sorted
/// so argument order doesn't perturb the key.
fn config_key(resolved: &Resolved, spec: &PackageSpec) -> String {
    let f = &resolved.filter;
    let mut h = Sha256::new();
    h.update(resolved.src_base.as_bytes());
    h.update([0]);
    h.update([u8::from(f.only_wheels), u8::from(f.only_sdists)]);
    for tags in [
        &f.python_tag,
        &f.abi_tag,
        &f.platform_tag,
        &f.exclude_platform_tag,
    ] {
        let mut sorted = tags.clone();
        sorted.sort();
        for t in &sorted {
            h.update(t.as_bytes());
            h.update([0]);
        }
        h.update([0x1f]);
    }
    h.update(
        f.exclude_newer
            .map_or(0i64, |d| d.unix_timestamp())
            .to_le_bytes(),
    );
    h.update(
        f.exclude_older
            .map_or(0i64, |d| d.unix_timestamp())
            .to_le_bytes(),
    );
    if let Some(s) = &spec.specifiers {
        h.update(s.to_string().as_bytes());
    }
    format!("{:x}", h.finalize())
}

/// Every filename in the upstream listing mapped to its yank state, captured
/// *before* filtering — reconcile must tell "filtered out, still upstream" from
/// "gone upstream".
struct UpstreamFiles {
    by_name: HashMap<String, Yanked>,
}

impl UpstreamFiles {
    /// What a local file's yank state *should* be: upstream's verdict if the
    /// file is still listed, else flagged [`REMOVED_UPSTREAM`].
    fn desired(&self, filename: &str) -> Yanked {
        match self.by_name.get(filename) {
            Some(y) => y.clone(),
            None => Yanked::Reason(REMOVED_UPSTREAM.to_string()),
        }
    }
}

/// Load the cursor memo for this run from the destination's `/sync/cursors`.
/// `--full` (or any read failure) yields an empty map, which forces
/// unconditional fetches — the memo only ever speeds a run up, never changes
/// its result.
async fn load_cursors(client: &Client, resolved: &Resolved) -> Cursors {
    if resolved.full {
        return Cursors::new();
    }
    let url = format!("{}/sync/cursors", resolved.dst_base.trim_end_matches('/'));
    let mut req = client.get(&url);
    if let (Some(u), Some(p)) = (&resolved.username, &resolved.password) {
        req = req.basic_auth(u, Some(p));
    }
    match req.send().await {
        Ok(resp) if resp.status().is_success() => resp.json().await.unwrap_or_default(),
        _ => Cursors::new(),
    }
}

/// Persist the merged cursor memo to the destination. Best-effort: a failure
/// just means the next run re-fetches, so it must never fail an otherwise-good
/// sync.
async fn save_cursors(client: &Client, resolved: &Resolved, cursors: &Cursors) {
    let body = match serde_json::to_vec(cursors) {
        Ok(b) => b,
        Err(e) => {
            warn!(error=?e, "could not encode sync cursors");
            return;
        }
    };
    let url = format!("{}/sync/cursors", resolved.dst_base.trim_end_matches('/'));
    let mut req = client.put(&url).body(body);
    if let (Some(u), Some(p)) = (&resolved.username, &resolved.password) {
        req = req.basic_auth(u, Some(p));
    }
    let result = async {
        let resp = req.send().await?;
        if !resp.status().is_success() {
            bail!("saving sync cursors failed [{}]", resp.status());
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(e) = result {
        warn!(error=?e, "failed to persist sync cursors (next run re-fetches)");
    }
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

    let endpoint = normalize_legacy_endpoint(&resolved.dst_base);
    info!("mirror-over-HTTP mode: uploading to {endpoint}");

    // The conditional-fetch memo from the last run; an empty map (first run,
    // --full, or any read error) simply means every project full-fetches.
    let cursors = load_cursors(&client, &resolved).await;

    // Packages in parallel (chunked join_all — same pattern as the worker
    // sweep), files within each package in parallel below. The long tail of a
    // mirror is small packages, so serial-per-package was the throughput cap.
    let mut failures = 0usize;
    let mut refreshed: Cursors = Cursors::new();
    for chunk in resolved.specs.chunks(resolved.package_concurrency) {
        let results = futures::future::join_all(chunk.iter().map(|spec| {
            sync_one_package(&client, &resolved, &endpoint, spec, cursors.get(&spec.name))
        }))
        .await;
        for (spec, result) in chunk.iter().zip(results) {
            match result {
                Ok(outcome) => {
                    if let Some(entry) = outcome.new_cursor {
                        refreshed.insert(spec.name.clone(), entry);
                    }
                }
                Err(e) => {
                    error!(package=%spec.name, error=?e, "package sync failed");
                    failures += 1;
                }
            }
        }
    }

    // Keep cursors for projects this run didn't touch (or 304'd), overwrite the
    // ones we re-fetched. A failed project advances nothing — it re-fetches next
    // run. Persisting is best-effort; the memo only ever speeds things up.
    let mut merged = cursors;
    merged.extend(refreshed);
    save_cursors(&client, &resolved, &merged).await;

    if failures > 0 {
        bail!("{failures} package(s) failed to sync");
    }
    Ok(())
}

/// What a single project's sync produced for the run-level cursor memo.
struct PackageOutcome {
    /// `Some` only on a successful 200 fetch with an ETag — the entry to store.
    /// `None` means "leave the existing cursor as-is" (a 304 skip, a dry run, or
    /// a source with no ETag).
    new_cursor: Option<CursorEntry>,
}

async fn sync_one_package(
    client: &Client,
    resolved: &Resolved,
    endpoint: &str,
    spec: &PackageSpec,
    prev_cursor: Option<&CursorEntry>,
) -> Result<PackageOutcome> {
    let pkg = spec.name.as_str();

    // Policy gate before any network traffic. The server enforces it again —
    // defense in both places.
    if let Some(prefix) = &resolved.private_prefix {
        let prefix = normalize_pkg_name(prefix);
        if matches_prefix(pkg, &prefix) {
            bail!("'{pkg}' is inside the private namespace '{prefix}'; refusing to mirror");
        }
    }

    // Conditional fetch: replay last run's ETag (unless its config differs, or
    // this is a dry run that wants the full picture). A 304 means nothing
    // changed upstream — no files to add, nothing to reconcile — so skip.
    let cfg_key = config_key(resolved, spec);
    let if_none_match = if resolved.dry_run {
        None
    } else {
        prev_cursor
            .filter(|c| c.config == cfg_key)
            .map(|c| c.etag.as_str())
    };
    let (index, etag, last_serial) =
        match simple::fetch_index_conditional(client, &resolved.src_base, pkg, None, if_none_match)
            .await?
        {
            IndexFetch::NotModified => {
                info!("{pkg}: upstream unchanged since last sync (304)");
                return Ok(PackageOutcome { new_cursor: None });
            }
            IndexFetch::NotFound => bail!("Package not found on source: {pkg}"),
            IndexFetch::Found {
                index,
                etag,
                last_serial,
            } => (index, etag, last_serial),
        };

    let (selected, upstream_status, upstream_files) = select_from_index(index, resolved, spec);
    info!("Syncing {pkg} ({} matching files selected)", selected.len());

    if resolved.dry_run {
        for s in &selected {
            println!("[dry-run] would copy {} ({})", s.file.filename, s.file.url);
        }
        return Ok(PackageOutcome { new_cursor: None });
    }

    let results: Vec<Result<bool>> = stream::iter(selected)
        .map(|s| async move { upload_via_http(client, resolved, endpoint, pkg, &s).await })
        .buffer_unordered(resolved.concurrency)
        .collect()
        .await;

    let mut errors = 0usize;
    for r in &results {
        if let Err(e) = r {
            error!(package=%pkg, error=?e, "file failed");
            errors += 1;
        }
    }

    // Upstream's authoritative PEP 792 verdict for this run.
    let upstream_blocks = matches!(&upstream_status, Some(doc) if doc.status.blocks_downloads());
    let upstream_frozen = matches!(&upstream_status, Some(doc) if !doc.status.is_active());

    // The dest's own materialized PEP 691 index — read via `/sync/local-index`
    // so the on-demand proxy never shadows it — is the truth that reconcile and
    // status relay diff against. A fetch error fails the package so the cursor
    // doesn't advance over an un-reconciled state.
    let local = match fetch_local_index(client, resolved, pkg).await {
        Ok(local) => local,
        Err(e) => {
            error!(package=%pkg, error=?e, "local-index fetch failed");
            errors += 1;
            None
        }
    };

    // Hold the cursor (force a re-fetch next run) when this run couldn't fully
    // reconcile despite a clean upload — otherwise a 304 next run masks the gap
    // until `--full`.
    let mut hold_cursor = false;

    match &local {
        Some(local) => {
            // Reconcile mutable metadata of files already mirrored: yank
            // set/cleared to match upstream, and files gone upstream flagged
            // removed.
            //
            // A quarantined upstream (PEP 792) MUST offer no files, so its
            // listing is empty by design — not because every file was removed.
            // Skip reconcile then (the status relay below blocks downloads
            // instead); flagging every file "removed upstream" would be both
            // wrong and a storm of churn that reverts when the quarantine lifts.
            let dest_blocks = local
                .project_status
                .as_ref()
                .is_some_and(|d| d.status.blocks_downloads());
            if !upstream_blocks {
                if dest_blocks {
                    // The quarantine is lifting: the dest's index is still the
                    // frozen (empty) render, so there are no files to diff yet.
                    // The relay below clears the freeze; hold the cursor so the
                    // next run reconciles for real once the dest rebuilds.
                    hold_cursor = true;
                } else if let Err(e) =
                    reconcile_yanks(client, resolved, pkg, local, &upstream_files).await
                {
                    error!(package=%pkg, error=?e, "reconcile failed");
                    errors += 1;
                }
            }

            // Relay PEP 792 project status regardless of the block — that relay
            // is how the freeze reaches the dest in the first place.
            // Authoritative for a mirror, so it both sets and clears.
            if let Err(e) = relay_status(client, resolved, pkg, local, &upstream_status).await {
                error!(package=%pkg, error=?e, "status relay failed");
                errors += 1;
            }
        }
        None => {
            // An older dest without `/sync/local-index`: the per-file yank set at
            // upload time still holds, so a plain mirror is fine. But a
            // project-level freeze (quarantine/archive/deprecate) can't be
            // relayed — fail loud rather than silently advance the cursor over an
            // un-enforced freeze (a later 304 would mask it forever).
            if upstream_frozen {
                error!(
                    package=%pkg,
                    "destination has no /sync/local-index endpoint; cannot relay project status — refusing to advance the cursor over an un-enforced freeze"
                );
                errors += 1;
            }
        }
    }

    if errors > 0 {
        bail!("{errors} error(s) syncing '{pkg}'");
    }

    // Advance the cursor only after a clean, fully-reconciled run, so any failure
    // (or a deferred lift-transition reconcile) re-fetches next time. A source
    // without an ETag simply never gets the 304 shortcut.
    let new_cursor = if hold_cursor {
        None
    } else {
        etag.map(|etag| CursorEntry {
            etag,
            config: cfg_key,
            serial: last_serial,
        })
    };
    Ok(PackageOutcome { new_cursor })
}

/// Fetch the destination's locally-materialized PEP 691 index (its own truth:
/// which files it holds, their yank state, and project status). `Ok(None)` means
/// an older dest without the `/sync/local-index` endpoint; reconcile and status
/// relay are then skipped rather than run against a proxied upstream view that
/// would hide a removed file.
async fn fetch_local_index(
    client: &Client,
    resolved: &Resolved,
    pkg: &str,
) -> Result<Option<SimpleIndex>> {
    let url = format!(
        "{}/sync/local-index/{pkg}",
        resolved.dst_base.trim_end_matches('/')
    );
    let mut req = client
        .get(&url)
        .header(reqwest::header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE);
    if let (Some(u), Some(p)) = (&resolved.username, &resolved.password) {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(resp.error_for_status()?.json().await?))
}

/// For each already-mirrored file whose yank state has drifted from upstream,
/// drive the server's `/files/.../yank` endpoint. (Newly-uploaded files already
/// carry the right yank from the upload; this catches drift on files already
/// there and flags removals.)
async fn reconcile_yanks(
    client: &Client,
    resolved: &Resolved,
    pkg: &str,
    local: &SimpleIndex,
    upstream: &UpstreamFiles,
) -> Result<()> {
    let base = resolved.dst_base.trim_end_matches('/');
    for file in &local.files {
        let desired = upstream.desired(&file.filename);
        if file.yanked != desired {
            apply_yank_http(client, resolved, base, pkg, &file.filename, &desired).await?;
        }
    }
    Ok(())
}

/// Set/clear a file's yank on the destination via the admin yank endpoint.
async fn apply_yank_http(
    client: &Client,
    resolved: &Resolved,
    base: &str,
    pkg: &str,
    filename: &str,
    yanked: &Yanked,
) -> Result<()> {
    let url = format!("{base}/files/{pkg}/{filename}/yank");
    let mut req = match yanked {
        Yanked::Flag(false) => client.delete(&url),
        Yanked::Flag(true) => client.post(&url).body(String::new()),
        Yanked::Reason(reason) => client.post(&url).body(reason.clone()),
    };
    if let (Some(u), Some(p)) = (&resolved.username, &resolved.password) {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        let code = resp.status();
        let body = resp.text().await.unwrap_or_else(|_| "<no body>".into());
        bail!("yank update failed for {filename} [{code}]: {body}");
    }
    Ok(())
}

/// Relay PEP 792 project status to match upstream, via the server's status
/// endpoint, when it has drifted. Upstream is authoritative for a mirror, so
/// this both sets a freeze and clears it. `current` comes from the dest's own
/// materialized index, so a no-op run issues no write (and triggers no rebuild).
async fn relay_status(
    client: &Client,
    resolved: &Resolved,
    pkg: &str,
    local: &SimpleIndex,
    upstream_status: &Option<ProjectStatusDoc>,
) -> Result<()> {
    let desired = match upstream_status {
        Some(doc) if !doc.status.is_active() => doc.clone(),
        _ => ProjectStatusDoc::default(),
    };
    let current = local.project_status.clone().unwrap_or_default();
    if current == desired {
        return Ok(());
    }
    let base = resolved.dst_base.trim_end_matches('/');
    let url = format!("{base}/project/{pkg}/status");
    // Active carries no marker, so an active target is a clear (DELETE); any
    // freeze is a POST of the status doc — same set/clear shape as yank.
    let mut req = if desired.status.is_active() {
        client.delete(&url)
    } else {
        client.post(&url).json(&desired)
    };
    if let (Some(u), Some(p)) = (&resolved.username, &resolved.password) {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        let code = resp.status();
        let body = resp.text().await.unwrap_or_else(|_| "<no body>".into());
        bail!("status update failed for {pkg} [{code}]: {body}");
    }
    Ok(())
}

/// From an already-fetched listing, derive the files to add (filtered), the
/// upstream project status, and the full unfiltered filename→yank map that
/// reconcile needs. Pure — the network fetch happens in the caller so it can be
/// conditional.
fn select_from_index(
    index: SimpleIndex,
    resolved: &Resolved,
    spec: &PackageSpec,
) -> (Vec<Selected>, Option<ProjectStatusDoc>, UpstreamFiles) {
    let base_url = format!(
        "{}/simple/{}/",
        resolved.src_base.trim_end_matches('/'),
        spec.name
    );

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

    let upstream_status = index.project_status.clone();
    // Every upstream filename → its yank, captured before filtering: reconcile
    // must distinguish "filtered out, still upstream" from "gone upstream".
    // Normalize to the form the server persists, so reconcile is idempotent
    // even against a sloppy upstream reason (whitespace / empty string).
    let upstream_files = UpstreamFiles {
        by_name: index
            .files
            .iter()
            .map(|f| (f.filename.clone(), f.yanked.normalized()))
            .collect(),
    };

    let mut selected = Vec::new();
    for mut file in index.files {
        // No digest, no service: every artifact we hand out must be verifiable.
        if file.sha256().is_none() {
            continue;
        }
        file.yanked = file.yanked.normalized();
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
    (selected, upstream_status, upstream_files)
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
    Err(last_err.unwrap_or_else(|| anyhow!("download failed for {}", file.filename)))
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

/// Push one file through the remote `/legacy/` as a mirror upload, carrying
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

/// A schemeless `--to` (e.g. `127.0.0.1:8000/simple/`) is a relative URL, which
/// makes every request `reqwest` builds fail with "relative URL without a base".
/// Default a missing scheme to `http://` — sync destinations are typically a
/// local/internal pypiron, not a public TLS host.
fn ensure_http_scheme(dst_base: String) -> String {
    if dst_base.contains("://") {
        dst_base
    } else {
        format!("http://{dst_base}")
    }
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
    fn ensure_http_scheme_defaults_and_preserves() {
        assert_eq!(
            ensure_http_scheme("127.0.0.1:8000/simple/".into()),
            "http://127.0.0.1:8000/simple/"
        );
        assert_eq!(
            ensure_http_scheme("https://dest.example".into()),
            "https://dest.example"
        );
        assert_eq!(
            ensure_http_scheme("http://dest.example".into()),
            "http://dest.example"
        );
    }

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

    fn cutoff(value: &str) -> Result<Option<OffsetDateTime>> {
        parse_cutoff("test", Some(&value.to_string()))
    }

    #[test]
    fn parse_cutoff_accepts_rfc3339_and_durations() {
        // Empty/whitespace is "no cutoff", not an error.
        assert!(cutoff("").unwrap().is_none());
        assert!(cutoff("   ").unwrap().is_none());

        // An absolute RFC 3339 timestamp is taken verbatim.
        assert_eq!(
            cutoff("2020-01-01T00:00:00Z").unwrap().unwrap(),
            OffsetDateTime::parse("2020-01-01T00:00:00Z", &Rfc3339).unwrap()
        );

        // Friendly and ISO 8601 durations resolve to (now - duration), within a
        // small window for the clock advancing across the call.
        for (input, secs) in [
            ("30 days", 30 * 86_400),
            ("24 hours", 24 * 3_600),
            ("1 week", 604_800),
            ("1h30m", 5_400),
            ("2 days 5 hours", 2 * 86_400 + 5 * 3_600),
            ("P30D", 30 * 86_400),
            ("PT24H", 24 * 3_600),
            ("P1W", 604_800),
            ("P1DT2H30M", 86_400 + 2 * 3_600 + 30 * 60),
            ("PT90M", 90 * 60),
        ] {
            let before = OffsetDateTime::now_utc();
            let got = cutoff(input).unwrap().unwrap();
            let after = OffsetDateTime::now_utc();
            let slack = Duration::seconds(5);
            assert!(
                got >= before - Duration::seconds(secs) - slack
                    && got <= after - Duration::seconds(secs) + slack,
                "{input} resolved to {got}, expected ~{secs}s ago"
            );
        }
    }

    #[test]
    fn parse_cutoff_rejects_calendar_units_and_garbage() {
        // Calendar months/years have no fixed length — rejected in both forms.
        for bad in [
            "1 month", "2 months", "1 year", "3 years", "1mo", "P1M", "P1Y", "P3Y6M",
        ] {
            assert!(cutoff(bad).is_err(), "{bad} must be rejected");
        }
        // Not durations or timestamps at all.
        for bad in [
            "tomorrow",
            "2020-01-01",
            "5",
            "30",
            "PT",
            "P",
            "1.5 hours",
            "garbage",
        ] {
            assert!(cutoff(bad).is_err(), "{bad} must be rejected");
        }
    }

    fn resolved_with(filter: ResolvedFilter, src_base: &str) -> Resolved {
        Resolved {
            specs: vec![],
            src_base: src_base.to_string(),
            dst_base: "https://dest.example".to_string(),
            username: None,
            password: None,
            private_prefix: None,
            concurrency: 1,
            package_concurrency: 1,
            dry_run: false,
            full: false,
            filter,
        }
    }

    fn spec(name: &str, specifiers: Option<&str>) -> PackageSpec {
        PackageSpec {
            name: name.to_string(),
            specifiers: specifiers.map(|s| VersionSpecifiers::from_str(s).unwrap()),
        }
    }

    #[test]
    fn desired_yank_reflects_upstream_or_flags_removal() {
        let mut by_name = HashMap::new();
        by_name.insert("a.whl".to_string(), Yanked::Flag(false));
        by_name.insert("b.whl".to_string(), Yanked::Reason("broken".into()));
        let up = UpstreamFiles { by_name };

        // Present + not yanked upstream → not yanked (clears a stale local yank).
        assert_eq!(up.desired("a.whl"), Yanked::Flag(false));
        // Present + yanked upstream with a reason → that reason.
        assert_eq!(up.desired("b.whl"), Yanked::Reason("broken".into()));
        // Gone from upstream → flagged removed (bytes stay downloadable).
        assert_eq!(
            up.desired("gone.whl"),
            Yanked::Reason("removed upstream".into())
        );
    }

    #[test]
    fn config_key_is_stable_and_change_sensitive() {
        let r = resolved_with(time_filter(None, None), "https://pypi.org");
        let s = spec("requests", None);
        let k = config_key(&r, &s);

        // Deterministic across calls.
        assert_eq!(k, config_key(&r, &s));

        // Tag argument order must not matter (vecs are sorted before hashing).
        let mut f1 = time_filter(None, None);
        f1.python_tag = vec!["cp311".into(), "cp310".into()];
        let mut f2 = time_filter(None, None);
        f2.python_tag = vec!["cp310".into(), "cp311".into()];
        assert_eq!(
            config_key(&resolved_with(f1, "https://pypi.org"), &s),
            config_key(&resolved_with(f2, "https://pypi.org"), &s),
        );

        // Source, filter, and specifier changes each invalidate the key.
        assert_ne!(
            k,
            config_key(
                &resolved_with(time_filter(None, None), "https://other.example"),
                &s
            )
        );
        let mut wheels = time_filter(None, None);
        wheels.only_wheels = true;
        assert_ne!(
            k,
            config_key(&resolved_with(wheels, "https://pypi.org"), &s)
        );
        assert_ne!(k, config_key(&r, &spec("requests", Some(">=2"))));
    }
}
