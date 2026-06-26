//! Mirror packages from PyPI into a pypiron server, over HTTP.
//!
//! Sync is a client: each selected file is POSTed to the destination server's
//! `/legacy/` with `mirror=true` plus PyPI's true `upload-time` and yank state,
//! authenticated against the server's admin credential. The server owns every
//! storage write — sync needs a URL (`--to`) and the admin credential, nothing
//! about the server's storage backend.
//!
//! Mirror rules (`--include-format`, tag filters, `--exclude-newer`/
//! `--exclude-older`, PEP 440 specifiers in the package list) gate only what a run *adds* — an
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
use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};
use tokio::fs;
use tracing::{error, info, warn};

use crate::config::{self, ConfigFile, MirrorConfig};
use crate::names::{
    checked_pkg_name, infer_version_from_filename, matches_prefix, normalize_pkg_name,
    parse_wheel_tags, WheelTags,
};
use crate::render::SIMPLE_JSON_CONTENT_TYPE;
use crate::sidecar::Yanked;
use crate::simple::{self, IndexFetch, SimpleFile, SimpleIndex};
use crate::status::ProjectStatusDoc;
use crate::upload::{FinishedSpool, UploadSpool};

#[derive(Debug, Clone, Args)]
pub struct SyncArgs {
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

    /// Admin username for the destination (mirroring is admin-only).
    #[arg(long, env = "PYPIRON_SYNC_ADMIN_USER")]
    pub admin_user: Option<String>,

    /// Admin password for the destination (mirroring is admin-only).
    #[arg(long, env = "PYPIRON_SYNC_ADMIN_PASS")]
    pub admin_pass: Option<String>,

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

    /// Directory for download spool files (default: the system temp dir). Sync
    /// streams each artifact to a spool file before re-uploading, so RAM stays
    /// bounded regardless of wheel size — point this at real disk, not a tmpfs,
    /// when mirroring large (multi-GB) wheels.
    #[arg(long, env = "PYPIRON_SYNC_SPOOL_DIR")]
    pub spool_dir: Option<PathBuf>,

    /// Print actions without downloading/uploading.
    #[arg(long, env = "PYPIRON_SYNC_DRY_RUN")]
    pub dry_run: bool,

    /// Ignore the conditional-fetch memo: re-fetch every project unconditionally
    /// and fully reconcile (yank/status/removed) what is already mirrored. Run
    /// periodically as the self-heal — a normal run only reconciles projects
    /// whose upstream listing actually changed.
    #[arg(long, env = "PYPIRON_SYNC_FULL")]
    pub full: bool,

    /// Disable the live progress meter (files/bytes, rate, and ETA on stderr).
    /// The meter is on by default; the end-of-run summary always prints.
    #[arg(long = "no-progress", env = "PYPIRON_SYNC_NO_PROGRESS")]
    pub no_progress: bool,

    /// Mirror selection flags (names, formats, tags, upload-time).
    #[command(flatten)]
    pub mirror: MirrorArgs,
}

/// The slice of PyPI to mirror/proxy. One surface, flattened into both `sync`
/// and `serve`: the same mirror flags and `PYPIRON_INCLUDE_*`/`PYPIRON_EXCLUDE_*`
/// env vars govern the push mirror and the on-demand proxy alike. The file form
/// is the `[mirror]` table in pypiron.toml (see [`MirrorConfig`]).
#[derive(Debug, Clone, Default, Args)]
pub struct MirrorArgs {
    /// Include these packages: a name with optional PEP 440 specifiers
    /// (e.g. `requests>=2.20,<3`); repeatable. `sync` mirrors exactly these;
    /// the `serve` proxy serves only these from upstream and 404s the rest
    /// (fail-closed). Commas belong to the specifier — pass multiple packages
    /// as repeated `--include-package`, not a comma-joined list (unlike the
    /// comma-splitting tag/format flags). A CLI include package source
    /// (`--include-package` and/or `--include-packages-from`) replaces the config
    /// file's `[mirror].include-packages`/`include-packages-from` entirely.
    #[arg(
        long = "include-package",
        env = "PYPIRON_INCLUDE_PACKAGE",
        value_name = "SPEC"
    )]
    pub include_package: Vec<String>,

    /// File of package specs to include, one per line; same syntax as
    /// `--include-package`. Blank lines and lines beginning with `#` are ignored.
    #[arg(
        long = "include-packages-from",
        env = "PYPIRON_INCLUDE_PACKAGES_FROM",
        value_name = "FILE"
    )]
    pub include_packages_from: Option<PathBuf>,

    /// Exclude these packages from the include set; same syntax as
    /// `--include-package`. Bare names deny the whole project; specifiers deny
    /// only matching versions.
    #[arg(
        long = "exclude-package",
        env = "PYPIRON_EXCLUDE_PACKAGE",
        value_name = "SPEC"
    )]
    pub exclude_package: Vec<String>,

    /// File of package specs to exclude, one per line; same syntax as
    /// `--exclude-package`.
    #[arg(
        long = "exclude-packages-from",
        env = "PYPIRON_EXCLUDE_PACKAGES_FROM",
        value_name = "FILE"
    )]
    pub exclude_packages_from: Option<PathBuf>,

    /// Artifact formats to include: wheel, sdist, other. Comma-separated or repeatable.
    #[arg(
        long = "include-format",
        env = "PYPIRON_INCLUDE_FORMAT",
        value_delimiter = ',',
        value_name = "VALUE"
    )]
    pub include_format: Vec<String>,

    /// Include wheels whose python tag matches any of these (e.g. py3, cp311). Comma-separated or repeatable.
    #[arg(
        long = "include-python-tag",
        env = "PYPIRON_INCLUDE_PYTHON_TAG",
        value_delimiter = ',',
        value_name = "TAG"
    )]
    pub include_python_tag: Vec<String>,

    /// Include wheels whose ABI tag matches any of these (e.g. none, cp311). Comma-separated or repeatable.
    #[arg(
        long = "include-abi-tag",
        env = "PYPIRON_INCLUDE_ABI_TAG",
        value_delimiter = ',',
        value_name = "TAG"
    )]
    pub include_abi_tag: Vec<String>,

    /// Include wheels whose platform tag matches any of these (e.g. any, manylinux2014_x86_64, macosx_*_arm64, win_amd64). Supports '*' wildcard.
    #[arg(
        long = "include-platform-tag",
        env = "PYPIRON_INCLUDE_PLATFORM_TAG",
        value_delimiter = ',',
        value_name = "TAG"
    )]
    pub include_platform_tag: Vec<String>,

    /// Exclude wheels whose platform tag matches any of these (supports '*' wildcard).
    #[arg(
        long = "exclude-platform-tag",
        env = "PYPIRON_EXCLUDE_PLATFORM_TAG",
        value_delimiter = ',',
        value_name = "TAG"
    )]
    pub exclude_platform_tag: Vec<String>,

    /// Only mirror/proxy files received upstream before this cutoff (the
    /// mirroring twin of uv's --exclude-newer). Accepts an RFC 3339 timestamp, a
    /// bare date (2008-12-03), a bare integer of days ago (7), a friendly
    /// duration ("30 days", "24 hours", "1 week"), or an ISO 8601 duration
    /// (P30D, PT24H); durations are relative to now. Calendar months/years are
    /// not allowed.
    #[arg(
        long = "exclude-newer",
        env = "PYPIRON_EXCLUDE_NEWER",
        value_name = "WHEN"
    )]
    pub exclude_newer: Option<String>,

    /// Only mirror/proxy files received upstream at or after this cutoff. Same
    /// formats as --exclude-newer.
    #[arg(
        long = "exclude-older",
        env = "PYPIRON_EXCLUDE_OLDER",
        value_name = "WHEN"
    )]
    pub exclude_older: Option<String>,

    /// Skip wheels built only for Python older than this floor (e.g. "3.10"
    /// drops cp36–cp39 and python-2 wheels). Version-agnostic wheels (py3,
    /// py2.py3), forward-compatible abi3 wheels, and all sdists are kept.
    #[arg(
        long = "exclude-python-below",
        env = "PYPIRON_EXCLUDE_PYTHON_BELOW",
        value_name = "X.Y"
    )]
    pub exclude_python_below: Option<String>,

    /// Skip PEP 440 dev releases (any version with a `.devN` segment).
    #[arg(long = "exclude-dev", env = "PYPIRON_EXCLUDE_DEV")]
    pub exclude_dev: bool,

    /// Skip Windows artifacts: wheels with a win32/win_amd64/win_arm64 platform
    /// tag, plus legacy Windows installers (.exe/.msi and .winXX filenames).
    #[arg(long = "exclude-windows", env = "PYPIRON_EXCLUDE_WINDOWS")]
    pub exclude_windows: bool,

    /// Skip PEP 440 pre-releases: alpha/beta/rc *and* dev releases. The strict
    /// superset of --exclude-dev (keep stable releases only).
    #[arg(long = "exclude-prereleases", env = "PYPIRON_EXCLUDE_PRERELEASES")]
    pub exclude_prereleases: bool,

    /// Skip artifacts larger than this size (e.g. 250MB, 1.5GiB, 1048576). Units
    /// are powers of 1024 (KB == KiB); a bare number is bytes. A file whose size
    /// is absent from the upstream listing is kept.
    #[arg(
        long = "exclude-larger",
        env = "PYPIRON_EXCLUDE_LARGER",
        value_name = "SIZE"
    )]
    pub exclude_larger: Option<String>,

    /// Mirror files yanked upstream (PEP 592). Yanked files are *excluded by
    /// default*; pass this to pull them anyway (still flagged yanked, so a pinned
    /// install resolves). Either way, files already mirrored are never removed —
    /// the filter only gates what a run pulls in.
    #[arg(long = "include-yanked", env = "PYPIRON_INCLUDE_YANKED")]
    pub include_yanked: bool,
}

impl MirrorArgs {
    /// The raw `--exclude-older` input (CLI/env over file), trimmed —
    /// kept verbatim so sync's [`config_key`] hashes a value that is stable
    /// across runs rather than the now-relative instant a duration resolves to.
    pub(crate) fn exclude_older_raw(&self, file: Option<&MirrorConfig>) -> Option<String> {
        self.exclude_older
            .as_deref()
            .or(file.and_then(|f| f.exclude_older.as_deref()))
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    }

    /// Resolve CLI/env over the optional `[mirror]` table into the runtime
    /// predicate. The single path used by both `sync` and the `serve` proxy, so
    /// the two can never drift. Precedence is CLI/env > file > default; a bool
    /// set in the file can be turned *on* but not off by the absence of a flag
    /// (clap cannot express an explicit `false`).
    pub(crate) fn resolve(&self, file: Option<&MirrorConfig>) -> Result<ResolvedMirror> {
        Ok(ResolvedMirror {
            include_packages: self.resolve_include_packages(file)?,
            exclude_packages: self.resolve_exclude_packages(file)?,
            include_format: parse_formats(&pick_vec(
                &self.include_format,
                file.and_then(|f| f.include_format.clone()),
            ))?,
            include_python_tag: pick_vec(
                &self.include_python_tag,
                file.and_then(|f| f.include_python_tag.clone()),
            ),
            include_abi_tag: pick_vec(
                &self.include_abi_tag,
                file.and_then(|f| f.include_abi_tag.clone()),
            ),
            include_platform_tag: pick_vec(
                &self.include_platform_tag,
                file.and_then(|f| f.include_platform_tag.clone()),
            ),
            exclude_platform_tag: pick_vec(
                &self.exclude_platform_tag,
                file.and_then(|f| f.exclude_platform_tag.clone()),
            ),
            exclude_newer: parse_cutoff(
                "exclude-newer",
                self.exclude_newer
                    .as_ref()
                    .or(file.and_then(|f| f.exclude_newer.as_ref())),
            )?,
            exclude_older: parse_cutoff("exclude-older", self.exclude_older_raw(file).as_ref())?,
            exclude_python_below: parse_exclude_python_below(
                self.exclude_python_below
                    .as_deref()
                    .or(file.and_then(|f| f.exclude_python_below.as_deref())),
            )?,
            exclude_dev: self.exclude_dev || file.and_then(|f| f.exclude_dev).unwrap_or(false),
            exclude_windows: self.exclude_windows
                || file.and_then(|f| f.exclude_windows).unwrap_or(false),
            exclude_prereleases: self.exclude_prereleases
                || file.and_then(|f| f.exclude_prereleases).unwrap_or(false),
            exclude_larger: parse_size(
                "exclude-larger",
                self.exclude_larger
                    .as_deref()
                    .or(file.and_then(|f| f.exclude_larger.as_deref())),
            )?,
            // Yanked is excluded by default; --include-yanked (CLI/env) or
            // `[mirror].include-yanked = true` opts back in. Same precedence shape
            // as the other bool filters: CLI can only turn the opt-in *on*, never
            // force-exclude over a file that opted in.
            exclude_yanked: !(self.include_yanked
                || file.and_then(|f| f.include_yanked).unwrap_or(false)),
        })
    }

    fn resolve_include_packages(&self, file: Option<&MirrorConfig>) -> Result<Vec<PackageSpec>> {
        resolve_packages(
            &self.include_package,
            self.include_packages_from.as_deref(),
            file.and_then(|f| f.include_packages.as_ref()),
            file.and_then(|f| f.include_packages_from.as_deref()),
            PackageSourceNames {
                cli_inline: "--include-package",
                cli_from_file: "--include-packages-from",
                table: "[mirror]",
                file_inline: "include-packages",
                file_from_file: "include-packages-from",
                label: "include package",
            },
        )
    }

    fn resolve_exclude_packages(&self, file: Option<&MirrorConfig>) -> Result<Vec<PackageSpec>> {
        resolve_packages(
            &self.exclude_package,
            self.exclude_packages_from.as_deref(),
            file.and_then(|f| f.exclude_packages.as_ref()),
            file.and_then(|f| f.exclude_packages_from.as_deref()),
            PackageSourceNames {
                cli_inline: "--exclude-package",
                cli_from_file: "--exclude-packages-from",
                table: "[mirror]",
                file_inline: "exclude-packages",
                file_from_file: "exclude-packages-from",
                label: "exclude package",
            },
        )
    }
}

struct PackageSourceNames {
    cli_inline: &'static str,
    cli_from_file: &'static str,
    table: &'static str,
    file_inline: &'static str,
    file_from_file: &'static str,
    label: &'static str,
}

/// Resolve one package-spec axis: CLI over file, parsed into specs. A CLI
/// source replaces the matching file source entirely; with no CLI source the
/// file's list file and inline array combine. Empty is allowed here — `serve`
/// reads an empty include set as "any name" and `sync` enforces non-empty
/// includes itself. The list files are read with `std::fs` so this stays
/// synchronous and serves both the async `sync` startup and the `serve` startup
/// through one path.
fn resolve_packages(
    cli_inline: &[String],
    cli_from_file: Option<&Path>,
    file_inline: Option<&Vec<String>>,
    file_from_file: Option<&Path>,
    names: PackageSourceNames,
) -> Result<Vec<PackageSpec>> {
    let mut lines: Vec<String> = Vec::new();
    if cli_from_file.is_some() || !cli_inline.is_empty() {
        if let Some(path) = cli_from_file {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?;
            lines.extend(text.lines().map(str::to_string));
        }
        lines.extend(cli_inline.iter().cloned());
        // Announce the override so a stray CLI package source doesn't
        // silently drop a populated `[mirror]` package source.
        let mut ignored = Vec::new();
        if let Some(n) = file_inline.map(Vec::len).filter(|n| *n > 0) {
            ignored.push(format!("{} ({n} spec(s))", names.file_inline));
        }
        if let Some(path) = file_from_file {
            ignored.push(format!("{} {}", names.file_from_file, path.display()));
        }
        if !ignored.is_empty() {
            info!(
                "CLI {}/{} overrides the pypiron.toml {} {} set (ignoring {})",
                names.cli_inline,
                names.cli_from_file,
                names.table,
                names.label,
                ignored.join(" + "),
            );
        }
    } else {
        if let Some(path) = file_from_file {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?;
            lines.extend(text.lines().map(str::to_string));
        }
        lines.extend(file_inline.cloned().unwrap_or_default());
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
    Ok(specs)
}

/// One package in the scope, with optional PEP 440 version constraints. The
/// name is PEP 503-normalized.
#[derive(Debug, Clone)]
pub(crate) struct PackageSpec {
    pub(crate) name: String,
    pub(crate) specifiers: Option<VersionSpecifiers>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Format {
    Wheel,
    Sdist,
    Other,
}

impl Format {
    fn as_str(self) -> &'static str {
        match self {
            Self::Wheel => "wheel",
            Self::Sdist => "sdist",
            Self::Other => "other",
        }
    }
}

fn parse_formats(values: &[String]) -> Result<Vec<Format>> {
    let mut formats = Vec::new();
    for raw in values {
        for value in raw.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            let format = match value.to_ascii_lowercase().as_str() {
                "wheel" => Format::Wheel,
                "sdist" => Format::Sdist,
                "other" => Format::Other,
                _ => bail!("include-format '{value}' is not valid; use wheel, sdist, or other"),
            };
            if !formats.contains(&format) {
                formats.push(format);
            }
        }
    }
    formats.sort();
    Ok(formats)
}

fn file_format(filename: &str) -> Format {
    let fname = filename.to_ascii_lowercase();
    if fname.ends_with(".whl") {
        Format::Wheel
    } else if fname.ends_with(".tar.gz")
        || fname.ends_with(".tgz")
        || fname.ends_with(".tar.bz2")
        || fname.ends_with(".zip")
    {
        Format::Sdist
    } else {
        Format::Other
    }
}

/// Everything resolved: CLI/env over pypiron.toml over defaults, all inputs
/// parsed and validated up front.
struct Resolved {
    src_base: String,
    dst_base: String,
    admin_user: Option<String>,
    admin_pass: Option<String>,
    private_prefix: Option<String>,
    concurrency: usize,
    package_concurrency: usize,
    spool_dir: PathBuf,
    dry_run: bool,
    full: bool,
    mirror: ResolvedMirror,
    /// The raw `--exclude-older` input (e.g. `"800 days"`), kept verbatim for
    /// [`config_key`]: a relative duration must hash to a value that is *stable*
    /// across runs, or the sync cursor never matches its own prior config and
    /// every run re-fetches. Only the older bound needs this — see [`config_key`].
    exclude_older_raw: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct ResolvedMirror {
    /// The package include scope (the mirror's name axis). Empty means "no name
    /// restriction": `sync` rejects that (it needs an explicit work list), but
    /// the `serve` proxy treats it as "serve any non-private name", preserving
    /// the open-proxy default. Non-empty is a fail-closed allowlist.
    pub(crate) include_packages: Vec<PackageSpec>,
    /// Package deny specs. Empty means no package subtraction.
    pub(crate) exclude_packages: Vec<PackageSpec>,
    pub(crate) include_format: Vec<Format>,
    pub(crate) include_python_tag: Vec<String>,
    pub(crate) include_abi_tag: Vec<String>,
    pub(crate) include_platform_tag: Vec<String>,
    pub(crate) exclude_platform_tag: Vec<String>,
    pub(crate) exclude_newer: Option<OffsetDateTime>,
    pub(crate) exclude_older: Option<OffsetDateTime>,
    /// Minimum Python `(major, minor)`; wheels that target only older Pythons
    /// are dropped. `None` disables the floor.
    pub(crate) exclude_python_below: Option<(u32, u32)>,
    /// Drop PEP 440 dev releases.
    pub(crate) exclude_dev: bool,
    /// Drop Windows artifacts: wheels with a `win*` platform tag and legacy
    /// Windows installers (`.exe`/`.msi`/`.winXX` filenames).
    pub(crate) exclude_windows: bool,
    /// Drop PEP 440 pre-releases (alpha/beta/rc and dev) — superset of
    /// `exclude_dev`.
    pub(crate) exclude_prereleases: bool,
    /// Drop artifacts whose listed size exceeds this many bytes. `None` disables
    /// the ceiling; a file with no size in the listing is kept.
    pub(crate) exclude_larger: Option<u64>,
    /// Drop files yanked upstream (PEP 592).
    pub(crate) exclude_yanked: bool,
}

impl Resolved {
    async fn merge(args: &SyncArgs, cfg: ConfigFile) -> Result<Self> {
        let sync = cfg.sync;

        // Sync mirrors over HTTP; a destination is mandatory.
        let dst_base = ensure_http_scheme(args.dst_base.clone().or(sync.to).ok_or_else(|| {
            anyhow!(
                "no destination: pass --to <server> (or set [sync].to) — sync mirrors over HTTP"
            )
        })?);

        // The mirror selection — package scope included — is resolved through the one
        // shared path (CLI/env over the `[mirror]` table), so a sync run and
        // the serve proxy can never drift. The proxy treats an empty scope as
        // "any name"; sync needs an explicit work list, so it bails here.
        let mirror = args.mirror.resolve(Some(&cfg.mirror))?;
        if mirror.include_packages.is_empty() {
            bail!(
                "no packages to sync: provide --include-package/--include-packages-from or [mirror].include-packages in pypiron.toml; exclude-packages alone is not a work list"
            );
        }
        let exclude_older_raw = args.mirror.exclude_older_raw(Some(&cfg.mirror));

        // A `0` here would mean "no work in flight" — `chunks(0)`/`buffer_unordered(0)`
        // panic or stall — so refuse it rather than silently coercing a typo to 1.
        let concurrency = args.concurrency.or(sync.concurrency).unwrap_or(4);
        let package_concurrency = args
            .package_concurrency
            .or(sync.package_concurrency)
            .unwrap_or(8);
        if concurrency == 0 {
            bail!("--concurrency must be at least 1");
        }
        if package_concurrency == 0 {
            bail!("--package-concurrency must be at least 1");
        }

        Ok(Self {
            src_base: args
                .src_base
                .clone()
                .or(sync.from)
                .unwrap_or_else(|| "https://pypi.org".to_string()),
            dst_base,
            admin_user: args.admin_user.clone().or(sync.admin_user),
            admin_pass: args.admin_pass.clone().or(sync.admin_pass),
            private_prefix: args.private_prefix.clone().or(cfg.private_prefix),
            concurrency,
            package_concurrency,
            spool_dir: args.spool_dir.clone().unwrap_or_else(std::env::temp_dir),
            dry_run: args.dry_run,
            full: args.full,
            mirror,
            exclude_older_raw,
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
    // A bare calendar date (no time) — the common "since 2008-12-03" cutoff,
    // taken as that day at 00:00:00 UTC.
    if let Some(ts) = parse_bare_date(value) {
        return Ok(Some(ts));
    }
    // A bare integer is that many days ago, so `--exclude-newer 7` means "before
    // 7 days ago" — the obvious reading of a unit-less count of days.
    if let Ok(days) = value.parse::<i64>() {
        if days >= 0 {
            return Ok(Some(OffsetDateTime::now_utc() - Duration::days(days)));
        }
    }
    // Otherwise a relative duration, resolved against now.
    if let Some(secs) = parse_duration_secs(value) {
        return Ok(Some(OffsetDateTime::now_utc() - Duration::seconds(secs)));
    }
    bail!(
        "{what} '{value}' is not a valid cutoff: use an RFC 3339 timestamp \
         (e.g. 2026-01-01T00:00:00Z), a bare date (e.g. 2008-12-03), a number of days ago \
         (e.g. 7), a friendly duration (e.g. \"30 days\", \"24 hours\", \"1 week\"), or an \
         ISO 8601 duration (e.g. P30D, PT24H). Calendar months and years are not allowed."
    );
}

/// A bare calendar date `YYYY-MM-DD` → that day at 00:00:00 UTC.
fn parse_bare_date(value: &str) -> Option<OffsetDateTime> {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    time::Date::parse(value, fmt)
        .ok()
        .map(|d| d.midnight().assume_utc())
}

/// Parse a Python floor like `3.10` (or bare `3`) into `(major, minor)`.
/// Components past the first two are ignored (`3.10.2` → `(3, 10)`).
pub(crate) fn parse_exclude_python_below(value: Option<&str>) -> Result<Option<(u32, u32)>> {
    let Some(value) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let invalid = || anyhow!("exclude-python-below '{value}' is not a version like 3.10");
    let mut parts = value.split('.');
    let major = parts
        .next()
        .and_then(|p| p.parse::<u32>().ok())
        .ok_or_else(invalid)?;
    let minor = match parts.next() {
        Some(p) => p.parse::<u32>().map_err(|_| invalid())?,
        None => 0,
    };
    Ok(Some((major, minor)))
}

/// Parse a human size — `250MB`, `1.5GiB`, `1048576`, `500k` — into bytes. Units
/// are powers of 1024 (so `KB` == `KiB`); a bare number is bytes. Empty/absent is
/// `None` (no ceiling), not an error. `what` names the flag for error context.
pub(crate) fn parse_size(what: &str, value: Option<&str>) -> Result<Option<u64>> {
    let Some(value) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let split = value
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let (num, unit) = value.split_at(split);
    let num: f64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow!("{what} '{value}' is not a size like 250MB or 1048576"))?;
    if !num.is_finite() || num < 0.0 {
        bail!("{what} '{value}' must be a non-negative size");
    }
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        "t" | "tb" | "tib" => 1 << 40,
        other => bail!("{what} '{value}' has unknown unit '{other}' (use B, KB, MB, GB, TB)"),
    };
    Ok(Some((num * mult as f64) as u64))
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

pub(crate) fn spec_matches_filename(specifiers: &VersionSpecifiers, filename: &str) -> bool {
    infer_version_from_filename(filename)
        .and_then(|v| Version::from_str(&v).ok())
        .is_some_and(|v| specifiers.contains(&v))
}

fn package_fully_denied(exclude_packages: &[PackageSpec], pkg: &str) -> bool {
    exclude_packages
        .iter()
        .any(|spec| spec.name == pkg && spec.specifiers.is_none())
}

fn package_file_denied(exclude_packages: &[PackageSpec], pkg: &str, filename: &str) -> bool {
    exclude_packages
        .iter()
        .filter(|spec| spec.name == pkg)
        .any(|spec| match &spec.specifiers {
            None => true,
            Some(specifiers) => spec_matches_filename(specifiers, filename),
        })
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
    let m = &resolved.mirror;
    let mut h = Sha256::new();
    h.update(resolved.src_base.as_bytes());
    h.update([0]);
    for format in &m.include_format {
        h.update(format.as_str().as_bytes());
        h.update([0]);
    }
    h.update([0x20]);
    for tags in [
        &m.include_python_tag,
        &m.include_abi_tag,
        &m.include_platform_tag,
        &m.exclude_platform_tag,
    ] {
        let mut sorted = tags.clone();
        sorted.sort();
        for t in &sorted {
            h.update(t.as_bytes());
            h.update([0]);
        }
        h.update([0x1f]);
    }
    // The two cutoffs are hashed asymmetrically — on purpose, don't "tidy" it.
    //
    // `--exclude-newer` keeps its *resolved* instant: an absolute timestamp is
    // stable across runs anyway, and a relative one ("30 days") is meant to
    // slide — as releases age past the upper bound they become eligible, which a
    // 304 would silently miss, so its config must change each run to force a
    // re-fetch and re-evaluation.
    h.update(
        m.exclude_newer
            .map_or(0i64, |d| d.unix_timestamp())
            .to_le_bytes(),
    );
    // `--exclude-older` hashes its raw *input* instead, so a relative duration
    // stays stable run to run. Its bound only ever slides files *out* of the
    // set and a mirror never deletes, so there is nothing to re-evaluate —
    // letting the cursor 304 a quiet package. Hashing the resolved instant here
    // (as the newer bound does) would change every run and defeat the cursor for
    // every relative-duration mirror.
    h.update(
        resolved
            .exclude_older_raw
            .as_deref()
            .unwrap_or("")
            .as_bytes(),
    );
    h.update([0x1e]);
    h.update([
        u8::from(m.exclude_dev),
        u8::from(m.exclude_windows),
        u8::from(m.exclude_prereleases),
        u8::from(m.exclude_yanked),
    ]);
    if let Some((maj, min)) = m.exclude_python_below {
        h.update(maj.to_le_bytes());
        h.update(min.to_le_bytes());
    }
    h.update(m.exclude_larger.unwrap_or(0).to_le_bytes());
    h.update([0x1c]);
    let mut excludes: Vec<(String, String)> = m
        .exclude_packages
        .iter()
        .map(|spec| {
            (
                spec.name.clone(),
                spec.specifiers
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            )
        })
        .collect();
    excludes.sort();
    for (name, specifiers) in excludes {
        h.update(name.as_bytes());
        h.update([0]);
        h.update(specifiers.as_bytes());
        h.update([0]);
    }
    h.update([0x1d]);
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

/// Verify the destination is reachable and the admin credentials are accepted
/// before doing any work. One authenticated GET to `/sync/cursors` (admin-gated,
/// cheap) distinguishes the three ways a run is doomed from the start —
/// unreachable server, wrong/missing credentials, or a `--to` that isn't a
/// pypiron — and turns each into a single actionable error instead of letting it
/// recur once per file.
async fn preflight(client: &Client, resolved: &Resolved) -> Result<()> {
    let url = format!("{}/sync/cursors", resolved.dst_base.trim_end_matches('/'));
    let mut req = client.get(&url);
    if let (Some(u), Some(p)) = (&resolved.admin_user, &resolved.admin_pass) {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await.with_context(|| {
        format!(
            "cannot reach sync destination {} — is the server running and the URL correct?",
            resolved.dst_base
        )
    })?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let detail = resp.text().await.unwrap_or_default();
    let detail = detail.trim();
    match status {
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => bail!(
            "sync destination {} rejected the admin credentials [{status}]{}{} \
             — check --admin-user/--admin-pass (or [sync] in the config)",
            resolved.dst_base,
            if detail.is_empty() { "" } else { ": " },
            detail
        ),
        reqwest::StatusCode::NOT_FOUND => bail!(
            "sync destination {} has no /sync/cursors endpoint [404] \
             — does --to point at a pypiron server?",
            resolved.dst_base
        ),
        _ => bail!(
            "sync destination {} preflight failed [{status}]",
            resolved.dst_base
        ),
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
    if let (Some(u), Some(p)) = (&resolved.admin_user, &resolved.admin_pass) {
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
    if let (Some(u), Some(p)) = (&resolved.admin_user, &resolved.admin_pass) {
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

/// Live throughput meter for a sync run — a tqdm-style one-liner on stderr with
/// rate and ETA. Counters are atomic so the background ticker can read them while
/// the package/file fan-out updates them.
struct Progress {
    start: Instant,
    pkgs_total: usize,
    pkgs_done: AtomicUsize,
    files_done: AtomicU64,
    bytes_done: AtomicU64,
    bytes_seen: AtomicU64,
    skipped: AtomicU64,
    errors: AtomicU64,
    /// Signalled when the run is over so the ticker stops immediately instead of
    /// sleeping out its interval.
    done: tokio::sync::Notify,
}

impl Progress {
    fn new(pkgs_total: usize) -> Self {
        Self {
            start: Instant::now(),
            pkgs_total,
            pkgs_done: AtomicUsize::new(0),
            files_done: AtomicU64::new(0),
            bytes_done: AtomicU64::new(0),
            bytes_seen: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            done: tokio::sync::Notify::new(),
        }
    }

    /// Files newly selected to upload for a package (and the bytes they'll move),
    /// fed into the ETA's total-size extrapolation.
    fn discover(&self, bytes: u64) {
        self.bytes_seen.fetch_add(bytes, Ordering::Relaxed);
    }
    fn skip(&self, n: u64) {
        self.skipped.fetch_add(n, Ordering::Relaxed);
    }
    fn file_done(&self, bytes: u64) {
        self.files_done.fetch_add(1, Ordering::Relaxed);
        self.bytes_done.fetch_add(bytes, Ordering::Relaxed);
    }
    fn file_err(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    fn package_done(&self) {
        self.pkgs_done.fetch_add(1, Ordering::Relaxed);
    }

    /// The live status line; `final_` swaps the running prefix for a summary.
    fn render(&self, final_: bool) -> String {
        let el = self.start.elapsed().as_secs_f64();
        let pkgs_done = self.pkgs_done.load(Ordering::Relaxed);
        let files_done = self.files_done.load(Ordering::Relaxed);
        let bytes_done = self.bytes_done.load(Ordering::Relaxed);
        let bytes_seen = self.bytes_seen.load(Ordering::Relaxed);
        let skipped = self.skipped.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let rate = if el > 0.0 {
            bytes_done as f64 / el
        } else {
            0.0
        };

        if final_ {
            return format!(
                "sync done: {pkgs_done}/{} pkgs · {} files · {} in {} (avg {}/s) · {skipped} already present · {errors} errors",
                self.pkgs_total,
                fmt_count(files_done),
                human_bytes(bytes_done),
                human_dur(el),
                human_bytes(rate as u64),
            );
        }

        let pct = if self.pkgs_total > 0 {
            100.0 * pkgs_done as f64 / self.pkgs_total as f64
        } else {
            0.0
        };
        // Extrapolate the run's total bytes from the fraction of packages done,
        // then divide the remaining bytes by the current rate. Unknown until at
        // least one package has finished and some bytes have moved.
        let eta = if pkgs_done > 0 && bytes_done > 0 {
            let est_total = bytes_seen as f64 * self.pkgs_total as f64 / pkgs_done as f64;
            let remaining = (est_total - bytes_done as f64).max(0.0);
            format!(", ~{} left", human_dur(remaining / rate))
        } else {
            String::new()
        };
        format!(
            "sync {pkgs_done}/{} pkgs {pct:.0}% · {} files {} · {}/s {:.1} f/s · {} elapsed{eta} · {errors} err",
            self.pkgs_total,
            fmt_count(files_done),
            human_bytes(bytes_done),
            human_bytes(rate as u64),
            if el > 0.0 { files_done as f64 / el } else { 0.0 },
            human_dur(el),
        )
    }
}

/// Spawn the background ticker. On a TTY it overwrites one line each second; when
/// stderr is redirected (a log file) it prints a fresh line every 30s instead.
fn spawn_progress(progress: Arc<Progress>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let tty = std::io::stderr().is_terminal();
        let interval = std::time::Duration::from_secs(if tty { 1 } else { 30 });
        loop {
            // Wake on the interval or the instant the run finishes — whichever
            // comes first — so a fast run isn't padded out to a full interval.
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = progress.done.notified() => break,
            }
            if tty {
                eprint!("\r\x1b[2K{}", progress.render(false));
                let _ = std::io::Write::flush(&mut std::io::stderr());
            } else {
                eprintln!("{}", progress.render(false));
            }
        }
        if tty {
            // Leave the carriage at column 0 so the summary prints on its own line.
            eprintln!();
        }
    })
}

/// SI byte sizes (kB = 1000), matching how PyPI/uv report mirror sizes.
fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "kB", "MB", "GB", "TB", "PB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1000.0 && i < UNITS.len() - 1 {
        v /= 1000.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

/// Compact elapsed/ETA: `6h12m`, `12m04s`, `9s`.
fn human_dur(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    let (h, m, sec) = (s / 3600, (s % 3600) / 60, s % 60);
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{sec:02}s")
    } else {
        format!("{sec}s")
    }
}

/// Integer with thousands separators: `353417` → `353,417`.
fn fmt_count(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

pub async fn run_sync(args: SyncArgs, config_path: Option<PathBuf>) -> Result<()> {
    let cfg = config::load(config_path.as_deref())?;
    let resolved = Resolved::merge(&args, cfg).await?;

    let client = Client::builder()
        .user_agent("pypiron-sync/0.1 (+https://github.com/blackthorn-interstellar/pypiron)")
        // Bound the handshake and any mid-stream stall so a dead/dribbling
        // upstream fails a sync task cleanly (the retry loop absorbs it) instead
        // of hanging forever. read_timeout is per-read and resets on each chunk,
        // so it never bounds a large artifact that keeps streaming. The 300 s (vs
        // a tighter handshake) covers the *quiet* wait after a multi-GB upload is
        // sent, while the destination hashes it and PUTs it to object storage
        // before answering — at 30 s a 2–3 GB wheel (torch, the nvidia-cu* CUDA
        // libs, tensorflow) timed the client out mid-write and never persisted.
        .connect_timeout(std::time::Duration::from_secs(10))
        .read_timeout(std::time::Duration::from_secs(300))
        .build()?;

    let endpoint = normalize_legacy_endpoint(&resolved.dst_base);
    info!("mirror-over-HTTP mode: uploading to {endpoint}");

    // Probe the destination once before any per-file work: a dead server or bad
    // credentials becomes a single clear error here, not the same failure echoed
    // once per file.
    preflight(&client, &resolved).await?;

    // The conditional-fetch memo from the last run; an empty map (first run,
    // --full, or any read error) simply means every project full-fetches.
    let cursors = load_cursors(&client, &resolved).await;

    // Packages in parallel (chunked join_all — same pattern as the worker
    // sweep), files within each package in parallel below. The long tail of a
    // mirror is small packages, so serial-per-package was the throughput cap.
    let progress = Arc::new(Progress::new(resolved.mirror.include_packages.len()));
    let ticker = (!args.no_progress).then(|| spawn_progress(progress.clone()));
    let mut failures = 0usize;
    let mut refreshed: Cursors = Cursors::new();
    for chunk in resolved
        .mirror
        .include_packages
        .chunks(resolved.package_concurrency)
    {
        let results = futures::future::join_all(chunk.iter().map(|spec| {
            sync_one_package(
                &client,
                &resolved,
                &endpoint,
                spec,
                cursors.get(&spec.name),
                &progress,
            )
        }))
        .await;
        for (spec, result) in chunk.iter().zip(results) {
            progress.package_done();
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

    // Stop the ticker, then print the always-on summary on its own line.
    progress.done.notify_one();
    if let Some(ticker) = ticker {
        let _ = ticker.await;
    }
    eprintln!("{}", progress.render(true));

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
    progress: &Progress,
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
    if package_fully_denied(&resolved.mirror.exclude_packages, pkg) {
        info!("{pkg}: excluded by mirror package denylist");
        return Ok(PackageOutcome { new_cursor: None });
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

    let mut errors = 0usize;

    // The dest's own materialized PEP 691 index — read via `/sync/local-index`
    // so the on-demand proxy never shadows it — serves double duty: it tells us
    // which files are already mirrored (skipped below, so a re-run does no work)
    // and it is the truth reconcile and status relay diff against further down.
    // A fetch error fails the package so the cursor doesn't advance over an
    // un-reconciled state, and forgoes the skip — we fall back to uploading
    // everything, which the server 409s for the duplicates.
    let local = match fetch_local_index(client, resolved, pkg).await {
        Ok(local) => local,
        Err(e) => {
            error!(package=%pkg, error=?e, "local-index fetch failed");
            errors += 1;
            None
        }
    };

    // Skip files the destination already holds: re-uploading one only earns a
    // 409 after a wasted download. The server keys that 409 on the filename (the
    // storage key), so a filename match is exactly "already present".
    let (selected, already_present) = {
        let present: HashSet<&str> = local
            .as_ref()
            .map(|l| l.files.iter().map(|f| f.filename.as_str()).collect())
            .unwrap_or_default();
        let total = selected.len();
        let to_upload: Vec<Selected> = selected
            .into_iter()
            .filter(|s| !present.contains(s.file.filename.as_str()))
            .collect();
        let already_present = total - to_upload.len();
        (to_upload, already_present)
    };

    let sel_bytes: u64 = selected.iter().map(|s| s.file.size.unwrap_or(0)).sum();
    progress.discover(sel_bytes);
    progress.skip(already_present as u64);
    if already_present > 0 {
        info!(
            "Syncing {pkg} ({} new, {already_present} already mirrored)",
            selected.len()
        );
    } else {
        info!("Syncing {pkg} ({} matching files selected)", selected.len());
    }

    if resolved.dry_run {
        for s in &selected {
            println!("[dry-run] would copy {} ({})", s.file.filename, s.file.url);
        }
        return Ok(PackageOutcome { new_cursor: None });
    }

    let results: Vec<Result<bool>> = stream::iter(selected)
        .map(|s| async move {
            let r = upload_via_http(client, resolved, endpoint, pkg, &s).await;
            match &r {
                Ok(_) => progress.file_done(s.file.size.unwrap_or(0)),
                Err(_) => progress.file_err(),
            }
            r
        })
        .buffer_unordered(resolved.concurrency)
        .collect()
        .await;

    for r in &results {
        if let Err(e) = r {
            error!(package=%pkg, error=?e, "file failed");
            errors += 1;
        }
    }

    // Upstream's authoritative PEP 792 verdict for this run.
    let upstream_blocks = matches!(&upstream_status, Some(doc) if doc.status.blocks_downloads());
    let upstream_frozen = matches!(&upstream_status, Some(doc) if !doc.status.is_active());

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
    if let (Some(u), Some(p)) = (&resolved.admin_user, &resolved.admin_pass) {
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
    if let (Some(u), Some(p)) = (&resolved.admin_user, &resolved.admin_pass) {
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
    if let (Some(u), Some(p)) = (&resolved.admin_user, &resolved.admin_pass) {
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
        if !matches_mirror(&file, &resolved.mirror) {
            continue;
        }
        let version = infer_version_from_filename(&file.filename);
        if let Some(specifiers) = &spec.specifiers {
            // A specifier gates by version, which the Simple API doesn't carry —
            // we infer it from the filename. A file whose version can't be
            // parsed can't be proven to match, so it's skipped (the same
            // conservative rule the release-keyed API applied to junk versions).
            if !spec_matches_filename(specifiers, &file.filename) {
                continue;
            }
        }
        if package_file_denied(
            &resolved.mirror.exclude_packages,
            &spec.name,
            &file.filename,
        ) {
            continue;
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

/// Provenance objects are small JSON, and the destination's upload path rejects
/// a `provenance` field past 4 MiB — so cap the fetch at the same bound rather
/// than buffer an unbounded hostile-upstream body into RAM.
const MAX_PROVENANCE_BYTES: u64 = 4 * 1024 * 1024;

/// Best-effort provenance fetch — supplemental, never fails the file.
async fn download_provenance(client: &Client, url: &str) -> Option<Vec<u8>> {
    let resp = match client
        .get(url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
    {
        Ok(resp) => resp,
        Err(e) => {
            warn!(%url, error=?e, "sync: provenance fetch failed");
            return None;
        }
    };
    if resp
        .content_length()
        .is_some_and(|len| len > MAX_PROVENANCE_BYTES)
    {
        warn!(%url, "sync: provenance exceeds cap (Content-Length)");
        return None;
    }
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                warn!(%url, error=?e, "sync: provenance body read failed");
                return None;
            }
        };
        if buf.len() as u64 + chunk.len() as u64 > MAX_PROVENANCE_BYTES {
            warn!(%url, "sync: provenance exceeds cap");
            return None;
        }
        buf.extend_from_slice(&chunk);
    }
    Some(buf)
}

/// How many times a single file download is attempted before the package is
/// marked failed. At mirror scale, transient CDN errors are a statistical
/// certainty — one 503 in 7,714 files failed an entire sync run before this
/// existed. Hash mismatches retry too: a truncated body looks identical.
const DOWNLOAD_ATTEMPTS: u32 = 3;

async fn download_verified(
    client: &Client,
    file: &SimpleFile,
    spool_dir: &Path,
) -> Result<FinishedSpool> {
    let expected = file
        .sha256()
        .ok_or_else(|| anyhow!("no sha256 for {}", file.filename))?;
    let mut last_err = None;
    for attempt in 1..=DOWNLOAD_ATTEMPTS {
        match download_once(client, file, spool_dir).await {
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
            warn!(file=%file.filename, attempt, error=?last_err, "download failed; retrying");
            tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("download failed for {}", file.filename)))
}

/// Stream the artifact to a spool file on disk, hashing as it lands, so RAM
/// stays chunk-sized regardless of wheel size. The old path read the whole body
/// into a `Vec` — and at default concurrency ~32 artifacts were resident at
/// once, OOMing a small box. The upstream-declared `size` bounds the read: a
/// body that overruns it is wrong (it would fail the sha check anyway) and is
/// aborted before it can fill the disk.
async fn download_once(
    client: &Client,
    file: &SimpleFile,
    spool_dir: &Path,
) -> Result<FinishedSpool> {
    let resp = client.get(&file.url).send().await?.error_for_status()?;
    let mut spool = UploadSpool::new(spool_dir).await?;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        spool.write_chunk(&chunk?).await?;
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
    // Held until after the POST below: dropping the spool deletes its temp file.
    let spool = download_verified(client, &s.file, &resolved.spool_dir).await?;

    info!("  - uploading {}", s.file.filename);
    // Stream the spool file into the multipart body instead of re-buffering it
    // in RAM (the artifact already lives on disk from the download).
    let file = fs::File::open(spool.path.path())
        .await
        .with_context(|| format!("reopening spool for {}", s.file.filename))?;
    let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
    let part = multipart::Part::stream_with_length(body, spool.size)
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
    if let (Some(u), Some(p)) = (resolved.admin_user.as_ref(), resolved.admin_pass.as_ref()) {
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

pub(crate) fn matches_mirror(file: &SimpleFile, m: &ResolvedMirror) -> bool {
    let fname = file.filename.to_ascii_lowercase();
    let format = file_format(&fname);
    let is_wheel = format == Format::Wheel;

    if !m.include_format.is_empty() && !m.include_format.contains(&format) {
        return false;
    }

    // Yank and size gate every file. A file yanked upstream (PEP 592) is dropped;
    // a file larger than the ceiling is dropped, but one whose size is missing
    // from the listing is kept (we can't prove it exceeds).
    if m.exclude_yanked && is_yanked(&file.yanked) {
        return false;
    }
    if let Some(max) = m.exclude_larger {
        if file.size.is_some_and(|s| s > max) {
            return false;
        }
    }

    // Pre-release / dev gates every file — sdists too. The version isn't in the
    // Simple API, so it's inferred from the filename; a file whose version can't
    // be parsed is kept (we can't prove it's a pre-release). `exclude_prereleases`
    // (alpha/beta/rc + dev) is the superset of `exclude_dev` (dev only).
    if m.exclude_dev || m.exclude_prereleases {
        if let Some(v) = infer_version_from_filename(&file.filename) {
            if m.exclude_prereleases && is_prerelease_version(&v) {
                return false;
            }
            if m.exclude_dev && is_dev_version(&v) {
                return false;
            }
        }
    }

    // Upload-time bounds. With a bound set, a file without a parseable
    // timestamp is excluded — same rule uv applies to --exclude-newer.
    if m.exclude_newer.is_some() || m.exclude_older.is_some() {
        let uploaded = file
            .upload_time
            .as_deref()
            .and_then(|ts| OffsetDateTime::parse(ts, &Rfc3339).ok());
        let Some(uploaded) = uploaded else {
            return false;
        };
        if m.exclude_newer.is_some_and(|cutoff| uploaded >= cutoff) {
            return false;
        }
        if m.exclude_older.is_some_and(|cutoff| uploaded < cutoff) {
            return false;
        }
    }

    // Only *inclusion* filters gate non-wheels (sdists have no tags). An
    // exclusion-only filter (e.g. --exclude-platform-tag win*) must not silently
    // drop every sdist — an sdist can't match a platform exclusion.
    let has_inclusion_filters = !(m.include_python_tag.is_empty()
        && m.include_abi_tag.is_empty()
        && m.include_platform_tag.is_empty());

    if !is_wheel {
        // A non-wheel can still be a Windows installer (.exe/.msi/.winXX) — those
        // carry no wheel tags, so platform exclusion can't reach them.
        if m.exclude_windows && is_windows_installer_filename(&fname) {
            return false;
        }
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
    if m.exclude_windows && is_windows_wheel_platform(&tags) {
        return false;
    }
    if !m.exclude_platform_tag.is_empty()
        && tokens_match_any(&tags.platform, &m.exclude_platform_tag)
    {
        return false;
    }
    if let Some(floor) = m.exclude_python_below {
        if !wheel_reaches_python_floor(&tags, floor) {
            return false;
        }
    }

    if !m.include_python_tag.is_empty() && !tokens_match_any(&tags.python, &m.include_python_tag) {
        return false;
    }
    if !m.include_abi_tag.is_empty() && !tokens_match_any(&tags.abi, &m.include_abi_tag) {
        return false;
    }
    if !m.include_platform_tag.is_empty()
        && !tokens_match_any(&tags.platform, &m.include_platform_tag)
    {
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

/// True if a wheel's platform tags are Windows. Wheel platform tags are a fixed
/// vocabulary (any / manylinux* / musllinux* / macosx* / win*), so a `win`
/// prefix is unambiguous here — unlike a raw filename, where a package name can
/// start with "win" (windrose, winnow).
fn is_windows_wheel_platform(tags: &WheelTags) -> bool {
    tags.platform
        .iter()
        .any(|p| p.to_ascii_lowercase().starts_with("win"))
}

/// True if a non-wheel filename is a Windows installer: `.exe`/`.msi` (always
/// Windows) or a legacy `.winXX` platform segment (`.win32`, `.win-amd64`,
/// `.win_amd64`, …). The leading dot anchors the marker to a platform component,
/// so an sdist whose name merely starts with "win" is never matched. `fname`
/// must already be lowercased.
fn is_windows_installer_filename(fname: &str) -> bool {
    const MARKERS: [&str; 8] = [
        ".win32",
        ".win64",
        ".win-amd64",
        ".win_amd64",
        ".win-arm64",
        ".win_arm64",
        ".win-ia64",
        ".win_ia64",
    ];
    fname.ends_with(".exe") || fname.ends_with(".msi") || MARKERS.iter().any(|m| fname.contains(m))
}

/// True if `version` (a PEP 440 string) is a dev release — i.e. its canonical
/// form carries a `.devN` segment. Canonicalizing first normalizes the handful
/// of legacy spellings (`1.0dev1`, `1.0-dev1`); an unparseable version falls
/// back to a substring check.
fn is_dev_version(version: &str) -> bool {
    let canon = Version::from_str(version)
        .map(|v| v.to_string())
        .unwrap_or_else(|_| version.to_string());
    canon.to_ascii_lowercase().contains(".dev")
}

/// True if `version` (a PEP 440 string) is a pre-release — an alpha/beta/rc *or*
/// dev release. An unparseable version is treated as not-a-pre-release (kept):
/// we can't prove it is one. The superset of [`is_dev_version`].
fn is_prerelease_version(version: &str) -> bool {
    Version::from_str(version).is_ok_and(|v| v.any_prerelease())
}

/// True if a file is yanked upstream (PEP 592): anything but an explicit
/// `false`. A bare `true` or any reason string counts as yanked.
fn is_yanked(yanked: &Yanked) -> bool {
    !matches!(yanked, Yanked::Flag(false))
}

/// The interpreter version a PEP 427 python tag pins to: strip the leading
/// interpreter letters (cp/py/pp/…), then read the leading digit run as a
/// major plus optional minor. `cp39` → `(3, Some(9))`, `cp310` → `(3, Some(10))`,
/// `py3` → `(3, None)`, `cp313t` → `(3, Some(13))`. `None` when there's no
/// version (e.g. an unrecognized tag).
fn interp_tag_version(tag: &str) -> Option<(u32, Option<u32>)> {
    let after = tag.trim_start_matches(|c: char| c.is_ascii_alphabetic());
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    let mut chars = digits.chars();
    let major = chars.next()?.to_digit(10)?;
    let rest: String = chars.collect();
    let minor = if rest.is_empty() {
        None
    } else {
        rest.parse::<u32>().ok()
    };
    Some((major, minor))
}

/// Whether a wheel's tags reach a minimum Python `(major, minor)`. A wheel
/// passes if any of its python tags targets a Python at or above the floor: a
/// version-agnostic tag (`py3`/`cp3`) covers its whole major line, an `abi3`
/// wheel is forward-compatible from its tagged minor onward, and an exact tag
/// (`cp39`) must itself be ≥ the floor. Unrecognized tags are kept.
fn wheel_reaches_python_floor(tags: &WheelTags, floor: (u32, u32)) -> bool {
    let (fmaj, fmin) = floor;
    let abi3 = tags.abi.iter().any(|a| a.eq_ignore_ascii_case("abi3"));
    tags.python.iter().any(|t| match interp_tag_version(t) {
        None => true,
        Some((major, None)) => major >= fmaj,
        Some((major, Some(minor))) => (abi3 && major >= fmaj) || (major, minor) >= (fmaj, fmin),
    })
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

    fn base_filter() -> ResolvedMirror {
        ResolvedMirror {
            include_packages: vec![],
            exclude_packages: vec![],
            include_format: vec![],
            include_python_tag: vec![],
            include_abi_tag: vec![],
            include_platform_tag: vec![],
            exclude_platform_tag: vec![],
            exclude_newer: None,
            exclude_older: None,
            exclude_python_below: None,
            exclude_dev: false,
            exclude_windows: false,
            exclude_prereleases: false,
            exclude_larger: None,
            exclude_yanked: false,
        }
    }

    fn named_file(filename: &str) -> SimpleFile {
        SimpleFile {
            filename: filename.into(),
            ..simple_file(None)
        }
    }

    fn time_filter(newer: Option<&str>, older: Option<&str>) -> ResolvedMirror {
        let parse = |v: Option<&str>| v.map(|s| OffsetDateTime::parse(s, &Rfc3339).unwrap());
        ResolvedMirror {
            exclude_newer: parse(newer),
            exclude_older: parse(older),
            ..base_filter()
        }
    }

    #[test]
    fn upload_time_bounds_filter() {
        let old = simple_file(Some("2015-10-07T13:41:23Z"));
        let new = simple_file(Some("2024-12-04T17:35:26Z"));
        let unknown = simple_file(None);

        let before_2016 = time_filter(Some("2016-01-01T00:00:00Z"), None);
        assert!(matches_mirror(&old, &before_2016));
        assert!(!matches_mirror(&new, &before_2016));
        assert!(!matches_mirror(&unknown, &before_2016));

        let since_2016 = time_filter(None, Some("2016-01-01T00:00:00Z"));
        assert!(!matches_mirror(&old, &since_2016));
        assert!(matches_mirror(&new, &since_2016));

        let unbounded = time_filter(None, None);
        assert!(matches_mirror(&unknown, &unbounded));
    }

    #[test]
    fn exclude_python_below_drops_old_wheels_keeps_modern_and_sdists() {
        let f = ResolvedMirror {
            exclude_python_below: Some((3, 10)),
            ..base_filter()
        };

        // Version-pinned wheels for Python ≤ 3.9 (and Python 2) are dropped.
        assert!(!matches_mirror(
            &named_file("foo-1.0-cp39-cp39-manylinux2014_x86_64.whl"),
            &f
        ));
        assert!(!matches_mirror(
            &named_file("foo-1.0-cp36-cp36m-win_amd64.whl"),
            &f
        ));
        assert!(!matches_mirror(
            &named_file("foo-1.0-py27-none-any.whl"),
            &f
        ));
        assert!(!matches_mirror(
            &named_file("foo-1.0-pp39-pypy39_pp73-linux_x86_64.whl"),
            &f
        ));

        // Modern pinned wheels pass — cp310 must not read as "cp3" + 1.
        assert!(matches_mirror(
            &named_file("foo-1.0-cp310-cp310-manylinux_2_17_x86_64.whl"),
            &f
        ));
        assert!(matches_mirror(
            &named_file("foo-1.0-cp313-cp313t-manylinux_2_17_x86_64.whl"),
            &f
        ));

        // Version-agnostic and abi3 (forward-compatible) wheels are kept.
        assert!(matches_mirror(&named_file("foo-1.0-py3-none-any.whl"), &f));
        assert!(matches_mirror(
            &named_file("six-1.16.0-py2.py3-none-any.whl"),
            &f
        ));
        assert!(matches_mirror(
            &named_file("foo-1.0-cp37-abi3-manylinux2014_x86_64.whl"),
            &f
        ));

        // sdists carry no python tag — the floor never touches them.
        assert!(matches_mirror(&named_file("foo-1.0.tar.gz"), &f));
    }

    #[test]
    fn exclude_dev_drops_dev_releases_only() {
        let f = ResolvedMirror {
            exclude_dev: true,
            ..base_filter()
        };
        assert!(!matches_mirror(
            &named_file("foo-1.0.dev1-py3-none-any.whl"),
            &f
        ));
        assert!(!matches_mirror(&named_file("foo-1.0.dev1.tar.gz"), &f));
        assert!(!matches_mirror(
            &named_file("foo-1.0.dev0-cp311-cp311-manylinux_2_17_x86_64.whl"),
            &f
        ));
        // Final, pre-release, and post-release versions are kept.
        assert!(matches_mirror(&named_file("foo-1.0-py3-none-any.whl"), &f));
        assert!(matches_mirror(
            &named_file("foo-1.0rc1-py3-none-any.whl"),
            &f
        ));
        assert!(matches_mirror(&named_file("foo-1.0.post1.tar.gz"), &f));
    }

    #[test]
    fn exclude_windows_drops_win_wheels_and_installers_only() {
        let f = ResolvedMirror {
            exclude_windows: true,
            ..base_filter()
        };
        // Windows wheels (any win* platform tag).
        assert!(!matches_mirror(
            &named_file("foo-1.0-cp311-cp311-win_amd64.whl"),
            &f
        ));
        assert!(!matches_mirror(
            &named_file("foo-1.0-cp311-cp311-win32.whl"),
            &f
        ));
        assert!(!matches_mirror(
            &named_file("foo-1.0-cp311-cp311-win_arm64.whl"),
            &f
        ));
        // Legacy Windows installers carry no wheel tag.
        assert!(!matches_mirror(&named_file("Foo-1.0.win32-py2.7.exe"), &f));
        assert!(!matches_mirror(&named_file("Foo-1.0.win-amd64.msi"), &f));
        assert!(!matches_mirror(
            &named_file("Foo-1.0-cp27-none-win_amd64.msi"),
            &f
        ));

        // Non-Windows wheels and sdists are kept — including packages whose name
        // merely starts with "win" (the raw-filename foot-gun).
        assert!(matches_mirror(
            &named_file("foo-1.0-cp311-cp311-manylinux_2_17_x86_64.whl"),
            &f
        ));
        assert!(matches_mirror(
            &named_file("foo-1.0-cp311-cp311-macosx_11_0_arm64.whl"),
            &f
        ));
        assert!(matches_mirror(&named_file("foo-1.0-py3-none-any.whl"), &f));
        assert!(matches_mirror(&named_file("windrose-1.9.2.tar.gz"), &f));
        assert!(matches_mirror(&named_file("winnow-0.1.0.tar.gz"), &f));
    }

    #[test]
    fn exclude_prereleases_drops_alpha_beta_rc_and_dev() {
        let f = ResolvedMirror {
            exclude_prereleases: true,
            ..base_filter()
        };
        // Every PEP 440 pre-release spelling is dropped — wheels and sdists.
        for name in [
            "foo-1.0a1-py3-none-any.whl",
            "foo-1.0b2-py3-none-any.whl",
            "foo-1.0rc1-py3-none-any.whl",
            "foo-2.0.dev1.tar.gz",
            "foo-2.0rc1.dev3-cp311-cp311-manylinux_2_17_x86_64.whl",
        ] {
            assert!(!matches_mirror(&named_file(name), &f), "{name} should drop");
        }
        // Final and post releases stay — unlike exclude_dev, rc is also dropped.
        assert!(matches_mirror(&named_file("foo-1.0-py3-none-any.whl"), &f));
        assert!(matches_mirror(&named_file("foo-1.0.post1.tar.gz"), &f));
        // An unparseable version can't be proven a pre-release, so it's kept.
        assert!(matches_mirror(&named_file("foo-notaversion.tar.gz"), &f));
    }

    #[test]
    fn exclude_larger_drops_oversize_keeps_unsized() {
        let f = ResolvedMirror {
            exclude_larger: Some(1000),
            ..base_filter()
        };
        let sized = |n: u64| SimpleFile {
            size: Some(n),
            ..named_file("foo-1.0-py3-none-any.whl")
        };
        assert!(!matches_mirror(&sized(1001), &f)); // over
        assert!(matches_mirror(&sized(1000), &f)); // exactly at the ceiling
        assert!(matches_mirror(&sized(10), &f)); // under
                                                 // No size in the listing → can't prove it's oversize → kept.
        assert!(matches_mirror(&named_file("foo-1.0-py3-none-any.whl"), &f));
    }

    #[test]
    fn exclude_yanked_drops_yanked_files() {
        let f = ResolvedMirror {
            exclude_yanked: true,
            ..base_filter()
        };
        let with_yank = |y: Yanked| SimpleFile {
            yanked: y,
            ..named_file("foo-1.0-py3-none-any.whl")
        };
        assert!(!matches_mirror(&with_yank(Yanked::Flag(true)), &f));
        assert!(!matches_mirror(
            &with_yank(Yanked::Reason("CVE-2024-1".into())),
            &f
        ));
        assert!(matches_mirror(&with_yank(Yanked::Flag(false)), &f));
    }

    #[test]
    fn yanked_is_excluded_by_default_unless_opted_in() {
        let exclude =
            |f: MirrorArgs, file: Option<&MirrorConfig>| f.resolve(file).unwrap().exclude_yanked;
        // Nothing set: yanked is dropped by default.
        assert!(exclude(MirrorArgs::default(), None));
        // CLI/env opt-out (--include-yanked) mirrors them again.
        assert!(!exclude(
            MirrorArgs {
                include_yanked: true,
                ..Default::default()
            },
            None
        ));
        // File opt-out (`[mirror].include-yanked = true`).
        let on = MirrorConfig {
            include_yanked: Some(true),
            ..Default::default()
        };
        assert!(!exclude(MirrorArgs::default(), Some(&on)));
        // An explicit `include-yanked = false` keeps the default exclusion.
        let off = MirrorConfig {
            include_yanked: Some(false),
            ..Default::default()
        };
        assert!(exclude(MirrorArgs::default(), Some(&off)));
    }

    #[test]
    fn is_prerelease_version_spans_pre_and_dev() {
        assert!(is_prerelease_version("1.0a1"));
        assert!(is_prerelease_version("1.0b2"));
        assert!(is_prerelease_version("1.0rc1"));
        assert!(is_prerelease_version("1.0.dev1"));
        assert!(is_prerelease_version("2.0rc1.dev3"));
        assert!(!is_prerelease_version("1.0"));
        assert!(!is_prerelease_version("1.0.post1"));
        // Unparseable → not a pre-release (kept).
        assert!(!is_prerelease_version("notaversion"));
    }

    #[test]
    fn parse_size_parses_units_and_rejects_garbage() {
        let ok = |s: &str| parse_size("exclude-larger", Some(s)).unwrap();
        assert_eq!(ok(""), None);
        assert_eq!(ok("   "), None);
        assert_eq!(ok("1048576"), Some(1_048_576));
        assert_eq!(ok("500"), Some(500));
        assert_eq!(ok("1KB"), Some(1024));
        assert_eq!(ok("1kib"), Some(1024));
        assert_eq!(ok("250MB"), Some(250 << 20));
        assert_eq!(ok("1.5GiB"), Some(1_610_612_736));
        assert_eq!(ok(" 2 gb "), Some(2 << 30));
        assert!(parse_size("exclude-larger", Some("big")).is_err());
        assert!(parse_size("exclude-larger", Some("10XB")).is_err());
        assert!(parse_size("exclude-larger", Some("-5MB")).is_err());
    }

    #[test]
    fn interp_tag_version_parses_real_tags() {
        assert_eq!(interp_tag_version("cp39"), Some((3, Some(9))));
        assert_eq!(interp_tag_version("cp310"), Some((3, Some(10))));
        assert_eq!(interp_tag_version("cp313t"), Some((3, Some(13))));
        assert_eq!(interp_tag_version("py3"), Some((3, None)));
        assert_eq!(interp_tag_version("py27"), Some((2, Some(7))));
        assert_eq!(interp_tag_version("pp39"), Some((3, Some(9))));
        assert_eq!(interp_tag_version("none"), None);
        assert_eq!(interp_tag_version("any"), None);
    }

    #[test]
    fn is_dev_version_detects_dev_spellings() {
        assert!(is_dev_version("1.0.dev1"));
        assert!(is_dev_version("1.0dev0")); // canonicalizes to 1.0.dev0
        assert!(is_dev_version("2.0a1.dev3"));
        assert!(!is_dev_version("1.0"));
        assert!(!is_dev_version("1.0rc1"));
        assert!(!is_dev_version("1.0.post1"));
    }

    #[test]
    fn parse_exclude_python_below_parses_versions() {
        assert_eq!(parse_exclude_python_below(None).unwrap(), None);
        assert_eq!(parse_exclude_python_below(Some("  ")).unwrap(), None);
        assert_eq!(
            parse_exclude_python_below(Some("3.10")).unwrap(),
            Some((3, 10))
        );
        assert_eq!(parse_exclude_python_below(Some("3")).unwrap(), Some((3, 0)));
        assert_eq!(
            parse_exclude_python_below(Some("3.10.2")).unwrap(),
            Some((3, 10))
        );
        assert!(parse_exclude_python_below(Some("three.ten")).is_err());
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
    fn parse_cutoff_accepts_bare_date_and_day_count() {
        // A bare calendar date is that day at 00:00:00 UTC.
        assert_eq!(
            cutoff("2008-12-03").unwrap().unwrap(),
            OffsetDateTime::parse("2008-12-03T00:00:00Z", &Rfc3339).unwrap()
        );
        // A bare integer is that many days ago (so `--exclude-newer 7` works).
        for (input, days) in [("7", 7), ("30", 30), ("0", 0)] {
            let before = OffsetDateTime::now_utc();
            let got = cutoff(input).unwrap().unwrap();
            let after = OffsetDateTime::now_utc();
            let slack = Duration::seconds(5);
            assert!(
                got >= before - Duration::days(days) - slack
                    && got <= after - Duration::days(days) + slack,
                "{input} resolved to {got}, expected ~{days}d ago"
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
        // Not durations, dates, day-counts, or timestamps at all.
        for bad in [
            "tomorrow",
            "2020-13-01", // month 13 isn't a date
            "-5",         // a negative day count is nonsense
            "PT",
            "P",
            "1.5 hours",
            "garbage",
        ] {
            assert!(cutoff(bad).is_err(), "{bad} must be rejected");
        }
    }

    fn resolved_with(filter: ResolvedMirror, src_base: &str) -> Resolved {
        Resolved {
            src_base: src_base.to_string(),
            dst_base: "https://dest.example".to_string(),
            admin_user: None,
            admin_pass: None,
            private_prefix: None,
            concurrency: 1,
            package_concurrency: 1,
            spool_dir: std::env::temp_dir(),
            dry_run: false,
            full: false,
            mirror: filter,
            exclude_older_raw: None,
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
        f1.include_python_tag = vec!["cp311".into(), "cp310".into()];
        let mut f2 = time_filter(None, None);
        f2.include_python_tag = vec!["cp310".into(), "cp311".into()];
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
        wheels.include_format = vec![Format::Wheel];
        assert_ne!(
            k,
            config_key(&resolved_with(wheels, "https://pypi.org"), &s)
        );
        assert_ne!(k, config_key(&r, &spec("requests", Some(">=2"))));

        // Each new filter axis invalidates the cursor key too.
        let with = |mutate: fn(&mut ResolvedMirror)| {
            let mut f = time_filter(None, None);
            mutate(&mut f);
            config_key(&resolved_with(f, "https://pypi.org"), &s)
        };
        assert_ne!(k, with(|f| f.exclude_prereleases = true));
        assert_ne!(k, with(|f| f.exclude_yanked = true));
        assert_ne!(k, with(|f| f.exclude_larger = Some(1 << 20)));
        assert_ne!(k, with(|f| f.include_format = vec![Format::Sdist]));
        assert_ne!(
            k,
            with(|f| f.exclude_packages = vec![spec("requests", Some("<2"))])
        );
    }

    #[test]
    fn config_key_older_bound_is_stable_across_relative_runs() {
        let s = spec("requests", None);

        // Two runs of the same `--exclude-older "800 days"` resolve to two
        // different instants (now slides), but the raw input is identical. The
        // key must stay stable, or the sync cursor never matches its own prior
        // config and every relative-duration run needlessly re-fetches.
        let mut run1 = resolved_with(
            time_filter(None, Some("2024-01-01T00:00:00Z")),
            "https://pypi.org",
        );
        let mut run2 = resolved_with(
            time_filter(None, Some("2020-06-15T00:00:00Z")),
            "https://pypi.org",
        );
        run1.exclude_older_raw = Some("800 days".into());
        run2.exclude_older_raw = Some("800 days".into());
        assert_eq!(config_key(&run1, &s), config_key(&run2, &s));

        // A genuinely different older bound still invalidates the key.
        run2.exclude_older_raw = Some("400 days".into());
        assert_ne!(config_key(&run1, &s), config_key(&run2, &s));

        // The newer bound is hashed by its resolved instant, so a sliding
        // relative `--exclude-newer` keeps invalidating the key every run — by
        // design: releases aging past it become eligible and a 304 would miss them.
        let newer_a = resolved_with(
            time_filter(Some("2024-01-01T00:00:00Z"), None),
            "https://pypi.org",
        );
        let newer_b = resolved_with(
            time_filter(Some("2020-06-15T00:00:00Z"), None),
            "https://pypi.org",
        );
        assert_ne!(config_key(&newer_a, &s), config_key(&newer_b, &s));
    }
}
