//! `pypiron.toml`: file-based configuration, layered under CLI/env.
//!
//! Four pieces, one per concern:
//!   - top-level `private-prefix` — the reserved private namespace, shared by
//!     `sync` and the `serve` proxy (one knob, one place).
//!   - `[filter]` — the slice of PyPI you want, names included. Shared by
//!     `sync` (push mirror) and `serve --proxy-upstream` (on-demand pull
//!     mirror): set it once, it governs whichever you run.
//!   - `[serve]` — the server process (non-secret knobs only; credentials and
//!     cloud keys stay in CLI/env — see docs/concepts/authentication.md).
//!   - `[sync]` — the push-mirror job (source/dest + concurrency).
//!
//! Precedence is CLI/env (clap merges those) > file > built-in default.
//! Unknown keys are hard errors — a typo'd filter that silently no-ops is
//! how you mirror the wrong thing.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tracing::info;

pub const DEFAULT_CONFIG_PATH: &str = "pypiron.toml";

/// Filter keys that used to live under `[sync]` and now belong under `[filter]`.
/// Kept only to turn the `deny_unknown_fields` rejection into a migration hint.
const MOVED_FILTER_KEYS: &[&str] = &[
    "packages",
    "packages-list",
    "only-wheels",
    "only-sdists",
    "python-tag",
    "abi-tag",
    "platform-tag",
    "exclude-platform-tag",
    "exclude-newer",
    "exclude-older",
    "min-python",
    "exclude-dev",
    "exclude-windows",
];

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ConfigFile {
    /// Reserved private namespace (PEP 503-normalized). Shared by `sync` and the
    /// `serve` proxy — the dependency-confusion control belongs in one place.
    pub private_prefix: Option<String>,
    #[serde(default)]
    pub filter: FilterFile,
    #[serde(default)]
    pub serve: ServeConfig,
    #[serde(default)]
    pub sync: SyncConfig,
}

/// `[filter]`: the slice of PyPI to mirror/proxy. Same fields as the CLI
/// `--filter-*` flags; consumed by both `sync` and `serve --proxy-upstream`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct FilterFile {
    /// Inline package scope; each entry is a name with optional PEP 440
    /// specifiers (e.g. "requests>=2.20,<3"), same syntax as a packages.txt
    /// line. The slice's name axis: `sync` mirrors exactly these, and the
    /// `serve` proxy serves only these from upstream (fail-closed when set).
    pub packages: Option<Vec<String>>,
    /// File of package specs, one per line; same syntax as `packages`.
    pub packages_list: Option<PathBuf>,
    pub only_wheels: Option<bool>,
    pub only_sdists: Option<bool>,
    pub python_tag: Option<Vec<String>>,
    pub abi_tag: Option<Vec<String>>,
    pub platform_tag: Option<Vec<String>>,
    pub exclude_platform_tag: Option<Vec<String>>,
    pub exclude_newer: Option<String>,
    pub exclude_older: Option<String>,
    pub min_python: Option<String>,
    pub exclude_dev: Option<bool>,
    pub exclude_windows: Option<bool>,
    pub exclude_prereleases: Option<bool>,
    pub max_size: Option<String>,
    /// Yanked files (PEP 592) are excluded by default; set `true` to mirror them.
    pub include_yanked: Option<bool>,
}

/// `[serve]`: the server process. Non-secret knobs only — admin/uploader/read
/// passwords and the Azure access key stay in CLI/env. Mirrors the `serve` CLI
/// flags one-to-one (including storage selection).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ServeConfig {
    pub bind_addr: Option<String>,
    pub artifact_delivery: Option<String>,
    pub access_log: Option<bool>,
    pub access_log_format: Option<String>,
    pub proxy_upstream: Option<String>,
    pub spool_dir: Option<PathBuf>,
    pub wait_on_upload: Option<bool>,
    pub wait_on_upload_secs: Option<u64>,
    pub worker_interval_secs: Option<u64>,
    pub intent_grace_secs: Option<u64>,
    pub audit_on_boot: Option<bool>,
    pub reconcile_interval_secs: Option<u64>,
    pub lease_ttl_secs: Option<u64>,
    pub download_stats: Option<bool>,
    pub counters_resolution: Option<String>,
    pub counters_flush_interval_secs: Option<u64>,
    pub counters_rollup_interval_secs: Option<u64>,
    pub counters_retention_days: Option<i64>,
    // Storage backend selection (the same `--storage`/`--s3-*`/... knobs).
    pub storage: Option<String>,
    pub data_dir: Option<String>,
    pub s3_bucket: Option<String>,
    pub aws_region: Option<String>,
    pub s3_endpoint_url: Option<String>,
    pub s3_force_path_style: Option<bool>,
    pub gcs_bucket: Option<String>,
    pub gcs_service_account_path: Option<String>,
    pub gcs_endpoint_url: Option<String>,
    pub azure_account: Option<String>,
    pub azure_container: Option<String>,
    pub azure_endpoint_url: Option<String>,
    pub azure_use_emulator: Option<bool>,
}

/// `[sync]`: the push-mirror job. The package scope and filters moved to
/// `[filter]`; `private-prefix` moved to the top level.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SyncConfig {
    pub from: Option<String>,
    pub to: Option<String>,
    pub admin_user: Option<String>,
    pub admin_pass: Option<String>,
    pub concurrency: Option<usize>,
    pub package_concurrency: Option<usize>,
}

/// Load configuration. An explicit `--config` path must exist; without one,
/// `./pypiron.toml` is used when present and silently skipped when not.
/// Relative `packages-list` paths inside the file resolve against the config
/// file's own directory, not the process cwd.
pub fn load(explicit: Option<&Path>) -> Result<ConfigFile> {
    let path = match explicit {
        Some(p) => p.to_path_buf(),
        None => {
            let default = Path::new(DEFAULT_CONFIG_PATH);
            if !default.exists() {
                return Ok(ConfigFile::default());
            }
            default.to_path_buf()
        }
    };
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config {}", path.display()))?;

    let mut cfg: ConfigFile = match toml::from_str(&text) {
        Ok(cfg) => cfg,
        Err(e) => {
            // Turn the strict-field rejection of a pre-split config into a
            // migration hint instead of a bare "unknown field" error.
            migration_hint(&text)?;
            return Err(e).with_context(|| format!("parsing config {}", path.display()));
        }
    };

    if let Some(rel) = cfg
        .filter
        .packages_list
        .as_ref()
        .filter(|p| p.is_relative())
    {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            cfg.filter.packages_list = Some(dir.join(rel));
        }
    }
    // Announce only after a clean parse — silent auto-discovery of
    // ./pypiron.toml is how an unrelated CLI invocation gets quietly rewired,
    // but a malformed file shouldn't claim it "loaded". The read/parse errors
    // above carry the path via `with_context`, so failures still name the file.
    info!("loaded configuration from {}", path.display());
    Ok(cfg)
}

/// If `[sync]` still carries keys that moved out from under it, bail with a
/// pointed message naming the new home. Returns `Ok(())` when nothing matches,
/// letting the caller surface the original parse error.
fn migration_hint(text: &str) -> Result<()> {
    let Ok(value) = toml::from_str::<toml::Value>(text) else {
        return Ok(());
    };
    let Some(sync) = value.get("sync").and_then(|v| v.as_table()) else {
        return Ok(());
    };
    for key in MOVED_FILTER_KEYS {
        if sync.contains_key(*key) {
            bail!(
                "[sync].{key} was moved to [filter].{key} — the filter is now shared by sync and \
                 the serve proxy (see docs/reference/configuration.md)"
            );
        }
    }
    if sync.contains_key("private-prefix") {
        bail!(
            "[sync].private-prefix was moved to the top-level `private-prefix` key (see \
             docs/reference/configuration.md)"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sync_and_filter_sections() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            private-prefix = "acme"

            [sync]
            to = "http://localhost:8080"
            concurrency = 8
            package-concurrency = 16

            [filter]
            packages = ["requests>=2.20,<3", "six"]
            only-wheels = true
            python-tag = ["py3"]
            exclude-newer = "2026-01-01T00:00:00Z"
            exclude-prereleases = true
            max-size = "250MB"
            include-yanked = true
            "#,
        )
        .unwrap();
        assert_eq!(cfg.private_prefix.as_deref(), Some("acme"));
        assert_eq!(cfg.filter.packages.unwrap().len(), 2);
        assert_eq!(cfg.sync.concurrency, Some(8));
        assert_eq!(cfg.sync.package_concurrency, Some(16));
        assert_eq!(cfg.filter.only_wheels, Some(true));
        assert_eq!(
            cfg.filter.exclude_newer.as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
        assert_eq!(cfg.filter.exclude_prereleases, Some(true));
        assert_eq!(cfg.filter.max_size.as_deref(), Some("250MB"));
        assert_eq!(cfg.filter.include_yanked, Some(true));
    }

    #[test]
    fn parses_serve_section() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            [serve]
            bind-addr = "127.0.0.1:9000"
            proxy-upstream = "https://pypi.org"
            storage = "s3"
            s3-bucket = "acme-mirror"
            reconcile-interval-secs = 3600
            "#,
        )
        .unwrap();
        assert_eq!(cfg.serve.bind_addr.as_deref(), Some("127.0.0.1:9000"));
        assert_eq!(
            cfg.serve.proxy_upstream.as_deref(),
            Some("https://pypi.org")
        );
        assert_eq!(cfg.serve.storage.as_deref(), Some("s3"));
        assert_eq!(cfg.serve.s3_bucket.as_deref(), Some("acme-mirror"));
        assert_eq!(cfg.serve.reconcile_interval_secs, Some(3600));
    }

    #[test]
    fn unknown_filter_key_is_rejected() {
        let err = toml::from_str::<ConfigFile>("[filter]\nonly-weels = true\n").unwrap_err();
        assert!(err.to_string().contains("only-weels"));
    }

    #[test]
    fn moved_filter_key_under_sync_gives_migration_hint() {
        let err = migration_hint("[sync]\nonly-wheels = true\n").unwrap_err();
        assert!(
            err.to_string().contains("moved to [filter].only-wheels"),
            "got: {err}"
        );
    }

    #[test]
    fn moved_packages_under_sync_gives_migration_hint() {
        let err = migration_hint("[sync]\npackages = [\"requests\"]\n").unwrap_err();
        assert!(
            err.to_string().contains("moved to [filter].packages"),
            "got: {err}"
        );
    }

    #[test]
    fn moved_private_prefix_gives_migration_hint() {
        let err = migration_hint("[sync]\nprivate-prefix = \"acme\"\n").unwrap_err();
        assert!(
            err.to_string().contains("top-level `private-prefix`"),
            "got: {err}"
        );
    }

    #[test]
    fn empty_config_is_fine() {
        let cfg: ConfigFile = toml::from_str("").unwrap();
        assert!(cfg.filter.packages.is_none());
        assert!(cfg.filter.only_wheels.is_none());
        assert!(cfg.serve.bind_addr.is_none());
        assert!(cfg.private_prefix.is_none());
    }
}
