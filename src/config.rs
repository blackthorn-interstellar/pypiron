//! `pypiron.toml`: file-based configuration, layered under CLI/env.
//!
//! Four pieces, one per concern:
//!   - top-level `private-prefix` — the reserved private namespace, shared by
//!     `sync` and the `serve` proxy (one knob, one place).
//!   - `[mirror]` — the slice of PyPI you want, names included. Shared by
//!     `sync` (push mirror) and `serve --proxy-upstream` (on-demand pull
//!     mirror): set it once, it governs whichever you run.
//!   - `[serve]` — the server process (non-secret knobs only; credentials and
//!     cloud keys stay in CLI/env — see docs/concepts/authentication.md).
//!   - `[sync]` — the push-mirror job (source/dest + concurrency).
//!
//! Precedence is CLI/env (clap merges those) > file > built-in default.
//! Unknown keys are hard errors — a typo'd mirror rule that silently no-ops is
//! how you mirror the wrong thing.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

pub const DEFAULT_CONFIG_PATH: &str = "pypiron.toml";

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ConfigFile {
    /// Reserved private namespace (PEP 503-normalized). Shared by `sync` and the
    /// `serve` proxy — the dependency-confusion control belongs in one place.
    pub private_prefix: Option<String>,
    #[serde(default)]
    pub mirror: MirrorConfig,
    #[serde(default)]
    pub serve: ServeConfig,
    #[serde(default)]
    pub sync: SyncConfig,
}

/// `[mirror]`: the slice of PyPI to mirror/proxy. Same fields as the shared
/// mirror CLI flags; consumed by both `sync` and `serve --proxy-upstream`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct MirrorConfig {
    /// Inline package scope; each entry is a name with optional PEP 440
    /// specifiers (e.g. "requests>=2.20,<3"), same syntax as a packages.txt
    /// line. The slice's name axis: `sync` mirrors exactly these, and the
    /// `serve` proxy serves only these from upstream (fail-closed when set).
    pub include_packages: Option<Vec<String>>,
    /// File of package specs, one per line; same syntax as `include-packages`.
    pub include_packages_from: Option<PathBuf>,
    /// Package specs to subtract from the include set. Bare names deny the
    /// whole project; version specifiers deny only matching files.
    pub exclude_packages: Option<Vec<String>>,
    /// File of package deny specs, one per line.
    pub exclude_packages_from: Option<PathBuf>,
    /// Artifact formats to keep: wheel, sdist, other. Unset means all formats.
    pub include_format: Option<Vec<String>>,
    pub include_python_tag: Option<Vec<String>>,
    pub include_abi_tag: Option<Vec<String>>,
    pub include_platform_tag: Option<Vec<String>>,
    pub exclude_platform_tag: Option<Vec<String>>,
    pub exclude_newer: Option<String>,
    pub exclude_older: Option<String>,
    pub exclude_python_below: Option<String>,
    pub exclude_dev: Option<bool>,
    pub exclude_windows: Option<bool>,
    pub exclude_prereleases: Option<bool>,
    pub exclude_larger: Option<String>,
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

/// `[sync]`: the push-mirror job. The package scope and mirror rules live in
/// `[mirror]`; `private-prefix` lives at the top level.
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
/// Relative `include-packages-from` and `exclude-packages-from` paths inside the
/// file resolve against the config file's own directory, not the process cwd.
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

    let mut cfg: ConfigFile =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;

    rebase_relative(&mut cfg.mirror.include_packages_from, &path);
    rebase_relative(&mut cfg.mirror.exclude_packages_from, &path);
    // Announce only after a clean parse — silent auto-discovery of
    // ./pypiron.toml is how an unrelated CLI invocation gets quietly rewired,
    // but a malformed file shouldn't claim it "loaded". The read/parse errors
    // above carry the path via `with_context`, so failures still name the file.
    info!("loaded configuration from {}", path.display());
    Ok(cfg)
}

fn rebase_relative(path: &mut Option<PathBuf>, config_path: &Path) {
    let Some(rel) = path.as_ref().filter(|p| p.is_relative()) else {
        return;
    };
    if let Some(dir) = config_path.parent().filter(|d| !d.as_os_str().is_empty()) {
        *path = Some(dir.join(rel));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sync_and_mirror_sections() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            private-prefix = "acme"

            [sync]
            to = "http://localhost:8080"
            concurrency = 8
            package-concurrency = 16

            [mirror]
            include-packages = ["requests>=2.20,<3", "six"]
            exclude-packages = ["six==1.15.0"]
            include-format = ["wheel"]
            include-python-tag = ["py3"]
            exclude-newer = "2026-01-01T00:00:00Z"
            exclude-prereleases = true
            exclude-larger = "250MB"
            include-yanked = true
            "#,
        )
        .unwrap();
        assert_eq!(cfg.private_prefix.as_deref(), Some("acme"));
        assert_eq!(cfg.mirror.include_packages.unwrap().len(), 2);
        assert_eq!(cfg.mirror.exclude_packages.unwrap().len(), 1);
        assert_eq!(cfg.sync.concurrency, Some(8));
        assert_eq!(cfg.sync.package_concurrency, Some(16));
        assert_eq!(cfg.mirror.include_format.unwrap(), ["wheel"]);
        assert_eq!(
            cfg.mirror.exclude_newer.as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
        assert_eq!(cfg.mirror.exclude_prereleases, Some(true));
        assert_eq!(cfg.mirror.exclude_larger.as_deref(), Some("250MB"));
        assert_eq!(cfg.mirror.include_yanked, Some(true));
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
    fn unknown_mirror_key_is_rejected() {
        let err =
            toml::from_str::<ConfigFile>("[mirror]\ninclude-formatt = [\"wheel\"]\n").unwrap_err();
        assert!(err.to_string().contains("include-formatt"));
    }

    #[test]
    fn empty_config_is_fine() {
        let cfg: ConfigFile = toml::from_str("").unwrap();
        assert!(cfg.mirror.include_packages.is_none());
        assert!(cfg.mirror.include_format.is_none());
        assert!(cfg.serve.bind_addr.is_none());
        assert!(cfg.private_prefix.is_none());
    }
}
