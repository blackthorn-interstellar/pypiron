//! `pypiron.toml`: file-based configuration, layered under CLI/env.
//!
//! Precedence is CLI/env (clap merges those) > file > built-in default.
//! Unknown keys are hard errors — a typo'd filter that silently no-ops is
//! how you mirror the wrong thing.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

pub const DEFAULT_CONFIG_PATH: &str = "pypiron.toml";

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ConfigFile {
    #[serde(default)]
    pub sync: SyncConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SyncConfig {
    /// Inline package list; same line syntax as packages.txt
    /// (name plus optional PEP 440 specifiers, e.g. "requests>=2.20,<3").
    pub packages: Option<Vec<String>>,
    pub packages_list: Option<PathBuf>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub private_prefix: Option<String>,
    pub concurrency: Option<usize>,
    pub package_concurrency: Option<usize>,
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
    // Always announce a loaded config — silent auto-discovery of ./pypiron.toml
    // is how an unrelated CLI invocation gets quietly rewired.
    info!("loaded configuration from {}", path.display());
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config {}", path.display()))?;
    let mut cfg: ConfigFile =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;

    if let Some(rel) = cfg.sync.packages_list.as_ref().filter(|p| p.is_relative()) {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            cfg.sync.packages_list = Some(dir.join(rel));
        }
    }
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sync_section() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            [sync]
            packages = ["requests>=2.20,<3", "six"]
            to = "http://localhost:8080"
            only-wheels = true
            python-tag = ["py3"]
            exclude-newer = "2026-01-01T00:00:00Z"
            concurrency = 8
            package-concurrency = 16
            "#,
        )
        .unwrap();
        assert_eq!(cfg.sync.packages.unwrap().len(), 2);
        assert_eq!(cfg.sync.only_wheels, Some(true));
        assert_eq!(cfg.sync.concurrency, Some(8));
        assert_eq!(cfg.sync.package_concurrency, Some(16));
        assert_eq!(
            cfg.sync.exclude_newer.as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let err = toml::from_str::<ConfigFile>("[sync]\nonly-weels = true\n").unwrap_err();
        assert!(err.to_string().contains("only-weels"));
    }

    #[test]
    fn empty_config_is_fine() {
        let cfg: ConfigFile = toml::from_str("").unwrap();
        assert!(cfg.sync.packages.is_none());
    }
}
