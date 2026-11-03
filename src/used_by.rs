//! "Used by": the hosted projects whose newest release requires a project.
//!
//! Two derived views under `_state/`, regenerable from the stored `.metadata`
//! companions:
//!
//! - `requires-<shard>.json`: per package, the companion its newest release was
//!   read from and that release's requirements, grouped by the project each
//!   names. Sharded by first character, like the audit fingerprints.
//! - `used-by/<pkg>.json`: the inverse, one file per required project: each
//!   package requiring it and its requirement lines. The project page reads it.
//!
//! The tick refreshes the packages it rebuilt, and the audit everything else. A
//! companion is read only when a package's newest release moved, and only the
//! used-by files whose entry for that package changed are written, so a new
//! release with the same requirements writes nothing. Many packages naming one
//! project in the same pass rewrite its file once.

use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use futures::{stream, StreamExt};
use pep440_rs::Version;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::app::{AppState, PACKAGES_PREFIX};
use crate::deps::parse_requirement;
use crate::names::infer_version_from_filename;
use crate::sidecar::{
    is_artifact, metadata_key, FROZEN_SUFFIX, METADATA_SUFFIX, MIRROR_QUARANTINED_SUFFIX,
    TOMBSTONE_SUFFIX,
};
use crate::storage::{is_not_found, Storage, SHARD_CHARS};
use crate::worker::STATE_PREFIX;

/// Companion reads and used-by writes in flight at once.
const CONCURRENCY: usize = 16;

/// Requirement lines grouped by the project they name (forward), or by the
/// package declaring them (a used-by file).
pub(crate) type Lines = BTreeMap<String, Vec<String>>;

/// One package's newest release, as read.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct Forward {
    /// The artifact whose `.metadata` companion was read.
    source: String,
    requires: Lines,
}

type Shard = BTreeMap<String, Forward>;

/// A package and its new lines for one project (`None` = no longer required).
type Edit<'a> = (&'a str, Option<&'a Vec<String>>);

/// How much of the corpus a refresh saw.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Scope {
    /// Only the packages listed (the tick's rebuilds).
    Some,
    /// Every package: one missing from the list is gone (an audit).
    All,
    /// Every package, re-read from scratch (`rebuild-index`).
    Regenerate,
}

fn requires_key(shard: char) -> String {
    format!("{STATE_PREFIX}requires-{shard}.json")
}

fn used_by_key(pkg: &str) -> String {
    format!("{STATE_PREFIX}used-by/{pkg}.json")
}

/// The artifact standing for a package's newest release: the highest version
/// among `files` (each with a `.metadata` companion), finals before
/// pre-releases. Filename versions only, so the tick and the audit, which see
/// different inputs, pick alike.
pub(crate) fn newest<'a>(files: impl IntoIterator<Item = &'a str>) -> Option<String> {
    files
        .into_iter()
        .filter_map(|f| Some((infer_version_from_filename(f)?.parse::<Version>().ok()?, f)))
        .max_by(|(a, fa), (b, fb)| (!a.any_prerelease(), a, fa).cmp(&(!b.any_prerelease(), b, fb)))
        .map(|(_, f)| f.to_string())
}

/// [`newest`] from a listing of a package's directory: artifacts with a
/// companion that no delete, freeze, quarantine or denylist entry hides.
pub(crate) fn newest_listed(
    state: &AppState,
    pkg: &str,
    members: &HashSet<&str>,
) -> Option<String> {
    let has = |name: &str, suffix: &str| members.contains(format!("{name}{suffix}").as_str());
    newest(members.iter().copied().filter(|name| {
        is_artifact(name)
            && has(name, METADATA_SUFFIX)
            && ![TOMBSTONE_SUFFIX, FROZEN_SUFFIX, MIRROR_QUARANTINED_SUFFIX]
                .iter()
                .any(|suffix| has(name, suffix))
            && !state
                .denylist
                .as_ref()
                .is_some_and(|d| d.file_denied(pkg, name))
    }))
}

/// A release's requirements by the project each names; self-references and
/// direct URL references dropped.
fn requires(pkg: &str, metadata: &[u8]) -> Lines {
    let mut out = Lines::new();
    for line in crate::coremeta::parse(metadata).requires_dist {
        if let Some(req) = parse_requirement(&line) {
            if req.name != pkg {
                out.entry(req.name)
                    .or_default()
                    .push(line.trim().to_string());
            }
        }
    }
    out
}

/// The used-by entries whose requirements admit `version`; all of them when
/// it isn't PEP 440.
pub(crate) fn admitting(used_by: Lines, version: &str) -> Lines {
    let Ok(version) = version.parse::<Version>() else {
        return used_by;
    };
    used_by
        .into_iter()
        .filter(|(_, lines)| {
            lines.iter().any(|line| {
                parse_requirement(line)
                    .and_then(|req| req.specifiers)
                    .is_none_or(|s| s.contains(&version))
            })
        })
        .collect()
}

/// The projects requiring `pkg`, for its page. Empty when none do or the file
/// can't be read: the list is a convenience, never worth failing a page.
pub(crate) async fn read(storage: &dyn Storage, pkg: &str) -> Lines {
    read_json(storage, &used_by_key(pkg))
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// A derived JSON file, `None` when absent or unreadable as JSON (it is
/// regenerated); `Err` only for a storage failure.
async fn read_json<T: serde::de::DeserializeOwned>(
    storage: &dyn Storage,
    key: &str,
) -> Result<Option<T>> {
    match storage.get_bytes(key).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The used-by entries `pkg` changes moving from `old` to `new`: each project
/// either names, with `pkg`'s new lines for it (`None` = no longer required).
fn edits<'a>(old: Option<&'a Forward>, new: Option<&'a Forward>) -> Vec<Edit<'a>> {
    let old = old.map(|f| &f.requires);
    let new = new.map(|f| &f.requires);
    let mut names: Vec<&str> = old
        .into_iter()
        .chain(new)
        .flat_map(|r| r.keys().map(String::as_str))
        .collect();
    names.sort_unstable();
    names.dedup();
    names
        .into_iter()
        .filter_map(|name| {
            let after = new.and_then(|r| r.get(name));
            (old.and_then(|r| r.get(name)) != after).then_some((name, after))
        })
        .collect()
}

/// Serializes refreshes on this node: the tick and the audit both
/// read-modify-write the same files.
static REFRESH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Bring the used-by views up to date with `observed`: each package and the
/// artifact standing for its newest release ([`newest`]; `None` = nothing to
/// read). Leader-only, like every other derived write.
///
/// Crash-safe by ordering: a package's forward record is written only after
/// every used-by file it touched, so an interrupted pass leaves the record
/// stale and the next refresh redoes it.
pub(crate) async fn refresh(
    state: &AppState,
    storage: &dyn Storage,
    observed: Vec<(String, Option<String>)>,
    scope: Scope,
) -> Result<()> {
    let _serial = REFRESH.lock().await;
    let mut shards: BTreeMap<char, Shard> = BTreeMap::new();
    let mut seen: BTreeMap<char, HashSet<String>> = BTreeMap::new();
    if scope != Scope::Some {
        for &c in SHARD_CHARS {
            seen.entry(c).or_default();
        }
    }
    for (pkg, _) in &observed {
        if let Some(c) = pkg.chars().next() {
            seen.entry(c).or_default().insert(pkg.clone());
        }
    }
    for &c in seen.keys() {
        let stored = match scope {
            Scope::Regenerate => None,
            _ => read_json(storage, &requires_key(c)).await?,
        };
        shards.insert(c, stored.unwrap_or_default());
    }
    let stored = |pkg: &str| {
        pkg.chars()
            .next()
            .and_then(|c| shards.get(&c))
            .and_then(|s| s.get(pkg))
    };

    // What moved: packages whose newest release is no longer the one read, and
    // under a full scope, packages gone from the corpus.
    let mut gone: Vec<String> = Vec::new();
    let mut to_read: Vec<(String, String)> = Vec::new();
    for (pkg, source) in observed {
        let current = stored(&pkg).map(|f| &f.source);
        match source {
            Some(source) if current != Some(&source) => {
                // An audit observation can predate a tick that already moved
                // this record on; re-list before trusting it over the record.
                if scope == Scope::All && current.is_some() {
                    let fresh = relisted(state, storage, &pkg).await?;
                    if fresh.as_ref() == current {
                        continue;
                    }
                    match fresh {
                        Some(fresh) => to_read.push((pkg, fresh)),
                        None => gone.push(pkg),
                    }
                } else {
                    to_read.push((pkg, source));
                }
            }
            None if current.is_some() => gone.push(pkg),
            _ => {}
        }
    }
    if scope == Scope::All {
        for (c, shard) in &shards {
            let seen = seen.get(c);
            gone.extend(
                shard
                    .keys()
                    .filter(|pkg| !seen.is_some_and(|s| s.contains(*pkg)))
                    .cloned(),
            );
        }
    }
    let read: Vec<(String, Option<Forward>)> = stream::iter(to_read)
        .map(|(pkg, source)| async move {
            let key = metadata_key(&format!("{PACKAGES_PREFIX}{pkg}/{source}"));
            let requires = match storage.get_bytes(&key).await {
                Ok(bytes) => requires(&pkg, &bytes),
                // Deleted since the listing; the next one picks another.
                Err(e) if is_not_found(&e) => Lines::new(),
                Err(e) => {
                    warn!(package=%pkg, error=?e, "used-by: metadata read failed; retried next pass");
                    return (pkg, None);
                }
            };
            (pkg, Some(Forward { source, requires }))
        })
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;
    let changed: Vec<(String, Option<Forward>)> = read
        .into_iter()
        .filter(|(_, forward)| forward.is_some())
        .chain(gone.into_iter().map(|pkg| (pkg, None)))
        .collect();

    // The used-by edits, grouped per required project so each file is
    // rewritten once however many packages moved.
    let mut by_target: BTreeMap<&str, Vec<Edit>> = BTreeMap::new();
    for (pkg, forward) in &changed {
        for (target, lines) in edits(stored(pkg), forward.as_ref()) {
            by_target.entry(target).or_default().push((pkg, lines));
        }
    }
    let regenerate = scope == Scope::Regenerate;
    let mut targets: Vec<String> = by_target.keys().map(|t| t.to_string()).collect();
    if regenerate {
        // Start clean: a file no package names any more is dropped too.
        let prefix = format!("{STATE_PREFIX}used-by/");
        for entry in storage.list_dir_entries(&prefix).await? {
            let Some(pkg) = entry
                .key
                .strip_prefix(&prefix)
                .and_then(|k| k.strip_suffix(".json"))
            else {
                continue;
            };
            if !by_target.contains_key(pkg) {
                targets.push(pkg.to_string());
            }
        }
    }
    let written: Vec<(String, Result<bool>)> = stream::iter(targets)
        .map(|target| {
            let edits = by_target.get(target.as_str()).cloned().unwrap_or_default();
            async move {
                let result = write_used_by(storage, &target, &edits, regenerate).await;
                (target, result)
            }
        })
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;
    let mut failed: HashSet<&str> = HashSet::new();
    let mut rewritten = 0usize;
    for (target, result) in &written {
        match result {
            Ok(true) => {
                rewritten += 1;
                state.project_cache.invalidate_package(target);
            }
            Ok(false) => {}
            Err(e) => {
                warn!(package=%target, error=?e, "used-by: write failed; retried next pass");
                failed.insert(target);
            }
        }
    }

    // Record each package whose used-by edits all landed. A regenerated shard
    // is rewritten whole, dropping anything not re-read.
    let mut updated: BTreeMap<char, Shard> = BTreeMap::new();
    if regenerate {
        updated.extend(seen.keys().map(|&c| (c, Shard::new())));
    }
    for (pkg, forward) in &changed {
        let landed = edits(stored(pkg), forward.as_ref())
            .iter()
            .all(|(target, _)| !failed.contains(target));
        let Some(c) = pkg.chars().next().filter(|_| landed) else {
            continue;
        };
        let shard = updated
            .entry(c)
            .or_insert_with(|| shards.get(&c).cloned().unwrap_or_default());
        match forward {
            Some(forward) => shard.insert(pkg.clone(), forward.clone()),
            None => shard.remove(pkg),
        };
    }
    for (c, shard) in updated {
        let key = requires_key(c);
        if shard.is_empty() {
            storage.delete_keys(&[key]).await?;
        } else {
            storage
                .put_bytes(&key, serde_json::to_vec(&shard)?, Some("application/json"))
                .await?;
        }
    }
    if !changed.is_empty() {
        info!(
            packages = changed.len(),
            files = rewritten,
            failed = failed.len(),
            "used-by: refreshed"
        );
    }
    Ok(())
}

/// The newest release of `pkg` from a fresh listing of its directory.
async fn relisted(state: &AppState, storage: &dyn Storage, pkg: &str) -> Result<Option<String>> {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let entries = storage.list_dir_entries(&prefix).await?;
    let members: HashSet<&str> = entries
        .iter()
        .filter_map(|e| e.key.strip_prefix(&prefix))
        .collect();
    Ok(newest_listed(state, pkg, &members))
}

/// Apply `edits` to one project's used-by file (replacing it outright when
/// `regenerate`); `true` when it changed.
async fn write_used_by(
    storage: &dyn Storage,
    target: &str,
    edits: &[Edit<'_>],
    regenerate: bool,
) -> Result<bool> {
    let key = used_by_key(target);
    let before: Lines = read_json(storage, &key).await?.unwrap_or_default();
    let mut after = if regenerate {
        Lines::new()
    } else {
        before.clone()
    };
    for (pkg, lines) in edits {
        match lines {
            Some(lines) => after.insert(pkg.to_string(), (*lines).clone()),
            None => after.remove(*pkg),
        };
    }
    if after == before {
        return Ok(false);
    }
    if after.is_empty() {
        storage.delete_keys(&[key]).await?;
    } else {
        storage
            .put_bytes(&key, serde_json::to_vec(&after)?, Some("application/json"))
            .await?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forward(requires: &[(&str, &str)]) -> Forward {
        let mut lines = Lines::new();
        for (name, line) in requires {
            lines
                .entry(name.to_string())
                .or_default()
                .push(line.to_string());
        }
        Forward {
            source: "x".into(),
            requires: lines,
        }
    }

    #[test]
    fn newest_prefers_the_highest_final_release() {
        let files = [
            "app-1.0-py3-none-any.whl",
            "app-2.0-py3-none-any.whl",
            "app-10.0rc1-py3-none-any.whl",
            "app-2.0.tar.gz",
        ];
        assert_eq!(newest(files).as_deref(), Some("app-2.0.tar.gz"));
        let pre = ["app-1.0rc1-py3-none-any.whl", "app-1.0b1-py3-none-any.whl"];
        assert_eq!(newest(pre).as_deref(), Some("app-1.0rc1-py3-none-any.whl"));
        assert_eq!(newest(["not-a-dist.zip"]), None);
    }

    #[test]
    fn requires_groups_lines_by_project() {
        let metadata = b"Metadata-Version: 2.1\nName: app\nVersion: 1.0\n\
Requires-Dist: Requests>=2.28\n\
Requires-Dist: numpy>=1.26; python_version >= \"3.12\"\n\
Requires-Dist: numpy>=1.22; python_version < \"3.12\"\n\
Requires-Dist: app[extra]\n\
Requires-Dist: thing @ https://example.com/thing.whl\n";
        let got = requires("app", metadata);
        assert_eq!(got.keys().collect::<Vec<_>>(), ["numpy", "requests"]);
        assert_eq!(got["numpy"].len(), 2);
        assert_eq!(got["requests"], ["Requests>=2.28"]);
    }

    #[test]
    fn edits_touch_only_changed_projects() {
        let old = forward(&[("requests", "requests>=2"), ("click", "click")]);
        let same = forward(&[("requests", "requests>=2"), ("click", "click")]);
        assert!(edits(Some(&old), Some(&same)).is_empty());

        let new = forward(&[("requests", "requests>=3"), ("rich", "rich")]);
        let got: Vec<(&str, bool)> = edits(Some(&old), Some(&new))
            .into_iter()
            .map(|(name, lines)| (name, lines.is_some()))
            .collect();
        assert_eq!(got, [("click", false), ("requests", true), ("rich", true)]);
        assert_eq!(edits(None, Some(&new)).len(), 2);
        assert_eq!(edits(Some(&old), None).len(), 2);
    }

    #[test]
    fn admitting_filters_by_version() {
        let mut used_by = Lines::new();
        used_by.insert("new".into(), vec!["lib>=2".into()]);
        used_by.insert("old".into(), vec!["lib<2".into()]);
        used_by.insert("any".into(), vec!["lib".into()]);
        used_by.insert(
            "split".into(),
            vec![
                "lib<1; python_version < \"3\"".into(),
                "lib>=2.5; python_version >= \"3\"".into(),
            ],
        );
        let names = |v: &str| {
            admitting(used_by.clone(), v)
                .into_keys()
                .collect::<Vec<_>>()
        };
        assert_eq!(names("2.1"), ["any", "new"]);
        assert_eq!(names("1.0"), ["any", "old"]);
        assert_eq!(names("3.0"), ["any", "new", "split"]);
        assert_eq!(names("not-pep440").len(), 4);
    }
}
