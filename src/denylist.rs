//! The `--exclude-package` denylist: one shared predicate for what a blocklist
//! entry removes from the materialized indexes. A bare name delists the whole
//! project; a version-pinned entry delists only the matching releases. The bytes
//! are never deleted — they stay fetchable by direct `/files/` URL — so this only
//! governs what installers can resolve.
//!
//! It is deliberately a standalone type so the three paths that must agree on
//! "what an exclude hides" cannot drift: the live serving worker (index rebuild
//! and the `/project/` page), the offline `rebuild-index` audit, and the
//! `verify-index` oracle all rule through the exact same predicate.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use anyhow::{Context as _, Result};
use pep440_rs::{Version, VersionSpecifiers};

use crate::names::infer_version_from_filename;
use crate::sync::PackageSpec;

/// Whether a file's inferred version satisfies a name's constraints. A bare entry
/// (no specifiers) matches every version; otherwise the version must parse and
/// match at least one specifier — a file whose version can't be parsed can't be
/// proven to match, so it's treated as not matching (the conservative rule `sync`
/// and the proxy scope both apply).
pub(crate) fn version_allowed(constraints: &[Option<VersionSpecifiers>], filename: &str) -> bool {
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

/// A PEP 503-normalized name → its exclude constraints. `None` in the vec is a
/// bare (whole-name) exclude; a `Some` is a version-pinned one. An empty map is
/// the "nothing excluded" case (still a valid, live denylist — the reconcile uses
/// it to relist everything a prior config had delisted).
#[derive(Debug, Default, Clone)]
pub struct Denylist {
    deny: HashMap<String, Vec<Option<VersionSpecifiers>>>,
}

impl Denylist {
    /// Build from resolved mirror `exclude-packages` specs. Duplicate entries for
    /// one name union their constraints (matching the proxy scope and `sync`).
    pub(crate) fn from_specs(specs: &[PackageSpec]) -> Self {
        let mut deny: HashMap<String, Vec<Option<VersionSpecifiers>>> = HashMap::new();
        for spec in specs {
            deny.entry(spec.name.clone())
                .or_default()
                .push(spec.specifiers.clone());
        }
        Self { deny }
    }

    /// Reconstruct from a persisted [`Denylist::canonical`] stamp — the inverse of
    /// `canonical`, so the offline maintenance commands can adopt exactly what
    /// `serve` last enforced (stored in `_state/enforced-excludes.json`) rather
    /// than re-deriving it from a config channel `serve` may not have used. `"*"`
    /// is a bare whole-name exclude; any other token is a PEP 440 specifier set.
    ///
    /// `pub` also so the deterministic simulator can configure a denylist without
    /// building a whole mirror config: the stamp format is the smallest complete
    /// description of one, and it is the same shape the reconcile diffs.
    pub fn from_canonical(canonical: &BTreeMap<String, Vec<String>>) -> Result<Self> {
        let mut deny: HashMap<String, Vec<Option<VersionSpecifiers>>> = HashMap::new();
        for (name, specs) in canonical {
            let mut constraints = Vec::with_capacity(specs.len());
            for spec in specs {
                if spec == "*" {
                    constraints.push(None);
                } else {
                    constraints.push(Some(VersionSpecifiers::from_str(spec).with_context(
                        || {
                            format!(
                                "enforced-denylist stamp: unparsable specifier {spec:?} for {name}"
                            )
                        },
                    )?));
                }
            }
            deny.insert(name.clone(), constraints);
        }
        Ok(Self { deny })
    }

    pub fn is_empty(&self) -> bool {
        self.deny.is_empty()
    }

    /// Is the whole project denied? True only for a bare (unpinned) exclude, which
    /// is what lets the pull gate ([`crate::proxy::Proxy::eligible`]) keep a
    /// fully-denied name from ever being fetched upstream.
    pub fn name_fully_denied(&self, pkg: &str) -> bool {
        self.deny
            .get(pkg)
            .is_some_and(|constraints| constraints.iter().any(Option::is_none))
    }

    /// Would the denylist drop this file from what installers see? True for a
    /// bare exclude, or a version-pinned one the file's inferred version matches.
    /// This is the delisting primitive the index rebuild, the `/project/` page,
    /// and the verify oracle share.
    pub fn file_denied(&self, pkg: &str, filename: &str) -> bool {
        self.deny
            .get(pkg)
            .is_some_and(|constraints| version_allowed(constraints, filename))
    }

    /// A stable, comparable stamp: normalized name → sorted specifier strings,
    /// with `"*"` marking a bare whole-name exclude. Empty when nothing is
    /// excluded. The startup reconcile diffs this against the set the stored
    /// indexes were last built against, so an exclude change made across a restart
    /// delists (or relists) exactly the affected names.
    pub fn canonical(&self) -> BTreeMap<String, Vec<String>> {
        let mut out = BTreeMap::new();
        for (name, constraints) in &self.deny {
            let mut specs: Vec<String> = constraints
                .iter()
                .map(|c| match c {
                    Some(s) => s.to_string(),
                    None => "*".to_string(),
                })
                .collect();
            specs.sort();
            specs.dedup();
            out.insert(name.clone(), specs);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, specifiers: Option<&str>) -> PackageSpec {
        PackageSpec {
            name: name.to_string(),
            specifiers: specifiers.map(|s| VersionSpecifiers::from_str(s).unwrap()),
        }
    }

    #[test]
    fn file_denied_matches_the_delisting_the_index_rebuild_applies() {
        let dl = Denylist::from_specs(&[spec("blocked", None), spec("pinned", Some("<2"))]);
        // A bare exclude denies every file of the name — the rebuild empties the
        // package, so it delists entirely.
        assert!(dl.name_fully_denied("blocked"));
        assert!(dl.file_denied("blocked", "blocked-1.0-py3-none-any.whl"));
        assert!(dl.file_denied("blocked", "blocked-anything.tar.gz"));
        // A version pin denies only the matching releases; the rest survive.
        assert!(!dl.name_fully_denied("pinned"));
        assert!(dl.file_denied("pinned", "pinned-1.9-py3-none-any.whl"));
        assert!(!dl.file_denied("pinned", "pinned-2.0-py3-none-any.whl"));
        // An unparsable version under a pin can't be proven to match → not denied.
        assert!(!dl.file_denied("pinned", "pinned-garbage.whl"));
        // A name with no exclude entry is never denied.
        assert!(!dl.file_denied("free", "free-1.0-py3-none-any.whl"));
    }

    #[test]
    fn canonical_is_a_stable_comparable_stamp() {
        let dl = Denylist::from_specs(&[
            spec("blocked", None),
            spec("pinned", Some("<2")),
            spec("pinned", Some(">=3")),
        ]);
        let canonical = dl.canonical();
        assert_eq!(canonical.get("blocked"), Some(&vec!["*".to_string()]));
        // Specifier strings are sorted, so the same config always stamps equal.
        assert_eq!(
            canonical.get("pinned"),
            Some(&vec!["<2".to_string(), ">=3".to_string()])
        );
        // No excludes → empty stamp (the "nothing enforced" baseline).
        assert!(Denylist::default().canonical().is_empty());
        assert!(Denylist::default().is_empty());
    }

    #[test]
    fn from_canonical_round_trips_the_stamp() {
        // What the maintenance commands adopt from the stamp must decide exactly
        // what the config that wrote it would: canonical → Denylist → same verdicts.
        let original = Denylist::from_specs(&[
            spec("blocked", None),
            spec("pinned", Some("<2")),
            spec("pinned", Some(">=3")),
        ]);
        let restored = Denylist::from_canonical(&original.canonical()).unwrap();
        assert_eq!(restored.canonical(), original.canonical());
        assert!(restored.name_fully_denied("blocked"));
        assert!(restored.file_denied("pinned", "pinned-1.9-py3-none-any.whl"));
        assert!(restored.file_denied("pinned", "pinned-3.1-py3-none-any.whl"));
        assert!(!restored.file_denied("pinned", "pinned-2.5-py3-none-any.whl"));
        // A corrupt specifier surfaces as an error, not a silently empty denylist.
        let bad = BTreeMap::from([("x".to_string(), vec!["not-a-specifier".to_string()])]);
        assert!(Denylist::from_canonical(&bad).is_err());
    }
}
