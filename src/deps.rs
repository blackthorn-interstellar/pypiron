//! Dependency following, shared by `sync` and the `serve` proxy so the two
//! reach exactly the same projects: an include list plus everything its
//! packages' `Requires-Dist` reach, each dependency taking the versions some
//! requirement allows. Requirements come only from PEP 658 metadata files —
//! static text — so nothing is ever built or run to find them.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;

use pep440_rs::{Version, VersionSpecifiers};
use serde::{Deserialize, Serialize};

use crate::names::{
    checked_pkg_name, infer_version_from_filename, normalize_pkg_name, PrivateNames,
};
use crate::simple::SimpleFile;
use crate::sync::{file_format, spec_matches_filename, Format, PackageSpec};

/// One project and the versions taken of it: those admitted by any of
/// `specifiers`, or every version when `None`. A listed package brings its own
/// spec; a dependency brings each requirement that pulled it in.
pub(crate) struct Job {
    pub(crate) name: String,
    pub(crate) specifiers: Option<Vec<VersionSpecifiers>>,
    /// Pulled in by `include-dependencies` rather than listed.
    pub(crate) dependency: bool,
}

impl Job {
    pub(crate) fn admits(&self, filename: &str) -> bool {
        self.specifiers
            .as_ref()
            .is_none_or(|any| any.iter().any(|s| spec_matches_filename(s, filename)))
    }
}

/// The versions taken of one project, as specifier strings; `None` takes every
/// version. Strings, so two rounds can tell whether anything changed.
pub(crate) type Want = Option<BTreeSet<String>>;

fn widen(want: &mut Want, specifiers: Option<&VersionSpecifiers>) {
    match (want.as_mut(), specifiers) {
        (Some(set), Some(s)) => {
            set.insert(s.to_string());
        }
        (Some(_), None) => *want = None,
        (None, _) => {}
    }
}

/// One `Requires-Dist` entry, as far as following it needs.
#[derive(Debug, PartialEq)]
pub(crate) struct Requirement {
    pub(crate) name: String,
    /// `None` admits every version, as does a range that doesn't parse.
    pub(crate) specifiers: Option<VersionSpecifiers>,
    /// Extras this requirement asks of `name` (`urllib3[socks]`).
    extras: Vec<String>,
    /// The requirer's extras it belongs to (`; extra == "x"`); empty = always.
    only_for: Vec<String>,
}

/// Parse a `Requires-Dist` entry. Platform and Python markers are ignored, so
/// every platform is followed; only the `extra` ones matter. `None` for a
/// direct URL reference, which no index can serve.
pub(crate) fn parse_requirement(req: &str) -> Option<Requirement> {
    let (req, marker) = req.split_once(';').unwrap_or((req, ""));
    let req = req.trim();
    if req.contains('@') {
        return None;
    }
    let end = req
        .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
        .unwrap_or(req.len());
    let name = checked_pkg_name(&req[..end])?;
    let mut rest = req[end..].trim_start();
    let mut extras = Vec::new();
    if let Some(open) = rest.strip_prefix('[') {
        let (list, after) = open.split_once(']')?;
        extras = list
            .split(',')
            .map(|e| normalize_pkg_name(e.trim()))
            .filter(|e| !e.is_empty())
            .collect();
        rest = after;
    }
    let rest = rest
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let specifiers = (!rest.is_empty())
        .then(|| VersionSpecifiers::from_str(rest).ok())
        .flatten();
    Some(Requirement {
        name,
        specifiers,
        extras,
        only_for: marker_extras(marker),
    })
}

/// The extras an environment marker names with `extra == "…"` (either way
/// round). Any other test of `extra` is unusual enough to follow always.
fn marker_extras(marker: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut rest = marker.trim_start();
    while let Some(c) = rest.chars().next() {
        let len = match c {
            '"' | '\'' => rest[1..].find(c).map_or(rest.len(), |i| i + 2),
            c if c.is_alphanumeric() || c == '_' => rest
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(rest.len()),
            _ => rest
                .find(|c: char| c.is_alphanumeric() || "_\"' ()".contains(c))
                .filter(|&i| i > 0)
                .unwrap_or(c.len_utf8()),
        };
        tokens.push(&rest[..len]);
        rest = rest[len..].trim_start();
    }
    let quoted = |t: &str| {
        (t.len() >= 2 && (t.starts_with('"') || t.starts_with('\'')))
            .then(|| normalize_pkg_name(&t[1..t.len() - 1]))
    };
    tokens
        .windows(3)
        .filter(|w| w[1] == "==")
        .filter_map(|w| match (w[0], w[2]) {
            ("extra", value) | (value, "extra") => quoted(value),
            _ => None,
        })
        .collect()
}

/// Everything an include list reaches: the listed packages as given, plus every project
/// their known requirements reach, each taking the versions some requirement
/// admits. A listed package keeps its own spec — approval is explicit, and a
/// dependency never widens it. Every extra of a listed package is followed;
/// a dependency's extras only when a requirement asks for them, as an
/// installer would. Only the releases a project's wanted versions admit
/// count, so narrowing an include drops what only the excluded releases
/// needed. Reserved private names are never followed: the server serves
/// those itself.
pub(crate) fn closure(
    roots: &[PackageSpec],
    memos: &HashMap<String, DepsMemo>,
    private: &PrivateNames,
) -> BTreeMap<String, Want> {
    let mut wants = BTreeMap::new();
    for spec in roots {
        widen(
            wants
                .entry(spec.name.clone())
                .or_insert_with(|| Some(Default::default())),
            spec.specifiers.as_ref(),
        );
    }
    let listed: HashSet<String> = wants.keys().cloned().collect();
    // The extras followed so far per dependency; a project is revisited when
    // they or its wanted versions grow. Listed projects follow all extras.
    let mut extras: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut queue: Vec<String> = listed.iter().cloned().collect();
    while let Some(name) = queue.pop() {
        let Some(memo) = memos.get(&name) else {
            continue;
        };
        let requires = memo.requires(&wants[&name]);
        for req in requires {
            let Some(req) = parse_requirement(req) else {
                continue;
            };
            let active = listed.contains(&name)
                || req.only_for.is_empty()
                || extras
                    .get(&name)
                    .is_some_and(|on| req.only_for.iter().any(|e| on.contains(e)));
            if !active || listed.contains(&req.name) || private.matches(&req.name) {
                continue;
            }
            let before = wants.get(&req.name).cloned();
            let want = wants
                .entry(req.name.clone())
                .or_insert_with(|| Some(Default::default()));
            widen(want, req.specifiers.as_ref());
            let wider = before.as_ref() != Some(want);
            let on = extras.entry(req.name.clone()).or_default();
            let before = on.len();
            on.extend(req.extras);
            if wider || on.len() > before {
                queue.push(req.name);
            }
        }
    }
    wants
}

/// The projects a round still has to visit: those not yet visited for the
/// versions now wanted of them. Each (project, want) pair is visited once and
/// the pairs are finite, so repeating this until it comes back empty ends.
pub(crate) fn pending(
    roots: &[PackageSpec],
    memos: &HashMap<String, DepsMemo>,
    private: &PrivateNames,
    done: &HashSet<(String, Want)>,
) -> Vec<(Job, Want)> {
    closure(roots, memos, private)
        .into_iter()
        .filter(|(name, want)| !done.contains(&(name.clone(), want.clone())))
        .map(|(name, want)| {
            let job = Job {
                dependency: !roots.iter().any(|s| s.name == name),
                specifiers: specifiers(&want),
                name,
            };
            (job, want)
        })
        .collect()
}

pub(crate) fn specifiers(want: &Want) -> Option<Vec<VersionSpecifiers>> {
    want.as_ref().map(|set| {
        set.iter()
            .filter_map(|s| VersionSpecifiers::from_str(s).ok())
            .collect()
    })
}

/// What one project's selected releases require, as read so far. A release's
/// metadata never changes, so only new releases are ever read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct DepsMemo {
    /// Release version → its `Requires-Dist` entries; empty for a release
    /// without a metadata file.
    pub(crate) releases: BTreeMap<String, Vec<String>>,
}

impl DepsMemo {
    /// The release not yet read for each version among `files`, one file per
    /// version: one with a metadata file if any, a wheel's by preference.
    pub(crate) fn unread<'a>(
        &self,
        files: impl IntoIterator<Item = &'a SimpleFile>,
    ) -> Vec<(String, &'a SimpleFile)> {
        let rank = |f: &SimpleFile| {
            (
                f.has_core_metadata(),
                file_format(&f.filename) == Format::Wheel,
            )
        };
        let mut pick: BTreeMap<String, &SimpleFile> = BTreeMap::new();
        for file in files {
            let Some(version) = infer_version_from_filename(&file.filename) else {
                continue;
            };
            if self.releases.contains_key(&version) {
                continue;
            }
            let best = pick.entry(version).or_insert(file);
            if rank(file) > rank(best) {
                *best = file;
            }
        }
        pick.into_iter().collect()
    }

    /// Record a read release: its requirements, or `None` when it has no
    /// metadata file to read them from.
    pub(crate) fn record(&mut self, version: String, requires: Option<Vec<String>>) {
        self.releases.insert(version, requires.unwrap_or_default());
    }

    /// Forget releases no longer among `files` (a narrowed filter, a yank), so
    /// what only they required stops being followed. True if any went.
    pub(crate) fn keep_only<'a>(
        &mut self,
        files: impl IntoIterator<Item = &'a SimpleFile>,
    ) -> bool {
        let present: HashSet<String> = files
            .into_iter()
            .filter_map(|f| infer_version_from_filename(&f.filename))
            .collect();
        let before = self.releases.len();
        self.releases.retain(|version, _| present.contains(version));
        self.releases.len() < before
    }

    /// The requirements of the releases `want` admits.
    pub(crate) fn requires(&self, want: &Want) -> BTreeSet<&str> {
        let specs = specifiers(want);
        self.releases
            .iter()
            .filter(|(version, _)| {
                specs.as_ref().is_none_or(|any| {
                    Version::from_str(version).is_ok_and(|v| any.iter().any(|s| s.contains(&v)))
                })
            })
            .flat_map(|(_, requires)| requires.iter().map(String::as_str))
            .collect()
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
    fn parse_requirement_reads_name_range_and_extras() {
        let req = |name: &str, s: Option<&str>, extras: &[&str], only_for: &[&str]| {
            Some(Requirement {
                name: name.to_string(),
                specifiers: s.map(|s| VersionSpecifiers::from_str(s).unwrap()),
                extras: extras.iter().map(|e| e.to_string()).collect(),
                only_for: only_for.iter().map(|e| e.to_string()).collect(),
            })
        };
        assert_eq!(
            parse_requirement("idna<4,>=2.5"),
            req("idna", Some("<4,>=2.5"), &[], &[])
        );
        assert_eq!(
            parse_requirement("PySocks!=1.5.7,>=1.5.6; extra == \"socks\""),
            req("pysocks", Some("!=1.5.7,>=1.5.6"), &[], &["socks"])
        );
        assert_eq!(
            parse_requirement("chardet (<6,>=3.0.2) ; 'Use_Chardet' == extra"),
            req("chardet", Some("<6,>=3.0.2"), &[], &["use-chardet"])
        );
        assert_eq!(
            parse_requirement(
                "Foo_Bar[Baz, qux]>=1; python_version<'3.9' and (extra=='a' or extra == \"b\")"
            ),
            req("foo-bar", Some(">=1"), &["baz", "qux"], &["a", "b"])
        );
        // Non-ASCII in a marker is just another token.
        assert_eq!(
            parse_requirement("x; extra ≥ \"é\" or extra == \"ok\""),
            req("x", None, &[], &["ok"])
        );
        // Platform markers don't gate anything: every platform is followed.
        assert_eq!(
            parse_requirement("pywin32; sys_platform == \"win32\""),
            req("pywin32", None, &[], &[])
        );
        // An unparseable range admits every version; a URL is no index's to serve.
        assert_eq!(
            parse_requirement("odd >=1.0-beta-legacy"),
            req("odd", None, &[], &[])
        );
        assert_eq!(parse_requirement("pkg @ https://example.com/pkg.whl"), None);
        assert_eq!(parse_requirement(""), None);
    }

    /// A release's version and requirements.
    type Release<'a> = (&'a str, &'a [&'a str]);

    /// project → its releases' requirements.
    fn memos(entries: &[(&str, &[Release])]) -> HashMap<String, DepsMemo> {
        entries
            .iter()
            .map(|(name, releases)| {
                let releases = releases
                    .iter()
                    .map(|(v, r)| (v.to_string(), r.iter().map(|s| s.to_string()).collect()))
                    .collect();
                (name.to_string(), DepsMemo { releases })
            })
            .collect()
    }

    #[test]
    fn closure_follows_requirements_and_keeps_listed_specs() {
        let requires = memos(&[
            (
                "app",
                &[("1.0", &["lib>=2", "extra-dep; extra == \"x\"", "acme-auth"])],
            ),
            ("lib", &[("1.0", &[]), ("2.0", &["leaf==1.0", "app>=9"])]),
            ("other", &[("1.0", &["lib<3"])]),
            ("unlisted", &[("1.0", &["never"])]),
        ]);
        let private = PrivateNames::new(Some("acme"), Vec::new()).unwrap();
        let want = |set: &[&str]| Some(set.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>());

        let wants = closure(&[spec("app", Some("==1.0"))], &requires, &private);
        assert_eq!(
            wants.into_iter().collect::<Vec<_>>(),
            vec![
                // A dependency never widens a listed package (lib wants app>=9).
                ("app".to_string(), want(&["==1.0"])),
                ("extra-dep".to_string(), None),
                ("leaf".to_string(), want(&["==1.0"])),
                ("lib".to_string(), want(&[">=2"])),
            ]
        );

        // Two requirers union their ranges; a listed package keeps its own.
        let wants = closure(
            &[spec("app", None), spec("other", None)],
            &requires,
            &private,
        );
        assert_eq!(wants["lib"], want(&["<3", ">=2"]));
        let wants = closure(
            &[spec("app", None), spec("lib", Some(">=2"))],
            &requires,
            &private,
        );
        assert_eq!(wants["lib"], want(&[">=2"]));
        // Only the releases a want admits count: narrowing an include drops
        // what only the excluded releases needed.
        let wants = closure(&[spec("lib", Some("==1.0"))], &requires, &private);
        assert_eq!(wants.keys().collect::<Vec<_>>(), ["lib"]);
        let wants = closure(&[spec("lib", None)], &requires, &private);
        assert_eq!(wants.keys().collect::<Vec<_>>(), ["app", "leaf", "lib"]);
        // A want widened by a later requirer brings in the releases it admits.
        let widening = memos(&[
            ("a", &[("1", &["lib<2"])]),
            ("b", &[("1", &["lib>=2"])]),
            ("lib", &[("1.0", &["old"]), ("2.0", &["new"])]),
        ]);
        let wants = closure(&[spec("a", None), spec("b", None)], &widening, &private);
        assert_eq!(
            wants.keys().collect::<Vec<_>>(),
            ["a", "b", "lib", "new", "old"]
        );

        // A dependency's extras are followed only when a requirement asks.
        let requires = memos(&[
            ("top", &[("1", &["mid", "opt[fast]; extra == \"perf\""])]),
            ("mid", &[("1", &["test-only; extra == \"test\"", "opt"])]),
            (
                "opt",
                &[(
                    "1",
                    &["speedup; extra == \"fast\"", "docs-only; extra == \"docs\""],
                )],
            ),
        ]);
        let wants = closure(&[spec("top", None)], &requires, &private);
        assert_eq!(
            wants.keys().map(String::as_str).collect::<Vec<_>>(),
            ["mid", "opt", "speedup", "top"]
        );

        // Listing a name twice unions its specs; a bare listing takes everything.
        let wants = closure(
            &[spec("app", Some("<1")), spec("app", Some(">2"))],
            &requires,
            &private,
        );
        assert_eq!(wants["app"], want(&["<1", ">2"]));
        let wants = closure(
            &[spec("app", Some("<1")), spec("app", None)],
            &requires,
            &private,
        );
        assert_eq!(wants["app"], None);
    }
}
