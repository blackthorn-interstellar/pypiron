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
    /// The environment marker, unparsed; [`Targets::admits`] reads it.
    marker: String,
}

/// Parse a `Requires-Dist` entry. `None` for a direct URL reference, which no
/// index can serve.
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
        marker: marker.trim().to_string(),
    })
}

/// A marker's tokens: quoted strings, names, operators, parentheses.
fn marker_tokens(marker: &str) -> Vec<&str> {
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
    tokens
}

fn unquote(token: &str) -> Option<&str> {
    (token.len() >= 2 && (token.starts_with('"') || token.starts_with('\'')))
        .then(|| &token[1..token.len() - 1])
}

/// The extras an environment marker names with `extra == "…"` (either way
/// round). Any other test of `extra` is unusual enough to follow always.
fn marker_extras(marker: &str) -> Vec<String> {
    let tokens = marker_tokens(marker);
    tokens
        .windows(3)
        .filter(|w| w[1] == "==")
        .filter_map(|w| match (w[0], w[2]) {
            ("extra", value) | (value, "extra") => unquote(value).map(normalize_pkg_name),
            _ => None,
        })
        .collect()
}

/// A platform the dependency follower knows the marker values of, and wheel
/// platform tags that install on it.
struct Platform {
    sys_platform: &'static str,
    platform_system: &'static str,
    os_name: &'static str,
    platform_machine: &'static str,
    /// Tag stems: Linux and macOS take a run of glibc/musl/macOS versions
    /// (see [`Platform::tags`]); Windows tags are exact.
    os: Os,
    arch: &'static str,
}

#[derive(Clone, Copy)]
enum Os {
    Linux,
    Mac,
    Windows,
}

const PLATFORMS: &[Platform] = &[
    linux("x86_64"),
    linux("aarch64"),
    linux("i686"),
    linux("ppc64le"),
    linux("s390x"),
    linux("armv7l"),
    mac("x86_64"),
    mac("arm64"),
    windows("AMD64", "amd64"),
    windows("ARM64", "arm64"),
    windows("x86", ""),
];

const fn linux(arch: &'static str) -> Platform {
    Platform {
        sys_platform: "linux",
        platform_system: "Linux",
        os_name: "posix",
        platform_machine: arch,
        os: Os::Linux,
        arch,
    }
}

const fn mac(arch: &'static str) -> Platform {
    Platform {
        sys_platform: "darwin",
        platform_system: "Darwin",
        os_name: "posix",
        platform_machine: arch,
        os: Os::Mac,
        arch,
    }
}

const fn windows(machine: &'static str, arch: &'static str) -> Platform {
    Platform {
        sys_platform: "win32",
        platform_system: "Windows",
        os_name: "nt",
        platform_machine: machine,
        os: Os::Windows,
        arch,
    }
}

impl Platform {
    /// Wheel platform tags built for this platform, spanning the glibc, musl
    /// and macOS versions a filter might name.
    fn tags(&self) -> Vec<String> {
        let arch = self.arch;
        match self.os {
            Os::Linux => ["manylinux1", "manylinux2010", "manylinux2014", "linux"]
                .into_iter()
                .map(|p| format!("{p}_{arch}"))
                .chain((5..=45).map(|n| format!("manylinux_2_{n}_{arch}")))
                .chain((0..=5).map(|n| format!("musllinux_1_{n}_{arch}")))
                .collect(),
            Os::Mac => (0..=16)
                .map(|n| format!("macosx_10_{n}_{arch}"))
                .chain((11..=30).map(|n| format!("macosx_{n}_0_{arch}")))
                .collect(),
            Os::Windows if arch.is_empty() => vec!["win32".to_string()],
            Os::Windows => vec![format!("win_{arch}")],
        }
    }

    fn get(&self, var: &str) -> Option<&'static str> {
        Some(match var {
            "sys_platform" => self.sys_platform,
            "platform_system" => self.platform_system,
            "os_name" => self.os_name,
            "platform_machine" => self.platform_machine,
            _ => return None,
        })
    }
}

/// Every wheel platform tag the follower knows a platform for.
pub(crate) fn known_platform_tags() -> impl Iterator<Item = String> {
    PLATFORMS.iter().flat_map(Platform::tags)
}

/// Where the mirror's releases will be installed, as far as its filters say:
/// the platforms whose wheels they keep, and the oldest Python they keep. A
/// requirement whose marker can't hold anywhere there — `pywin32; sys_platform
/// == "win32"` on a Linux-only mirror, `importlib-metadata; python_version <
/// "3.8"` above a 3.9 floor — is not followed. Anything unsure is followed.
#[derive(Default)]
pub(crate) struct Targets {
    /// `None` = any platform at all.
    platforms: Option<Vec<&'static Platform>>,
    python_floor: Option<Version>,
}

impl Targets {
    /// `kept` says whether the mirror keeps a wheel with this platform tag;
    /// `None` when its filters don't narrow platforms (or name one the
    /// follower doesn't know).
    pub(crate) fn new(
        kept: Option<impl Fn(&str) -> bool>,
        python_floor: Option<(u32, u32)>,
    ) -> Self {
        let platforms = kept.map(|kept| {
            PLATFORMS
                .iter()
                .filter(|p| p.tags().iter().any(|t| kept(t)))
                .collect::<Vec<_>>()
        });
        Targets {
            // Keeping no known platform's wheels is a pure-Python mirror,
            // which installs anywhere.
            platforms: platforms.filter(|p| !p.is_empty()),
            python_floor: python_floor
                .and_then(|(major, minor)| Version::from_str(&format!("{major}.{minor}")).ok()),
        }
    }

    /// Whether a requirement with this marker can apply somewhere targeted.
    pub(crate) fn admits(&self, marker: &str) -> bool {
        let tokens = marker_tokens(marker);
        if tokens.is_empty() || (self.platforms.is_none() && self.python_floor.is_none()) {
            return true;
        }
        let holds = |platform| {
            let env = Env {
                platform,
                python_floor: self.python_floor.as_ref(),
            };
            let mut parser = Parser {
                tokens: &tokens,
                at: 0,
                env: &env,
            };
            // A marker that doesn't parse is followed.
            let value = parser.or().filter(|_| parser.at == tokens.len());
            value != Some(Some(false))
        };
        match &self.platforms {
            None => holds(None),
            Some(platforms) => platforms.iter().any(|p| holds(Some(p))),
        }
    }
}

/// One target environment. Marker values are three-valued: `None` = can't
/// tell (an extra, an implementation, a value the table doesn't know).
struct Env<'a> {
    platform: Option<&'a Platform>,
    python_floor: Option<&'a Version>,
}

impl Env<'_> {
    fn compare(&self, left: &str, op: &str, right: &str) -> Option<bool> {
        let (var, op, value) = match (unquote(left), unquote(right)) {
            (None, Some(value)) => (left, op, value),
            (Some(value), None) => {
                let flipped = match op {
                    "<" => ">",
                    "<=" => ">=",
                    ">" => "<",
                    ">=" => "<=",
                    "==" | "!=" => op,
                    _ => return None,
                };
                (right, flipped, value)
            }
            _ => return None,
        };
        match var {
            "python_version" | "python_full_version" => {
                let floor = self.python_floor?;
                let v = Version::from_str(value.trim_end_matches(".*")).ok()?;
                // Some Python at or above the floor could satisfy it.
                let possible = match op {
                    "<" => floor < &v,
                    "<=" | "==" | "===" => floor <= &v,
                    _ => true,
                };
                (!possible).then_some(false)
            }
            _ => {
                let platform = self.platform?;
                let actual = platform.get(var)?;
                // A value no known platform has may be a platform we don't know.
                if !PLATFORMS.iter().any(|p| p.get(var) == Some(value)) {
                    return None;
                }
                match op {
                    "==" => Some(actual == value),
                    "!=" => Some(actual != value),
                    _ => None,
                }
            }
        }
    }
}

/// `or` of `and`s of comparisons and parenthesized groups (PEP 508). Each
/// level returns `None` on a syntax error, else the three-valued result.
struct Parser<'a> {
    tokens: &'a [&'a str],
    at: usize,
    env: &'a Env<'a>,
}

impl<'a> Parser<'a> {
    fn next(&mut self) -> Option<&'a str> {
        let token = self.tokens.get(self.at).copied()?;
        self.at += 1;
        Some(token)
    }

    fn eat(&mut self, token: &str) -> bool {
        let found = self.tokens.get(self.at) == Some(&token);
        self.at += usize::from(found);
        found
    }

    fn or(&mut self) -> Option<Option<bool>> {
        let mut value = self.and()?;
        while self.eat("or") {
            value = match (value, self.and()?) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            };
        }
        Some(value)
    }

    fn and(&mut self) -> Option<Option<bool>> {
        let mut value = self.atom()?;
        while self.eat("and") {
            value = match (value, self.atom()?) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            };
        }
        Some(value)
    }

    fn atom(&mut self) -> Option<Option<bool>> {
        if self.eat("(") {
            let value = self.or()?;
            return self.eat(")").then_some(value);
        }
        let left = self.next()?;
        let op = match self.next()? {
            "not" => self.eat("in").then_some("not in")?,
            op => op,
        };
        let right = self.next()?;
        Some(self.env.compare(left, op, right))
    }
}

/// Everything an include list reaches: the listed packages as given, plus every project
/// their known requirements reach, each taking the versions some requirement
/// admits. A listed package keeps its own spec — approval is explicit, and a
/// dependency never widens it. Every extra of a listed package is followed;
/// a dependency's extras only when a requirement asks for them, as an
/// installer would. Only the releases a project's wanted versions admit
/// count, so narrowing an include drops what only the excluded releases
/// needed, and only requirements that can apply on `targets`. Reserved
/// private names are never followed: the server serves those itself.
pub(crate) fn closure(
    roots: &[PackageSpec],
    memos: &HashMap<String, DepsMemo>,
    private: &PrivateNames,
    targets: &Targets,
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
            if !active
                || listed.contains(&req.name)
                || private.matches(&req.name)
                || !targets.admits(&req.marker)
            {
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
    targets: &Targets,
    done: &HashSet<(String, Want)>,
) -> Vec<(Job, Want)> {
    closure(roots, memos, private, targets)
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

/// What one project's selected releases require, as read so far. A
/// release's wheels can disagree — torch's x86_64 Linux wheel alone requires
/// triton — so every distinct metadata file among the selected files is read.
/// Metadata never changes, so only new files are ever read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct DepsMemo {
    /// Release version → metadata file ([`metadata_key`]) → its
    /// `Requires-Dist` entries. A release without any metadata file has one
    /// empty entry under [`NO_METADATA`], so it is reported once.
    pub(crate) metadata: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}

/// The entry of a release none of whose files has metadata.
const NO_METADATA: &str = "";

/// Which metadata file `file` has: its digest, so wheels sharing one are read
/// once, else its filename. `None` without one.
fn metadata_key(file: &SimpleFile) -> Option<String> {
    file.has_core_metadata().then(|| {
        file.core_metadata_sha256()
            .map_or_else(|| file.filename.clone(), |d| format!("sha256:{d}"))
    })
}

impl DepsMemo {
    /// Each version's metadata files among `files`, one file to read each.
    /// A version with none has one [`NO_METADATA`] entry, a wheel by preference.
    fn wanted<'a>(
        files: impl IntoIterator<Item = &'a SimpleFile>,
    ) -> BTreeMap<String, BTreeMap<String, &'a SimpleFile>> {
        let mut wanted: BTreeMap<String, BTreeMap<String, &SimpleFile>> = BTreeMap::new();
        for file in files {
            let Some(version) = infer_version_from_filename(&file.filename) else {
                continue;
            };
            let keys = wanted.entry(version).or_default();
            match metadata_key(file) {
                Some(key) => {
                    keys.remove(NO_METADATA);
                    keys.entry(key).or_insert(file);
                }
                None if keys.is_empty() => {
                    keys.insert(NO_METADATA.to_string(), file);
                }
                None => {
                    if let Some(f) = keys.get_mut(NO_METADATA) {
                        if file_format(&file.filename) == Format::Wheel {
                            *f = file;
                        }
                    }
                }
            }
        }
        wanted
    }

    /// The (version, metadata file, file to read it from) among `files` not
    /// read yet. A file without metadata stands for a release that has none.
    pub(crate) fn unread<'a>(
        &self,
        files: impl IntoIterator<Item = &'a SimpleFile>,
    ) -> Vec<(String, String, &'a SimpleFile)> {
        Self::wanted(files)
            .into_iter()
            .flat_map(|(version, keys)| {
                let read = self.metadata.get(&version);
                keys.into_iter()
                    .filter(move |(key, _)| !read.is_some_and(|r| r.contains_key(key)))
                    .map(move |(key, file)| (version.clone(), key, file))
            })
            .collect()
    }

    /// Record a read metadata file: its requirements, or `None` when the
    /// release has no metadata file to read them from.
    pub(crate) fn record(&mut self, version: String, key: String, requires: Option<Vec<String>>) {
        self.metadata
            .entry(version)
            .or_default()
            .insert(key, requires.unwrap_or_default());
    }

    /// Forget metadata no longer among `files` (a narrowed filter, a yank),
    /// so what only those files required stops being followed. True if any
    /// went.
    pub(crate) fn keep_only<'a>(
        &mut self,
        files: impl IntoIterator<Item = &'a SimpleFile>,
    ) -> bool {
        let wanted = Self::wanted(files);
        let mut changed = false;
        self.metadata.retain(|version, keys| {
            let before = keys.len();
            keys.retain(|key, _| wanted.get(version).is_some_and(|w| w.contains_key(key)));
            changed |= keys.len() < before;
            !keys.is_empty()
        });
        changed
    }

    /// The requirements of the releases `want` admits, across all their files.
    pub(crate) fn requires(&self, want: &Want) -> BTreeSet<&str> {
        let specs = specifiers(want);
        self.metadata
            .iter()
            .filter(|(version, _)| {
                specs.as_ref().is_none_or(|any| {
                    Version::from_str(version).is_ok_and(|v| any.iter().any(|s| s.contains(&v)))
                })
            })
            .flat_map(|(_, keys)| keys.values().flatten().map(String::as_str))
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
                marker: String::new(),
            })
        };
        let parse = |s: &str| {
            parse_requirement(s).map(|r| Requirement {
                marker: String::new(),
                ..r
            })
        };
        assert_eq!(
            parse("idna<4,>=2.5"),
            req("idna", Some("<4,>=2.5"), &[], &[])
        );
        assert_eq!(
            parse("PySocks!=1.5.7,>=1.5.6; extra == \"socks\""),
            req("pysocks", Some("!=1.5.7,>=1.5.6"), &[], &["socks"])
        );
        assert_eq!(
            parse("chardet (<6,>=3.0.2) ; 'Use_Chardet' == extra"),
            req("chardet", Some("<6,>=3.0.2"), &[], &["use-chardet"])
        );
        assert_eq!(
            parse("Foo_Bar[Baz, qux]>=1; python_version<'3.9' and (extra=='a' or extra == \"b\")"),
            req("foo-bar", Some(">=1"), &["baz", "qux"], &["a", "b"])
        );
        // Non-ASCII in a marker is just another token.
        assert_eq!(
            parse("x; extra ≥ \"é\" or extra == \"ok\""),
            req("x", None, &[], &["ok"])
        );
        assert_eq!(
            parse_requirement("pywin32 ; sys_platform == \"win32\" ").map(|r| r.marker),
            Some("sys_platform == \"win32\"".to_string())
        );
        // An unparseable range admits every version; a URL is no index's to serve.
        assert_eq!(parse("odd >=1.0-beta-legacy"), req("odd", None, &[], &[]));
        assert_eq!(parse("pkg @ https://example.com/pkg.whl"), None);
        assert_eq!(parse(""), None);
    }

    /// A release's version and requirements.
    type Release<'a> = (&'a str, &'a [&'a str]);

    /// project → its releases' requirements.
    fn memos(entries: &[(&str, &[Release])]) -> HashMap<String, DepsMemo> {
        entries
            .iter()
            .map(|(name, releases)| {
                let mut memo = DepsMemo::default();
                for (v, r) in releases.iter() {
                    let requires = r.iter().map(|s| s.to_string()).collect();
                    memo.record(v.to_string(), "m".to_string(), Some(requires));
                }
                (name.to_string(), memo)
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

        let wants = closure(
            &[spec("app", Some("==1.0"))],
            &requires,
            &private,
            &Targets::default(),
        );
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
            &Targets::default(),
        );
        assert_eq!(wants["lib"], want(&["<3", ">=2"]));
        let wants = closure(
            &[spec("app", None), spec("lib", Some(">=2"))],
            &requires,
            &private,
            &Targets::default(),
        );
        assert_eq!(wants["lib"], want(&[">=2"]));
        // Only the releases a want admits count: narrowing an include drops
        // what only the excluded releases needed.
        let wants = closure(
            &[spec("lib", Some("==1.0"))],
            &requires,
            &private,
            &Targets::default(),
        );
        assert_eq!(wants.keys().collect::<Vec<_>>(), ["lib"]);
        let wants = closure(
            &[spec("lib", None)],
            &requires,
            &private,
            &Targets::default(),
        );
        assert_eq!(wants.keys().collect::<Vec<_>>(), ["app", "leaf", "lib"]);
        // A want widened by a later requirer brings in the releases it admits.
        let widening = memos(&[
            ("a", &[("1", &["lib<2"])]),
            ("b", &[("1", &["lib>=2"])]),
            ("lib", &[("1.0", &["old"]), ("2.0", &["new"])]),
        ]);
        let wants = closure(
            &[spec("a", None), spec("b", None)],
            &widening,
            &private,
            &Targets::default(),
        );
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
        let wants = closure(
            &[spec("top", None)],
            &requires,
            &private,
            &Targets::default(),
        );
        assert_eq!(
            wants.keys().map(String::as_str).collect::<Vec<_>>(),
            ["mid", "opt", "speedup", "top"]
        );

        // Listing a name twice unions its specs; a bare listing takes everything.
        let wants = closure(
            &[spec("app", Some("<1")), spec("app", Some(">2"))],
            &requires,
            &private,
            &Targets::default(),
        );
        assert_eq!(wants["app"], want(&["<1", ">2"]));
        let wants = closure(
            &[spec("app", Some("<1")), spec("app", None)],
            &requires,
            &private,
            &Targets::default(),
        );
        assert_eq!(wants["app"], None);
    }

    fn file(filename: &str, metadata: Option<&str>) -> SimpleFile {
        serde_json::from_value(serde_json::json!({
            "filename": filename,
            "url": format!("https://x/{filename}"),
            "core-metadata": metadata.map_or(serde_json::json!(false), |d| serde_json::json!({"sha256": d})),
        }))
        .unwrap()
    }

    #[test]
    fn memo_reads_each_distinct_metadata_file() {
        let files = [
            file("t-1.0-cp312-cp312-manylinux_2_17_x86_64.whl", Some("aa")),
            file("t-1.0-cp313-cp313-manylinux_2_17_x86_64.whl", Some("aa")),
            file("t-1.0-cp312-cp312-macosx_11_0_arm64.whl", Some("bb")),
            file("t-1.0.tar.gz", None),
            file("t-0.9.tar.gz", None),
        ];
        let mut memo = DepsMemo::default();
        let unread: Vec<_> = memo
            .unread(&files)
            .into_iter()
            .map(|(v, k, f)| (v, k, f.filename.as_str()))
            .collect();
        assert_eq!(
            unread,
            [
                ("0.9".into(), "".into(), "t-0.9.tar.gz"),
                (
                    "1.0".into(),
                    "sha256:aa".into(),
                    "t-1.0-cp312-cp312-manylinux_2_17_x86_64.whl"
                ),
                (
                    "1.0".into(),
                    "sha256:bb".into(),
                    "t-1.0-cp312-cp312-macosx_11_0_arm64.whl"
                ),
            ]
        );
        memo.record("0.9".into(), "".into(), None);
        memo.record(
            "1.0".into(),
            "sha256:aa".into(),
            Some(vec!["triton".into()]),
        );
        memo.record("1.0".into(), "sha256:bb".into(), Some(vec!["numpy".into()]));
        assert!(memo.unread(&files).is_empty());
        assert_eq!(memo.requires(&None), BTreeSet::from(["numpy", "triton"]));

        // A filter dropping the Linux wheels drops what only they required.
        assert!(memo.keep_only(&files[2..]));
        assert_eq!(memo.requires(&None), BTreeSet::from(["numpy"]));
        assert!(!memo.keep_only(&files[2..]));
    }

    #[test]
    fn targets_drop_requirements_that_cannot_apply() {
        let linux = Targets::new(Some(|t: &str| t.contains("linux")), Some((3, 9)));
        let any = Targets::new(None::<fn(&str) -> bool>, None);
        let cases = [
            // marker, on a Linux mirror with a 3.9 floor, anywhere
            ("", true, true),
            ("sys_platform == \"win32\"", false, true),
            ("sys_platform != 'win32'", true, true),
            (
                "platform_system == \"Darwin\" or os_name == \"nt\"",
                false,
                true,
            ),
            (
                "'Linux' == platform_system and platform_machine == 'x86_64'",
                true,
                true,
            ),
            // No Linux machine is called arm64.
            (
                "sys_platform == 'linux' and platform_machine == 'arm64'",
                false,
                true,
            ),
            // A platform the table doesn't know may still be targeted.
            ("platform_machine == 'riscv64'", true, true),
            ("sys_platform == 'emscripten'", true, true),
            ("python_version < \"3.8\"", false, true),
            ("python_version < \"3.10\"", true, true),
            ("python_full_version <= '3.8.10'", false, true),
            ("python_version == '3.8.*'", false, true),
            ("'3.9' > python_version", false, true),
            ("python_version >= '3.12'", true, true),
            (
                "python_version < '3.8' or sys_platform == 'linux'",
                true,
                true,
            ),
            (
                "(python_version < '3.8' or os_name == 'nt') and extra == 'x'",
                false,
                true,
            ),
            ("implementation_name == 'pypy'", true, true),
            ("extra == 'x' and sys_platform == 'win32'", false, true),
            // Unparseable markers are followed.
            ("sys_platform == 'win32' and (", true, true),
            ("sys_platform ==", true, true),
        ];
        for (marker, on_linux, anywhere) in cases {
            assert_eq!(linux.admits(marker), on_linux, "{marker}");
            assert_eq!(any.admits(marker), anywhere, "{marker}");
        }
        // Mac arm64 only: `arm64` is the machine there.
        let arm_mac = Targets::new(Some(|t: &str| t.ends_with("macosx_14_0_arm64")), None);
        assert!(arm_mac.admits("platform_machine == 'arm64'"));
        assert!(!arm_mac.admits("platform_machine == 'x86_64'"));
        // A filter keeping no known platform is pure Python: anywhere.
        let pure = Targets::new(Some(|t: &str| t == "any"), None);
        assert!(pure.admits("sys_platform == 'win32'"));
    }
}
