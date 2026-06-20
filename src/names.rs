//! Package-name and filename parsing shared by the server, worker, and sync.

/// PEP 503 normalization: lowercase; replace runs of [-_.] with single '-'.
pub fn normalize_pkg_name(name: &str) -> String {
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

/// The source-dist extension family. Corpus fact: every file ever uploaded
/// to PyPI (17.1M) ends in one of eleven extensions; these five are the
/// sdist-shaped ones. Matching is case-insensitive for exactly one real
/// file's sake: `notebook-4.0.5.ZIP`.
const SDIST_EXTS: [&str; 5] = [".tar.gz", ".tar.bz2", ".tar.xz", ".zip", ".tgz"];

fn strip_sdist_ext(filename: &str) -> Option<&str> {
    let lower = filename.to_ascii_lowercase();
    SDIST_EXTS
        .iter()
        .find(|ext| lower.ends_with(*ext))
        .map(|ext| &filename[..filename.len() - ext.len()])
}

/// Split an sdist stem into (name, version) at the first '-' where the rest
/// parses as a PEP 440 version. Raced against last-dash and dash-before-digit
/// over all 7.04M real PyPI sdists (src/corpus_check.rs): this wins with
/// 99.54% version / 99.50% name accuracy; the residue is mostly spam whose
/// filename never contained the release version at all.
fn split_sdist_stem(stem: &str) -> Option<(&str, &str)> {
    stem.match_indices('-').find_map(|(i, _)| {
        let rest = &stem[i + 1..];
        rest.parse::<pep440_rs::Version>()
            .ok()
            .map(|_| (&stem[..i], rest))
    })
}

/// Extract the distribution name from a wheel/sdist filename, normalized.
/// Fallback only — uploads should use the form's `name` field.
pub fn infer_package_from_filename(filename: &str) -> String {
    let stem = filename.split('/').next_back().unwrap_or(filename);
    let dist = if let Some(sdist_stem) = strip_sdist_ext(stem) {
        // sdist names keep their dashes; a versionless stem is all name.
        split_sdist_stem(sdist_stem)
            .map(|(name, _)| name)
            .unwrap_or(sdist_stem)
    } else if let Some(idx) = stem.find('-') {
        // wheel/egg style: '-' in the distribution name is escaped to '_'.
        &stem[..idx]
    } else {
        stem
    };
    normalize_pkg_name(dist)
}

/// A well-formed PEP 503 normalized name: nonempty, only `[a-z0-9-]`.
/// Anything else (slashes, dots that survived, unicode) is hostile input —
/// normalized names are storage path segments.
pub fn is_normalized(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('-')
        && !name.ends_with('-')
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Normalize `raw` and return it only if the result is a usable PEP 503 name.
/// Pairs the two halves of the gate — normalize then [`is_normalized`] — so a
/// caller can't accidentally use a normalized-but-unvalidated name (which could
/// be empty or otherwise hostile) as a storage path segment. `None` means the
/// input is not servable.
pub fn checked_pkg_name(raw: &str) -> Option<String> {
    let name = normalize_pkg_name(raw);
    is_normalized(&name).then_some(name)
}

/// True if normalized `pkg` falls under normalized `prefix`: the prefix
/// itself or anything below it (`acme` matches `acme` and `acme-foo`,
/// never `acmefoo`). Cf. PEP 752 reserved namespaces.
pub fn matches_prefix(pkg: &str, prefix: &str) -> bool {
    pkg == prefix
        || pkg
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('-'))
}

/// Best-effort version extraction from an artifact filename.
/// Fallback only — sidecars carry the authoritative version. Legacy binary
/// formats (.egg/.exe/.msi/.rpm/.dmg/.deb, 0.86% of PyPI, frozen since ~2013)
/// are intentionally None: their version grammar is per-tool guesswork.
pub fn infer_version_from_filename(filename: &str) -> Option<String> {
    if let Some(stem) = filename.strip_suffix(".whl") {
        // PEP 427: distribution-version(-build)?-python-abi-platform.whl
        return stem.split('-').nth(1).map(str::to_string);
    }
    let stem = strip_sdist_ext(filename)?;
    split_sdist_stem(stem).map(|(_, v)| v.to_string())
}

/// Order two version strings newest-first by PEP 440 semantics (so `1.10` sorts
/// above `1.9`). When either side isn't valid PEP 440 — legacy or malformed —
/// fall back to a reversed lexical compare so the list still sorts
/// deterministically. Used by the project page's release history.
pub fn version_cmp_desc(a: &str, b: &str) -> std::cmp::Ordering {
    match (
        a.parse::<pep440_rs::Version>(),
        b.parse::<pep440_rs::Version>(),
    ) {
        (Ok(va), Ok(vb)) => vb.cmp(&va),
        // A parseable version outranks an unparseable one; otherwise lexical.
        (Ok(_), Err(_)) => std::cmp::Ordering::Less,
        (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
        (Err(_), Err(_)) => b.cmp(a),
    }
}

/// The PEP 427 compatibility tags of a wheel: the dot-separated alternatives in
/// each of the python / abi / platform fields.
#[derive(Debug, Clone)]
pub struct WheelTags {
    pub python: Vec<String>,
    pub abi: Vec<String>,
    pub platform: Vec<String>,
}

/// Parse a wheel filename into its (python, abi, platform) tags — the last three
/// dash-separated fields before `.whl` per PEP 427. `None` for anything that
/// isn't a wheel or is missing fields. The filename is attacker/upstream
/// controlled (an upload form field or a mirrored artifact name), so this never
/// panics: the `< 5` guard makes the `len() - 3/2/1` indexing total.
pub fn parse_wheel_tags(filename: &str) -> Option<WheelTags> {
    let stem = filename.strip_suffix(".whl")?;
    let parts: Vec<&str> = stem.split('-').collect();
    if parts.len() < 5 {
        // name, version, [build?], py, abi, platform  -> min 5 fields (without build)
        return None;
    }
    let dotted = |field: &str| field.split('.').map(str::to_string).collect::<Vec<_>>();
    Some(WheelTags {
        python: dotted(parts[parts.len() - 3]),
        abi: dotted(parts[parts.len() - 2]),
        platform: dotted(parts[parts.len() - 1]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_pep503() {
        assert_eq!(normalize_pkg_name("Foo.Bar_baz"), "foo-bar-baz");
        assert_eq!(normalize_pkg_name("six"), "six");
        assert_eq!(normalize_pkg_name("A--B"), "a-b");
    }

    #[test]
    fn version_cmp_desc_orders_newest_first() {
        use std::cmp::Ordering;
        // PEP 440, not lexical: 1.10 is newer than 1.9.
        assert_eq!(version_cmp_desc("1.10.0", "1.9.0"), Ordering::Less);
        assert_eq!(version_cmp_desc("2.0.0", "2.0.0"), Ordering::Equal);
        assert_eq!(version_cmp_desc("15.0.0", "14.3.0"), Ordering::Less);
        // 2.0.0a1 is a pre-release of 2.0.0, so it outranks every 1.x.
        let mut v = vec!["1.9.0", "1.10.0", "1.2.0", "2.0.0a1"];
        v.sort_by(|a, b| version_cmp_desc(a, b));
        assert_eq!(v, vec!["2.0.0a1", "1.10.0", "1.9.0", "1.2.0"]);
        // Parseable outranks unparseable; junk still sorts deterministically.
        assert_eq!(version_cmp_desc("1.0.0", "not-a-version"), Ordering::Less);
    }

    // Edge-case filenames below are real PyPI uploads, found by running the
    // parsers over every file ever uploaded (src/corpus_check.rs).

    #[test]
    fn infers_package_from_wheel() {
        assert_eq!(
            infer_package_from_filename("six-1.16.0-py2.py3-none-any.whl"),
            "six"
        );
    }

    #[test]
    fn infers_multi_dash_sdist_names() {
        // 24% of all sdists have '-' in the name; first-dash split got these wrong.
        assert_eq!(
            infer_package_from_filename("0-core-client-1.1.0a3.tar.gz"),
            "0-core-client"
        );
        assert_eq!(
            infer_package_from_filename("django-debug-toolbar-1.0.tar.gz"),
            "django-debug-toolbar"
        );
        assert_eq!(
            infer_package_from_filename("zope.interface-5.4.0.tar.gz"),
            "zope-interface"
        );
    }

    #[test]
    fn sdist_split_survives_dashes_in_versions() {
        // Pre-release and post-release separators are valid PEP 440.
        assert_eq!(
            infer_package_from_filename("Pootle-2.0.0-rc2.tar.gz"),
            "pootle"
        );
        assert_eq!(
            infer_version_from_filename("Pootle-2.0.0-rc2.tar.gz").as_deref(),
            Some("2.0.0-rc2")
        );
        assert_eq!(
            infer_version_from_filename("0-orchestrator-1.1.0-alpha-7-1.tar.gz").as_deref(),
            Some("1.1.0-alpha-7-1")
        );
    }

    #[test]
    fn sdist_split_survives_digit_leading_name_segments() {
        // UUID-shaped project name: every segment is a tempting version start.
        let f = "01d61084-d29e-11e9-96d1-7c5cf84ffe8e-0.1.0.tar.gz";
        assert_eq!(
            infer_package_from_filename(f),
            "01d61084-d29e-11e9-96d1-7c5cf84ffe8e"
        );
        assert_eq!(infer_version_from_filename(f).as_deref(), Some("0.1.0"));
    }

    #[test]
    fn versionless_sdist_is_all_name() {
        assert_eq!(
            infer_package_from_filename("AccordionWidget.tar.bz2"),
            "accordionwidget"
        );
        assert_eq!(infer_version_from_filename("AccordionWidget.tar.bz2"), None);
        // Spam-era names whose version never made it into the filename.
        let f = "007-no-time-to-die-2021-watch-full-online-free.tar.gz";
        assert_eq!(
            infer_package_from_filename(f),
            "007-no-time-to-die-2021-watch-full-online-free"
        );
        assert_eq!(infer_version_from_filename(f), None);
    }

    #[test]
    fn handles_tgz_and_uppercase_extensions() {
        assert_eq!(
            infer_version_from_filename("clustershell-1.2.84.tgz").as_deref(),
            Some("1.2.84")
        );
        // The one uppercase-extension file in PyPI history.
        assert_eq!(
            infer_package_from_filename("notebook-4.0.5.ZIP"),
            "notebook"
        );
        assert_eq!(
            infer_version_from_filename("notebook-4.0.5.ZIP").as_deref(),
            Some("4.0.5")
        );
    }

    #[test]
    fn legacy_binary_formats_have_no_inferred_version() {
        // .egg/.exe/.msi/.rpm/.dmg/.deb: 0.86% of PyPI, frozen since ~2013.
        for f in [
            "102003634-0.0.2-py3.11.egg",
            "4Suite-XML-1.0.1.win32-py2.2.exe",
            "Cheetah-2.2.2-1.src.rpm",
            "Aglyph-2.1.1.win32.msi",
        ] {
            assert_eq!(infer_version_from_filename(f), None, "{f}");
        }
        // Name inference still works for the dominant first-dash shapes.
        assert_eq!(
            infer_package_from_filename("102003634-0.0.2-py3.11.egg"),
            "102003634"
        );
    }

    #[test]
    fn wheel_version_slot_is_taken_verbatim() {
        // Build tag is skipped (PEP 427 field order)…
        assert_eq!(
            infer_version_from_filename("demo-1.0-1-py3-none-any.whl").as_deref(),
            Some("1.0")
        );
        // …local versions ride along…
        assert_eq!(
            infer_version_from_filename("Adeepspeed-0.9.2+torch1.12-py3-none-any.whl").as_deref(),
            Some("0.9.2+torch1.12")
        );
        // …and a malformed wheel with no version yields its python tag —
        // garbage in, garbage out, but never a panic (real upload).
        assert_eq!(
            infer_version_from_filename("BloxFlip-py3-none-any.whl").as_deref(),
            Some("py3")
        );
    }

    #[test]
    fn validates_normalized_names() {
        assert!(is_normalized("six"));
        assert!(is_normalized("acme-foo2"));
        assert!(!is_normalized(""));
        assert!(!is_normalized("foo/bar"));
        assert!(!is_normalized("Foo"));
        assert!(!is_normalized("foo..bar"));
        assert!(!is_normalized("-foo"));
    }

    #[test]
    fn checked_pkg_name_normalizes_then_validates() {
        assert_eq!(
            checked_pkg_name("Foo.Bar_baz").as_deref(),
            Some("foo-bar-baz")
        );
        assert_eq!(checked_pkg_name("  six "), None); // spaces aren't separators; caller must trim
        assert_eq!(checked_pkg_name("..."), None); // normalizes to empty
        assert_eq!(checked_pkg_name("foo/bar"), None); // slash survives normalization
    }

    #[test]
    fn prefix_matching_is_namespace_shaped() {
        assert!(matches_prefix("acme", "acme"));
        assert!(matches_prefix("acme-foo", "acme"));
        assert!(matches_prefix("acme-foo-bar", "acme"));
        assert!(!matches_prefix("acmefoo", "acme"));
        assert!(!matches_prefix("other", "acme"));
    }

    #[test]
    fn infers_wheel_version() {
        assert_eq!(
            infer_version_from_filename("six-1.16.0-py2.py3-none-any.whl"),
            Some("1.16.0".to_string())
        );
    }

    #[test]
    fn infers_sdist_version() {
        assert_eq!(
            infer_version_from_filename("six-1.16.0.tar.gz"),
            Some("1.16.0".to_string())
        );
    }

    #[test]
    fn wheel_tags_parse_and_reject_real_shapes() {
        // Standard, compound python tag, and build-tag wheels parse.
        let t = parse_wheel_tags("six-1.16.0-py2.py3-none-any.whl").unwrap();
        assert_eq!(t.python, ["py2", "py3"]);
        assert_eq!(t.abi, ["none"]);
        assert_eq!(t.platform, ["any"]);
        let t = parse_wheel_tags("demo-1.0-1-cp311-cp311-manylinux_2_17_x86_64.whl").unwrap();
        assert_eq!(t.python, ["cp311"]);

        // Real malformed uploads (126 of 9.94M wheels): missing version or
        // tag fields. Must be None, not a panic or a bogus parse.
        assert!(parse_wheel_tags("JHVIT-0.0.1-py3-any.whl").is_none());
        assert!(parse_wheel_tags("CLUEstering-1.0.2-none-any.whl").is_none());
        assert!(parse_wheel_tags("GoldenFace1.1-py3-none-any.whl").is_none());
        assert!(parse_wheel_tags("not-a-wheel-1.0.tar.gz").is_none());
    }
}
