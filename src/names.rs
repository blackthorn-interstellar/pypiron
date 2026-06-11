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

/// Extract the distribution name from a wheel/sdist filename, normalized.
/// Fallback only — uploads should use the form's `name` field.
pub fn infer_package_from_filename(filename: &str) -> String {
    let stem = filename.split('/').next_back().unwrap_or(filename);
    let dist = if let Some(idx) = stem.find('-') {
        &stem[..idx]
    } else {
        stem
    };
    normalize_pkg_name(dist)
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
/// Fallback only — sidecars carry the authoritative version.
pub fn infer_version_from_filename(filename: &str) -> Option<String> {
    if let Some(stem) = filename.strip_suffix(".whl") {
        // PEP 427: distribution-version(-build)?-python-abi-platform.whl
        return stem.split('-').nth(1).map(str::to_string);
    }
    let stem = filename
        .strip_suffix(".tar.gz")
        .or_else(|| filename.strip_suffix(".tar.bz2"))
        .or_else(|| filename.strip_suffix(".tar.xz"))
        .or_else(|| filename.strip_suffix(".zip"))?;
    stem.rsplit_once('-').map(|(_, v)| v.to_string())
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
    fn infers_package_from_wheel() {
        assert_eq!(
            infer_package_from_filename("six-1.16.0-py2.py3-none-any.whl"),
            "six"
        );
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
}
