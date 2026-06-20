//! Parse a PEP 740 provenance object far enough to name the *publisher* the
//! upstream index (PyPI) verified when it accepted the artifact.
//!
//! pypiron does NOT verify attestations — it relays them (a `.provenance`
//! companion only ever exists on mirror-origin files; first-party attestations
//! are refused fail-closed). The `publisher` object inside a bundle is generated
//! by the publishing index and records the Trusted Publisher *it* verified. We
//! surface it on the human page, attributed to the publishing index and linked
//! to the raw `.provenance` for independent re-verification — never claiming
//! pypiron did the cryptography. Pure, best-effort: any malformed input yields
//! `None` and the page simply renders without a "Verified details" section.

use serde_json::Value;

/// The verified publishing identity drawn from a provenance attestation bundle.
#[derive(Debug, Clone, PartialEq)]
pub struct Publisher {
    /// Publisher kind, e.g. `GitHub`, `GitLab`, `Google`, `ActiveState`.
    pub kind: String,
    /// `owner/repo` for repo-backed publishers, when present.
    pub repository: Option<String>,
    /// Publishing workflow / job reference, when present.
    pub workflow: Option<String>,
    /// Deployment environment, when present.
    pub environment: Option<String>,
}

/// Parse provenance JSON bytes, returning the first bundle's publisher if one
/// with a non-empty `kind` is present.
pub fn parse_publisher(bytes: &[u8]) -> Option<Publisher> {
    let v: Value = serde_json::from_slice(bytes).ok()?;
    let bundles = v.get("attestation_bundles")?.as_array()?;
    let pubv = bundles
        .iter()
        .filter_map(|b| b.get("publisher"))
        .find(|p| get_str(p, "kind").is_some())?;
    let kind = get_str(pubv, "kind")?;
    // PyPI carries publisher-specific fields at the top level of `publisher`;
    // older/minimal forms nest them under `claims`. Check both.
    let field = |name: &str| {
        get_str(pubv, name).or_else(|| pubv.get("claims").and_then(|c| get_str(c, name)))
    };
    Some(Publisher {
        repository: field("repository"),
        workflow: field("workflow").or_else(|| field("workflow_ref")),
        environment: field("environment"),
        kind,
    })
}

fn get_str(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_github_style_publisher() {
        let json = br#"{"version":1,"attestation_bundles":[{"publisher":
            {"kind":"GitHub","repository":"brycedrennan/imaginAIry",
             "workflow":"release.yml","environment":"pypi"},"attestations":[]}]}"#;
        let p = parse_publisher(json).unwrap();
        assert_eq!(p.kind, "GitHub");
        assert_eq!(p.repository.as_deref(), Some("brycedrennan/imaginAIry"));
        assert_eq!(p.workflow.as_deref(), Some("release.yml"));
        assert_eq!(p.environment.as_deref(), Some("pypi"));
    }

    #[test]
    fn reads_fields_nested_under_claims() {
        let json = br#"{"attestation_bundles":[{"publisher":
            {"kind":"GitLab","claims":{"repository":"grp/proj","workflow_ref":"ci.yml"}}}]}"#;
        let p = parse_publisher(json).unwrap();
        assert_eq!(p.kind, "GitLab");
        assert_eq!(p.repository.as_deref(), Some("grp/proj"));
        assert_eq!(p.workflow.as_deref(), Some("ci.yml"));
        assert_eq!(p.environment, None);
    }

    #[test]
    fn minimal_publisher_keeps_only_kind() {
        let json = br#"{"attestation_bundles":[{"publisher":{"kind":"pytest","claims":{}},"attestations":[]}]}"#;
        let p = parse_publisher(json).unwrap();
        assert_eq!(p.kind, "pytest");
        assert_eq!(p.repository, None);
    }

    #[test]
    fn malformed_or_empty_yields_none() {
        assert_eq!(parse_publisher(b""), None);
        assert_eq!(parse_publisher(b"not json"), None);
        assert_eq!(parse_publisher(b"{}"), None);
        assert_eq!(parse_publisher(br#"{"attestation_bundles":[]}"#), None);
        // publisher present but kind blank → not usable
        assert_eq!(
            parse_publisher(br#"{"attestation_bundles":[{"publisher":{"kind":"  "}}]}"#),
            None
        );
    }
}
