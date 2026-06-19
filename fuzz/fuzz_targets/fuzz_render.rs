//! Fuzz the index renderers in `src/render.rs`.
//!
//! Indexes are built from sidecar fields (filename, sha256, version, yanked
//! reason, requires-python). Several of those are attacker-influenced and land
//! inside double-quoted HTML attributes (`href="/files/.../<filename>#sha256=<h>"`).
//! Two properties, checked over arbitrary field values:
//!
//!   1. The PEP 691 JSON is always valid JSON.
//!   2. The per-package HTML's `href` attribute is quote-safe: decoding the
//!      attribute value recovers exactly the intended URL. If any interpolated
//!      field smuggles a raw `"` into the attribute, the value gets truncated
//!      and this fails — i.e. this catches HTML-attribute injection.
#![no_main]
#![allow(dead_code)]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

// render.rs reaches for `crate::names`, `crate::sidecar`, and
// `crate::status::ProjectStatusDoc`; provide them at the binary root so those
// absolute paths resolve, then pull render in beside them.
#[path = "../../src/names.rs"]
mod names;
#[path = "../../src/sidecar.rs"]
mod sidecar;
#[path = "../../src/render.rs"]
mod render;

// The real `status.rs` drags in the storage/anyhow stack (S3, axum, tokio) for
// its sidecar read/write helpers, which would pull the whole crate into this
// otherwise-minimal harness. render only touches the pure PEP 792 status types,
// so mirror just those here. Any drift in their shape or serde shows up as a
// compile error in this exact fuzz-build job.
mod status {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum ProjectStatus {
        Active,
        Archived,
        Quarantined,
        Deprecated,
    }

    impl ProjectStatus {
        pub fn as_str(self) -> &'static str {
            match self {
                ProjectStatus::Active => "active",
                ProjectStatus::Archived => "archived",
                ProjectStatus::Quarantined => "quarantined",
                ProjectStatus::Deprecated => "deprecated",
            }
        }

        pub fn is_active(self) -> bool {
            matches!(self, ProjectStatus::Active)
        }

        pub fn blocks_downloads(self) -> bool {
            matches!(self, ProjectStatus::Quarantined)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProjectStatusDoc {
        pub status: ProjectStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub reason: Option<String>,
    }

    impl Default for ProjectStatusDoc {
        fn default() -> Self {
            Self {
                status: ProjectStatus::Active,
                reason: None,
            }
        }
    }
}

use render::FileMetadata;
use sidecar::Yanked;
use status::{ProjectStatus, ProjectStatusDoc};

fn carve(u: &mut Unstructured) -> (String, FileMetadata) {
    let pkg = String::arbitrary(u).unwrap_or_default();
    let yanked = match u8::arbitrary(u).unwrap_or(0) % 3 {
        0 => Yanked::Flag(false),
        1 => Yanked::Flag(true),
        _ => Yanked::Reason(String::arbitrary(u).unwrap_or_default()),
    };
    let fm = FileMetadata {
        filename: String::arbitrary(u).unwrap_or_default(),
        sha256: String::arbitrary(u).unwrap_or_default(),
        size: u64::arbitrary(u).unwrap_or(0),
        upload_time: Option::<String>::arbitrary(u).unwrap_or(None),
        version: Option::<String>::arbitrary(u).unwrap_or(None),
        yanked,
        requires_python: Option::<String>::arbitrary(u).unwrap_or(None),
        core_metadata: bool::arbitrary(u).unwrap_or(false),
        provenance: bool::arbitrary(u).unwrap_or(false),
    };
    (pkg, fm)
}

// PEP 792 status doc — its `reason` is arbitrary text that lands inside a
// double-quoted HTML attribute, so it's an injection surface worth fuzzing.
fn carve_status(u: &mut Unstructured) -> ProjectStatusDoc {
    let status = match u8::arbitrary(u).unwrap_or(0) % 4 {
        0 => ProjectStatus::Active,
        1 => ProjectStatus::Archived,
        2 => ProjectStatus::Quarantined,
        _ => ProjectStatus::Deprecated,
    };
    let reason = Option::<String>::arbitrary(u).unwrap_or(None);
    ProjectStatusDoc { status, reason }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let (pkg, fm) = carve(&mut u);
    let status = carve_status(&mut u);
    let files = std::slice::from_ref(&fm);

    // 1. PEP 691 JSON must always parse.
    let json = render::pep691_package_json(&pkg, files, &status);
    serde_json::from_str::<serde_json::Value>(&json).expect("PEP 691 JSON is not valid JSON");

    // 2. The HTML `href` must be quote-safe and lossless.
    let html = render::pep503_package_html(&pkg, files, &status);
    if let Some(start) = html.find("href=\"/files/") {
        let after = &html[start + "href=\"".len()..];
        if let Some(end) = after.find('"') {
            let raw_attr = &after[..end];
            let decoded = html_escape::decode_html_entities(raw_attr);
            let intended = format!("/files/{}/{}#sha256={}", pkg, fm.filename, fm.sha256);
            assert_eq!(
                decoded, intended,
                "href attribute truncated/altered — HTML-attribute injection via an interpolated field"
            );
        }
    }
});
