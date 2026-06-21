//! Bake the git commit into the binary as `PYPIRON_GIT_HASH`.
//!
//! Order of truth: an explicit `PYPIRON_GIT_HASH` env var (set by the Docker
//! build, where `.git` is excluded from the context) wins; otherwise we ask git
//! directly (the common case for local and CI wheel builds, which have `.git`);
//! failing both (e.g. an sdist build from an extracted tarball) we record
//! `unknown`.
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=PYPIRON_GIT_HASH");

    let hash = match std::env::var("PYPIRON_GIT_HASH") {
        Ok(h) if !h.trim().is_empty() => h.trim().to_string(),
        _ => git_hash().unwrap_or_else(|| "unknown".to_string()),
    };
    println!("cargo:rustc-env=PYPIRON_GIT_HASH={hash}");
}

/// `<short-sha>` (plus `-dirty` for a modified tree), or `None` without git.
fn git_hash() -> Option<String> {
    // Rebuild when the checked-out commit moves or the index changes.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    let out = Command::new("git")
        .args(["describe", "--always", "--dirty", "--exclude=*"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let hash = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!hash.is_empty()).then_some(hash)
}
