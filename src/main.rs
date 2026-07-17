//! Thin binary entry point; everything lives in the library (`src/lib.rs`)
//! so tests, the protocol models, and the simulator link the real code.

/// The request path allocates many small, short-lived objects per request
/// (header values, the index-cache key, axum/tower's per-request service
/// clones). The platform allocator was ~17% of on-CPU time under index-read
/// load; mimalloc's thread-local free lists cut that contention. Gated off
/// s390x/ppc64le, whose old manylinux cross-GCC rejects a libmimalloc-sys build
/// flag; those niche arches fall back to the system allocator (see Cargo.toml).
#[cfg(not(any(target_arch = "s390x", target_arch = "powerpc64")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pypiron::app::cli_main().await
}
