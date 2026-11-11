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

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(pypiron::app::cli_main());
    // Exit without waiting on the blocking pool. Dropping a runtime blocks until
    // every running `spawn_blocking` job returns, so a graceful `serve` stop that
    // landed mid-parse of the 30 MB advisory feed (seconds of CPU, far longer on
    // a slow box) sat there after the lease hand-off, defeating the bounded
    // drain. Everything that matters was awaited by `cli_main`; a job still
    // running here has no awaiter, and abandoning it is no different from the
    // async tasks the runtime already drops.
    runtime.shutdown_background();
    result
}
