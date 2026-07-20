//! PypIron as a library: the crate root for every module, so integration
//! tests (`tests/*.rs`), the stateright protocol models, and the deterministic
//! simulator (`examples/vopr.rs`) drive the exact code the shipped binary
//! runs. The binary itself is a thin `src/main.rs` over [`app::cli_main`].
//!
//! This is not a public API — it is the server's own internals made linkable
//! for verification. Nothing here is semver-stable.

pub mod advisories;
pub mod app;
pub mod bucket_health;
pub mod buckets;
pub mod cache;
pub mod cli;
pub mod clock;
pub mod config;
pub mod coremeta;
#[cfg(test)]
mod corpus_check;
pub mod counters;
pub mod hash;
pub mod lease;
pub mod markdown;
pub mod markers;
pub mod metrics;
pub mod names;
pub mod node_region;
pub mod observed_storage;
pub mod origin;
pub mod pages;
pub mod project_cache;
pub mod provenance;
pub mod proxy;
pub mod range;
pub mod render;
pub mod replicate;
pub mod sidecar;
pub mod sim;
pub mod simple;
pub mod ssrf;
pub mod status;
pub mod storage;
pub mod sync;
pub mod token;
pub mod tombstone;
pub mod transparency;
pub mod upload;
pub mod verify;
pub mod web;
pub mod wheel;
pub mod worker;

// Keep every historical `crate::X` path valid: the items that lived at the
// crate root when `main.rs` was the root (AppState, the storage-layout
// prefixes, shared helpers) are re-exported here. The CLI surface moved to
// `cli` but keeps its crate-flat paths the same way.
pub use app::*;
pub use cli::*;
pub use pages::*;
