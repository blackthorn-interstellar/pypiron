//! PypIron as a library: the crate root for every module, so integration
//! tests (`tests/*.rs`), the stateright protocol models, and the deterministic
//! simulator (`examples/vopr.rs`) drive the exact code the shipped binary
//! runs. The binary itself is a thin `src/main.rs` over [`app::cli_main`].
//!
//! This is not a public API — it is the server's own internals made linkable
//! for verification. Nothing here is semver-stable.

pub mod admin;
pub mod advisories;
pub mod app;
pub mod auth;
pub mod bucket_health;
pub mod buckets;
pub mod cache;
pub mod cli;
pub mod clock;
pub mod config;
pub mod coremeta;
#[cfg(test)]
mod corpus_check;
pub mod counted_storage;
pub mod counters;
pub mod format;
pub mod hash;
pub mod html;
pub mod layout;
pub mod lease;
pub mod markdown;
pub mod markers;
pub mod metrics;
pub mod names;
pub mod node_region;
pub mod observed_storage;
pub mod origin;
pub mod osv;
pub mod pages;
pub mod project_cache;
pub mod provenance;
pub mod proxy;
pub mod publish;
pub mod range;
pub mod render;
pub mod replicate;
pub mod reqsign;
pub mod serve;
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
pub mod upstream_tls;
pub mod verify;
pub mod wheel;
pub mod worker;

// `AppState` and the record write path are re-exported at the crate root only
// because out-of-tree consumers reach them over `pypiron::X`: the integration
// tests (`tests/*.rs`) and the deterministic simulator (`examples/vopr.rs`).
// Everything internal resolves through its owning module (`crate::app::X`,
// `crate::publish::X`, …) — there is no flat-path shim for sibling modules.
pub use app::AppState;
pub use publish::{delete_record, publish_record, PublishBody, PublishRequest};
