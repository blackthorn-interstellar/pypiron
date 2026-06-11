//! Sloppy leader election: an S3 conditional-write lease with TTL and
//! heartbeat. Rebuilds are idempotent, so dual leadership for a few seconds
//! merely duplicates work — this is a cost optimization, never a correctness
//! requirement. No Raft, no fencing.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{debug, info, warn};

use crate::storage::Storage;

pub const LEASE_KEY: &str = "_leader/lease.json";

#[derive(Debug, Serialize, Deserialize)]
struct Lease {
    holder: String,
    term: u64,
    #[serde(rename = "expires-at")]
    expires_at: i64,
}

pub struct LeaseManager {
    storage: Arc<dyn Storage>,
    holder: String,
    ttl_secs: i64,
}

impl LeaseManager {
    pub fn new(storage: Arc<dyn Storage>, ttl: Duration) -> Self {
        let holder = format!(
            "{}-{}",
            std::process::id(),
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        );
        Self {
            storage,
            holder,
            ttl_secs: ttl.as_secs().max(1) as i64,
        }
    }

    /// Acquire, renew, or steal the lease; heartbeat by calling every tick.
    /// Any failure path simply reports "not leader" — sloppy by design.
    pub async fn is_leader(&self) -> bool {
        match self.try_hold().await {
            Ok(held) => held,
            Err(e) => {
                warn!(error=?e, "lease check failed; assuming follower");
                false
            }
        }
    }

    async fn try_hold(&self) -> anyhow::Result<bool> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let storage = self.storage.as_ref();

        let Some((bytes, etag)) = storage.get_with_etag(LEASE_KEY).await? else {
            // No lease yet: create-if-absent.
            let won = storage
                .put_if_none_match(LEASE_KEY, self.lease_json(1, now))
                .await?
                .is_some();
            if won {
                info!(holder=%self.holder, "lease acquired");
            }
            return Ok(won);
        };

        let current: Lease = match serde_json::from_slice(&bytes) {
            Ok(lease) => lease,
            Err(_) => Lease {
                // Corrupt lease: treat as expired and steal over its ETag.
                holder: String::new(),
                term: 0,
                expires_at: 0,
            },
        };

        if current.holder == self.holder {
            // Renew our own lease.
            let renewed = storage
                .put_if_match(LEASE_KEY, &etag, self.lease_json(current.term, now))
                .await?
                .is_some();
            if !renewed {
                debug!("lost lease renewal race");
            }
            return Ok(renewed);
        }

        if now > current.expires_at {
            let stolen = storage
                .put_if_match(LEASE_KEY, &etag, self.lease_json(current.term + 1, now))
                .await?
                .is_some();
            if stolen {
                info!(holder=%self.holder, previous=%current.holder, term = current.term + 1, "lease stolen (expired)");
            }
            return Ok(stolen);
        }

        Ok(false)
    }

    fn lease_json(&self, term: u64, now: i64) -> Vec<u8> {
        serde_json::to_vec(&Lease {
            holder: self.holder.clone(),
            term,
            expires_at: now + self.ttl_secs,
        })
        .unwrap_or_default()
    }
}
