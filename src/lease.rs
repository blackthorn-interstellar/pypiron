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

        // Expired — or impossibly far in the future: a clock-skewed holder
        // that died would otherwise leave an unstealabe lease and a silent
        // leadership vacuum. Anything past now + 3×ttl is bogus.
        if now > current.expires_at || current.expires_at > now + 3 * self.ttl_secs {
            let stolen = storage
                .put_if_match(LEASE_KEY, &etag, self.lease_json(current.term + 1, now))
                .await?
                .is_some();
            if stolen {
                info!(holder=%self.holder, previous=%current.holder, term = current.term + 1, "lease stolen (expired or bogus expiry)");
            }
            return Ok(stolen);
        }

        Ok(false)
    }

    /// Best-effort release on graceful shutdown: delete the lease if we still
    /// hold it, so a restarted (or replacement) node becomes leader on its
    /// next tick instead of waiting out the TTL — without this, every restart
    /// was a TTL-long window where uploads went unprocessed. The read-then-
    /// delete race (a peer steals between the two) merely deletes the peer's
    /// fresh lease; it re-creates it next tick. Sloppy by design.
    pub async fn release(&self) {
        let held = match self.storage.get_with_etag(LEASE_KEY).await {
            Ok(Some((bytes, _))) => serde_json::from_slice::<Lease>(&bytes)
                .map(|l| l.holder == self.holder)
                .unwrap_or(false),
            _ => false,
        };
        if held {
            match self.storage.delete_keys(&[LEASE_KEY.to_string()]).await {
                Ok(()) => info!(holder=%self.holder, "lease released on shutdown"),
                Err(e) => warn!(error=?e, "failed to release lease on shutdown"),
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;
    use std::time::Duration;

    #[tokio::test]
    async fn release_deletes_own_lease_only() {
        let storage = Arc::new(InMemStorage::default());
        let lm = LeaseManager::new(storage.clone(), Duration::from_secs(30));
        assert!(lm.is_leader().await, "first node acquires the lease");
        lm.release().await;
        assert!(
            storage.get_bytes(LEASE_KEY).await.is_err(),
            "own lease must be deleted on release"
        );

        // A foreign lease survives someone else's release.
        let other = LeaseManager::new(storage.clone(), Duration::from_secs(30));
        assert!(other.is_leader().await);
        let late = LeaseManager::new(storage.clone(), Duration::from_secs(30));
        assert!(!late.is_leader().await, "lease is held by `other`");
        late.release().await;
        assert!(
            storage.get_bytes(LEASE_KEY).await.is_ok(),
            "a non-holder's release must not delete the lease"
        );
    }

    #[tokio::test]
    async fn released_lease_is_acquired_immediately_not_after_ttl() {
        let storage = Arc::new(InMemStorage::default());
        let a = LeaseManager::new(storage.clone(), Duration::from_secs(3600));
        assert!(a.is_leader().await);
        a.release().await;
        // TTL is an hour; without release the successor would wait it out.
        let b = LeaseManager::new(storage.clone(), Duration::from_secs(3600));
        assert!(
            b.is_leader().await,
            "successor must acquire instantly after a graceful release"
        );
    }
}
