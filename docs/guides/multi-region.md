# Survive a region or cloud outage

Run one index across regions — or across clouds — and keep serving when one goes
down. Give every node the same ordered list of buckets. Each upload lands on all
of them before it returns `200`, so any single bucket can serve the whole index
on its own. Label the buckets by region and each node reads from the one in its
own region — in-region latency, no cross-region egress.

Failover is automatic. No promotion command, no DNS surgery.

## Deploy across two regions

One bucket per region, one or more nodes per region, and the **same ordered
bucket list** on every node. Order is preference: nodes read from the first
healthy bucket.

The same `pypiron.toml` in every region:

```toml
[serve]
bind-addr = "0.0.0.0:8080"
buckets = ["s3://iron-a@us-east-1", "s3://iron-b@eu-west-1"]
```

Or one environment variable, again identical everywhere:

```bash
export PYPIRON_BUCKETS=s3://iron-a@us-east-1,s3://iron-b@eu-west-1
```

Put a health-checked load balancer or failover DNS in front of the nodes, keyed
on `/health`. Every region can serve the full index; the front door picks which
region a client reaches.

## What survives

- **A region outage.** A node that sees three straight failures on a bucket
  switches to the next healthy one within seconds. Every bucket already holds
  every file, so there is no catch-up.
- **A whole cloud outage**, when the buckets span two clouds (below).
- **Acknowledged uploads.** A publish returns `200` only once the file is on
  every reachable bucket — two buckets, two copies, before the client hears
  success. If one bucket was down, pypiron copies the file over when it returns.
- **Deletes.** A deleted filename never comes back, even if a bucket was
  unreachable when you deleted it.

## During an outage

You do nothing.

1. A bucket starts failing. After three consecutive failures, each node stops
   using it and serves from the next one in the list.
2. `/health` fails on nodes that can no longer reach any bucket, and your load
   balancer routes clients to a surviving region. Installs continue.
3. Uploads keep succeeding on the remaining buckets, and pypiron queues a repair
   for the one that is down.
4. When the bucket recovers, the repairs drain — immediately on recovery, with a
   periodic sweep as a backstop and a full comparison daily and at boot. Nodes
   return to a more-preferred bucket only after five minutes of continuous
   health, so a flapping region does not bounce traffic back and forth.

Timeouts, connection failures, and 5xx responses count as an outage.
Credential, permission, KMS, quota, and configuration errors do not — they raise
alarms instead, so a misconfigured node cannot flee a healthy region.

## Span two clouds

Mix backends in the list:

```toml
buckets = ["s3://iron-a@us-east-1", "gs://iron-b"]
```

Losing all of AWS, or all of Google, is then one more bucket going away. Each
bucket authenticates with its own cloud's native credentials. Cross-cloud
replication pays egress on upload — cents a gigabyte, noise at real publish
rates.

## Reads stay in their region

Label each bucket with its region and every node serves reads — index pages and
downloads — from the bucket in its own region. Installs pull bytes from nearby,
and steady-state cross-region egress drops to zero. Writes don't change: every
upload still lands on the preferred bucket and fans out to the rest before the
client hears `200`.

```toml
buckets = ["s3://iron-east@us-east-1", "s3://iron-west@us-west-2"]
```

That label is the only new configuration, and it's identical on every node. Each
node detects its own region at boot from its cloud's instance metadata (AWS, GCP,
Azure) and reads from the matching bucket. A node that detects no region, or
matches no labeled bucket, reads from the preferred bucket exactly as before.
On-prem or MinIO fleets have no metadata to read — name the region yourself:

```bash
export PYPIRON_NODE_REGION=dc-east
```

A wrong region only ever costs latency, never correctness: when in doubt, a node
reads from the preferred bucket.

**A local read never returns less than the preferred bucket would.**

- An acked file never 404s. If a new release hasn't reached the local bucket
  yet, the node reads through to the preferred bucket and serves it.
- A brand-new package installs everywhere the moment it's published — the same
  read-through covers its first release.
- A private name never falls through to the public proxy. Whether a name is
  yours is decided on the preferred bucket, so a lagging local bucket can't serve
  a public package in a private name's place.
- A new release of an existing package can take a few seconds to appear in a
  remote region's index. The file installs the whole time; only the listing
  lags.
- A yank or delete can stay visible in a region whose bucket missed it until the
  repair sweep drains — the same window a failed-over node has today.

Reads leave a region's bucket on the same repeated-failure streak that moves a
write — within seconds — and fall back to the preferred bucket. They return only
after it's healthy again *and* holds every acked file, so a recovering bucket
never serves a stale read.

## Operating rules

**Restore every bucket together.** Rolling one bucket back to an older snapshot
resurrects packages and reserved names you had deleted — pypiron trusts what the
buckets hold now. Version them as a set (bucket versioning, or a coordinated
snapshot) and restore them as a set.

**Change the list with stop-migrate-restart.** The list is stamped into every
bucket, and a node refuses a different one. To add, remove, replace, or reorder:

1. Stop the fleet.
2. Run the migration once with the new complete list:

    ```bash
    PYPIRON_BUCKETS=s3://iron-a@us-east-1,gs://iron-b \
      pypiron buckets migrate
    ```

3. Start every node with that same list.

Migration refuses while any bucket has pending repairs, and a bucket you remove
must be reachable and drained — a stranded repair there could be a file's only
copy. Bring it back, let it drain, retry. Shrinking to a single bucket needs no
migration: stop the fleet and restart with that one bucket.

Never run two nodes with different lists.

## Upload collisions

Two uploads of **different bytes under the same filename** are both accepted only
if they hit different buckets during a partition; otherwise the second is
rejected, exactly as with one bucket. pypiron keeps the earliest, quarantines the
other (both, if the arrival times are too close to call), deletes nothing, and
increments `pypiron_replication_freezes_total`. Alert on it. Resolve by
publishing under a new filename.

## Private names during a partition

A brand-new private name can exist on one side of a long partition before its
reservation reaches the other side. If you also proxy public PyPI, reserve the
namespace on every node:

```toml
private-prefix = "acme"

[serve]
buckets = ["s3://iron-a@us-east-1", "s3://iron-b@eu-west-1"]
proxy-upstream = "https://pypi.org"
```

`acme` and `acme-*` are private everywhere, and the proxy never fills those names
from public PyPI. If your private names share no prefix, deny each exact name in
the `[mirror]` rules instead.

Public packages fetched through the proxy are not copied between buckets — each
bucket re-fetches that replaceable cache from upstream. Deleting a proxy-cached
file by hand is refused when you run more than one bucket.

## Limits

- Destroying (not just disconnecting) a bucket in the seconds after an upload
  that could not reach it loses those last writes; re-publish them.
- Already-issued download links live until their one-hour expiry. A client caught
  mid-switch may need one install retry.
- A CDN or a client that already cached bytes can still serve them after a
  collision is quarantined.
- A quarantined collision needs a human: publish a new filename to move on.

Every flag lives in
[Configuration](../reference/configuration.md#multiple-regions-and-clouds), and
[multi-bucket metrics](../reference/configuration.md#multi-bucket-metrics) covers
what to alert on.
