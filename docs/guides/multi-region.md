---
description: A region or a whole cloud goes down; installs and uploads keep working, and you do nothing. The setup is one bucket list and a load balancer.
---

# Survive a region or cloud outage

A region goes down — or a whole cloud — and installs keep working. Uploads keep
working. You do nothing: every node fails over by itself within seconds, with no
promotion command and no DNS surgery. Any one bucket can serve the entire index
alone: an upload is not acknowledged until every reachable bucket holds the
file, and a bucket that was down catches up when it returns. Label the buckets
by region and nodes read from their own region day to day — in-region latency,
no cross-region egress.

## The whole setup

One bucket per region. One or more nodes per region. The **same ordered bucket
list** on every node — order is preference, and each node reads from the first
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

In front of the nodes, a health-checked load balancer or failover DNS keyed on
`/ready`. Every region can serve the full index; the front door only picks
which region a client reaches. `/ready` reports `503` on a node that can no
longer serve reads — and during a graceful shutdown, so the front door pulls
the node before it stops accepting.

## What survives

- **A region outage.** A node that sees three straight failures on a bucket
  switches to the next healthy one within seconds. Every bucket already holds
  every file, so there is no catch-up.
- **A whole cloud outage**, when the buckets span two clouds
  ([below](#span-two-clouds)).
- **Every acknowledged upload.** A publish returns `200` only once the file is
  on every reachable bucket — two buckets, two copies, before the client hears
  success. If one bucket was down, pypiron copies the file over when it
  returns.
- **Your mirrored corpus.** Packages you pulled in with `sync --to` replicate
  exactly like your own uploads, so every bucket holds the whole mirror.
  Packages fetched on demand from public PyPI (the proxy cache) converge too:
  the fetch is served immediately from the bucket that got it, and the copy to
  the other buckets happens in the background, within minutes. Nothing you
  serve stays pinned to a single bucket.
- **Deletes.** A deleted filename never comes back, even if a bucket was
  unreachable when you deleted it.

## You do nothing

1. A bucket starts failing. After three consecutive failures, each node stops
   using it and serves from the next one in the list.
2. `/ready` turns `503` on nodes that can no longer reach any bucket, and your
   load balancer routes clients to a surviving region. Installs continue.
3. Uploads keep succeeding on the remaining buckets, and pypiron queues a
   repair for the one that is down.
4. When the bucket recovers, the repairs drain — immediately on recovery, with
   a periodic sweep as a backstop and a full comparison daily and at boot.
   Nodes return to a more-preferred bucket only after five minutes of
   continuous health, so a flapping region does not bounce traffic back and
   forth.

Timeouts, connection failures, and `5xx` responses count as an outage.
Credential, permission, KMS, quota, and configuration errors do not — they
raise alarms instead, so a misconfigured node cannot flee a healthy region.

## Span two clouds

Losing all of AWS — or all of Google — becomes one more bucket going away. Mix
backends in the list:

```toml
buckets = ["s3://iron-a@us-east-1", "gs://iron-b"]
```

Each bucket authenticates with its own cloud's native credentials. Cross-cloud
replication pays egress on upload — cents a gigabyte, noise at real publish
rates.

## Reads stay in their region

Installs pull bytes from nearby, and steady-state cross-region egress drops to
zero. The region label is the only new configuration, identical on every node:

```toml
buckets = ["s3://iron-east@us-east-1", "s3://iron-west@us-west-2"]
```

Each node detects its own region at boot from its cloud's instance metadata
(AWS, GCP, Azure) and serves reads — index pages and downloads — from the
matching bucket. Writes don't change: every upload still lands on the preferred
bucket and fans out to the rest before the client hears `200`. Your own uploads
and your `sync --to` mirror both live in every region's bucket, so a local
install of either pulls bytes from nearby and pays no cross-region egress. (A
fresh on-demand proxy fill is the exception until its background copy lands,
within minutes.)

A node that detects no region, or matches no labeled bucket, reads from the
preferred bucket exactly as before. On-prem or MinIO fleets have no metadata to
read — name the region yourself:

```bash
export PYPIRON_NODE_REGION=dc-east
```

A wrong region only ever costs latency, never correctness: when in doubt, a
node reads from the preferred bucket.

**A local read never returns less than the preferred bucket would.**

- An acked file never 404s. If a new release hasn't reached the local bucket
  yet, the node reads through to the preferred bucket and serves it.
- A brand-new package installs everywhere the moment it's published — the same
  read-through covers its first release.
- A private name never falls through to the public proxy. Whether a name is
  yours is decided on the preferred bucket, so a lagging local bucket can't
  serve a public package in a private name's place.
- A new release of an existing package can take a few seconds to appear in a
  remote region's index. The file installs the whole time; only the listing
  lags.
- A yank or delete can stay visible in a region whose bucket missed it until
  the repair sweep drains — the same window a failed-over node has today.

Reads leave a region's bucket on the same repeated-failure streak that moves a
write — within seconds — and fall back to the preferred bucket. They return
only after it's healthy again *and* holds every acked file, so a recovering
bucket never serves a stale read.

## Operating rules

**Restore every bucket together.** Rolling one bucket back to an older snapshot
resurrects packages and reserved names you had deleted — pypiron trusts what
the buckets hold now. Version them as a set (bucket versioning, or a
coordinated snapshot) and restore them as a set.

**Change the list with stop-migrate-restart.** The list is stamped into every
bucket, and a node refuses a different one. To add, remove, replace, or
reorder:

1. Stop the fleet.
2. Run the migration once with the new complete list:

    ```bash
    PYPIRON_BUCKETS=s3://iron-a@us-east-1,gs://iron-b \
      pypiron buckets migrate
    ```

3. Start every node with that same list.

**Adding a bucket backfills itself before it serves.** A fresh bucket starts
empty, so its region's reads keep coming from the write bucket until the corpus
has copied over — you never serve a half-filled index. Once every file has
replicated, that region's reads move to the local bucket automatically. Seed a
very large corpus out of band first (`aws s3 sync`, `rclone`) and the copy step
becomes a quick verify.

**Removing a bucket refuses to lose data.** Migration will not drop a bucket
that holds the fleet's only copy of a file — it names examples and stops. Add
the replacement, let replication converge (so the content lives elsewhere),
then remove the old bucket. To drop it anyway and *discard that content*, pass
`--force`.

Migration also refuses while any bucket has pending repairs, and a bucket you
remove must be reachable — one it cannot inspect it will not drop. Bring it
back, let it drain, retry. Shrinking to a single bucket needs no migration:
stop the fleet and restart with that one bucket.

Never run two nodes with different lists.

## Upload collisions

Two uploads of **different bytes under the same filename** are both accepted
only if they hit different buckets during a partition; otherwise the second is
rejected, exactly as with one bucket. pypiron keeps the earliest, quarantines
the other (both, if the arrival times are too close to call), deletes nothing,
and increments `pypiron_replication_freezes_total`. Alert on it. Resolve by
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

`acme` and `acme-*` are private everywhere, and the proxy never fills those
names from public PyPI. If your private names share no prefix, deny each exact
name in the `[mirror]` rules instead.

Public packages fetched **on demand through the proxy** converge across buckets
too, in the background — the fetch is served the instant it lands, and the copy
to the other buckets follows within minutes (a lag you never see on the
download path). Once converged, the zero-cross-region-egress promise covers
everything: a surviving bucket serves proxied packages, mirrored packages, and
your own uploads alike. Deleting a proxy-cached file by hand is refused when
you run more than one bucket — the entry replicates like everything else now,
and it stays re-fillable from upstream rather than being tombstoned fleet-wide.

## Limits

- Destroying (not just disconnecting) a bucket in the seconds after an upload
  that could not reach it loses those last writes; re-publish them.
- Already-issued download links live until their one-hour expiry. A client
  caught mid-switch may need one install retry.
- A CDN or a client that already cached bytes can still serve them after a
  collision is quarantined.
- A quarantined collision needs a human: publish a new filename to move on.

Every flag lives in
[Configuration](../reference/configuration.md#multiple-regions-and-clouds), and
[multi-bucket metrics](../reference/configuration.md#multi-bucket-metrics)
covers what to alert on.
