---
description: Keep installs and uploads available through a region or cloud outage with replicated object-storage buckets.
---

# Survive a region or cloud outage

Multi-region pypiron requires object storage. Every node uses the same ordered
bucket list. When one bucket fails, nodes use another and repair the failed
bucket after it returns.

## Set up the regions

Create one bucket per region, then save the same `pypiron.toml` on every node:

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
buckets = ["s3://iron-east@us-east-1", "s3://iron-west@us-west-2"]
proxy-upstream = "https://pypi.org"
```

Each node needs credentials for every bucket. Start nodes as described in
[Deploy on cloud storage](standard-cloud.md), and give every bucket the
[lifecycle rules for interrupted uploads](standard-cloud.md#clean-up-interrupted-uploads):
replication writes large files to each of them.

The first start on a new bucket list needs every bucket to answer. If one
can't be reached, the node refuses to start and names it. After that first
start, nodes start fine while a bucket is down.

Put a load balancer in front of them:

- Send readiness checks to `/ready`.
- Send process-liveness checks to `/health`.
- Route clients to the nearest healthy region.
- Set `--trusted-proxy` on every node, so a few wrong passwords lock out one
  client rather than everyone behind the balancer
  ([details](standard-cloud.md#more-nodes)).

`/ready` returns `503` only when a node cannot serve reads or is shutting down.
`/health` stays `200` while the process is alive, including during a storage
outage.

## What survives

- pypiron starts private uploads and packages copied by `sync` against every
  bucket before returning success. If one cannot finish within the write grace
  period, pypiron records durable repair work and returns after the surviving
  write.
- Public packages fetched through the on-demand proxy are served immediately;
  their other copies are made in the background.
- Yanks, deletes, package ownership, and the advisory feed also propagate.

A node leaves a bucket after three consecutive connection, timeout, or `5xx`
failures. Permission, credential, KMS, quota, and configuration errors raise
alarms instead of triggering failover.

When a bucket returns, repairs start immediately, with a periodic sweep as a
backstop. A node waits five healthy minutes before preferring the recovered
bucket again.

## Span two clouds

Bucket schemes may be mixed:

```toml
buckets = ["s3://iron-east@us-east-1", "gs://iron-west@us-west1"]
```

Each backend uses its own credentials. Upload replication between cloud
providers incurs network egress charges.

## Reads stay in their region

The `@region` suffix labels each bucket. AWS, GCP, and Azure nodes detect their
region at startup and read from a matching healthy bucket. For on-premises or
custom object storage, set it explicitly:

```bash
export PYPIRON_NODE_REGION=dc-east
```

If no label matches, the node reads from the first healthy bucket. A local
bucket missing a newly accepted file reads through to a complete bucket instead
of returning `404`.

## Operating rules

### Keep the list identical

Order is part of the topology. A node with a different list or order refuses to
start.

If a bucket comes back while nodes are running and carries a different list,
those nodes keep serving reads but refuse writes with `503` until you fix the
list and restart them. `/ready` stays `200`, so the fleet stays in rotation;
alert on `pypiron_bucket_topology_write_fenced` instead.

### Change the list with stop, migrate, restart

To add, remove, replace, or reorder a bucket:

1. Stop every node.
2. Run one migration with the new complete list.
3. Restart every node with that same list.

```bash
PYPIRON_BUCKETS=s3://iron-east@us-east-1,gs://iron-west@us-west1 \
  pypiron buckets migrate --config /etc/pypiron/pypiron.toml
```

Use the same config and cloud credentials as the stopped nodes. The environment
variable replaces only the old bucket list; storage prefixes, endpoints, and
backend account settings still come from the config.

A new empty bucket stays out of regional reads until its backfill completes.
A file another bucket can't copy yet, such as one still missing its metadata,
holds it out too. While that lasts, the server logs a warning naming the bucket
and the file's package.
Migration refuses to remove a bucket that is unreachable, has pending repairs,
or contains the only copy of a file or of a deletion. Add its replacement, let backfill finish,
then remove the old bucket. It also refuses any change while a pending repair
remains, and names that repair. `--force` accepts loss of unique content and
proceeds past pending repairs: one on a bucket you keep is still retried, one on
a bucket you remove is abandoned.

Migration also refuses when it can't learn the current list: no bucket it can
reach carries one and another bucket is down. Bring that bucket back and run it
again.

If a migration stops partway, run the same command again.

If migration reports repairs that appeared while it ran, the new list is
already in place and a node is still running. Stop it. A repair on a bucket you
kept finishes once nodes restart. A repair on a bucket you removed may be the
only copy of a file: migrate back to the previous list with `--force`, restart
the fleet, let replication finish, then remove the bucket again. Here `--force`
only gets past the pending repair on the bucket you are adding back, and the
restarted fleet delivers it.

To evacuate temporarily to one surviving bucket, stop the fleet and restart it
with that single bucket. Returning to a multi-bucket list requires the migration
sequence above.

### Restore buckets as a set

Restoring only one bucket from an older snapshot can bring back files or names
deleted later. Restore every bucket to the same point in time.

### A repair that keeps failing

When a copy to another bucket fails, pypiron records a pending repair and retries
it until it lands. An unreachable bucket only delays it. A damaged file record
makes it fail every time:

- `pypiron_replication_marker_failures` stays above `0` for that bucket.
- The server log has an error naming the file, the pending repair, and the cause.
- Reads stay on the write bucket instead of returning to that region's bucket.
- `buckets migrate` refuses and names the pending repair.

Replace the damaged file record (the `<filename>.meta.json` object the log names)
with the intact copy from another bucket. The next sweep finishes the repair, the
gauge returns to `0`, and reads return to the region.

## Upload collisions

During a partition, two different files can reach different buckets under the
same filename. pypiron keeps the earlier one when it can determine an order and
quarantines the conflict. Alert on `pypiron_replication_freezes_total` and
publish the corrected artifact under a new filename.

## Private names during a partition

Set `private-prefix` on every node before enabling the public proxy. This keeps
an unpublished private name from being claimed from PyPI on the other side of a
partition. If your private names share no prefix, list them in the top-level
`private-patterns` setting on every node instead.

If a public download finishes after the package becomes private, its late
mirror files are quarantined during queued replication repair. Private files
remain intact; recovery does not wait for the daily full reconcile.

## Limits

- If every bucket is unreachable, `/ready` returns `503` on every node until
  storage returns.
- Destroying the only bucket that received an outage-time upload before its
  queued repair finishes loses that upload.
- A signed object-storage download URL already issued can work until its
  one-hour expiry.
- A client caught during failover may need to retry one install.

See [multi-bucket configuration](../reference/configuration.md#multiple-regions-and-clouds)
and [metrics](../reference/configuration.md#multi-bucket-metrics).
