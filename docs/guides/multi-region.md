# Survive a region or cloud outage

Run your package index across regions — or across cloud providers — and keep
serving when one of them goes dark. Give every node the same ordered list of
buckets; each upload lands on all of them before it returns. Lose a region, or a
whole cloud, and installs never stop and no acknowledged upload is lost. Every
copy is already durable at the `200`.

No promotion command, no DNS surgery, no failover drill. The complexity lives
inside the binary; you deploy nodes and point them at buckets.

## Deploy across two regions

You need, per region: one bucket, and one or more pypiron nodes. Every node runs
the **same ordered bucket list**, regardless of which region it sits in. Order is
preference — the first bucket is the one nodes use while it is healthy.

```bash
# Every node, every region, identical list:
pypiron serve \
  --buckets s3://iron-a@us-east-1,s3://iron-b@eu-west-1
```

Or one environment variable, again identical everywhere:

```bash
export PYPIRON_BUCKETS=s3://iron-a@us-east-1,s3://iron-b@eu-west-1
pypiron serve
```

Put a health-checked load balancer or DNS failover in front of the nodes, keyed
on `/health`. That is what routes clients away from a dead region — pypiron makes
every region able to serve the full index; the front door decides which region a
client reaches. A regional load balancer behind a global accelerator, or
latency/failover DNS across two regional endpoints, both work.

That is the whole deployment: buckets in each region, nodes in each region on one
shared list, health-checked routing in front.

## What you get

- **The index survives a region outage.** When a region's bucket stops answering,
  every node switches to the next healthy bucket in the list within seconds.
  Because every bucket already holds every file, the switch needs no catch-up.
- **It survives a whole cloud outage too.** Put buckets in different providers
  (next section) and losing all of AWS, or all of GCS, is just another bucket
  going away.
- **Zero data loss for acknowledged uploads.** A publish returns `200` only once
  the file is on every reachable bucket. Durability is the number of copies that
  existed at the `200` — two buckets, two copies, before the client ever hears
  success. If one bucket was down at upload time, pypiron records a repair and
  copies the file over when that bucket returns.
- **Deletes stay deleted.** A removed filename never comes back on any bucket,
  even if the delete happened while a bucket was unreachable.

## During a region loss

You do nothing. Here is the sequence:

1. A region's bucket starts timing out. Each node counts consecutive failures and,
   after three (`--bucket-leave-failures`), stops using that bucket and serves
   from the next healthy one in the list.
2. Your load balancer's `/health` checks fail for nodes that can no longer reach
   any bucket, and it routes clients to a surviving region. Installs continue.
3. Uploads keep succeeding. With the failed bucket unreachable, a publish lands on
   the remaining buckets and pypiron queues a repair for the one that is down.
4. **Heal.** When the region and its bucket recover, the queued repairs drain
   automatically — immediately on the bucket coming back, and on a slow safety-net
   sweep otherwise. A full comparison runs daily and at boot to catch anything a
   repair missed. Nodes return to the more-preferred bucket only after it has been
   continuously healthy for five minutes (`--bucket-return-healthy-secs`), so a
   flapping region does not bounce traffic back and forth.

Timeouts, connection failures, and 5xx responses count as an outage.
Authentication, permission, KMS, quota, and configuration errors raise alarms
instead and never move a node off its bucket — a misconfigured node must not flee
a healthy region.

## Span two clouds

The bucket list can mix providers. A bucket in AWS and a bucket in Google is a
first-class setup, and it buys you survival of an entire cloud provider's outage:

```bash
pypiron serve \
  --buckets s3://iron-a@us-east-1,gs://iron-b
```

Each entry is a URI with a scheme: `s3://name` (add `@region` for S3), `gs://name`,
or `az://container`. Each backend uses its own native credentials — the AWS chain
for S3, a service-account key or Application Default Credentials for GCS, the
account key for Azure. A bucket with half its credentials configured refuses to
start, and one bad URI fails startup before any bucket is contacted.

Cross-cloud replication pays egress on upload (a few cents a gigabyte, noise at
real publish rates). The payoff is that no single provider's bad day takes your
index down.

## The honest limit: no read locality yet

Every node reads from the **same** bucket: the first healthy one in the shared
order. A node in your secondary region still reads from the primary region's
bucket in steady state, so its reads cross the region boundary. What this feature
buys today is **surviving an outage** — when the primary bucket dies, every node
fails over together — not serving each region from a nearby bucket. Regional
read-affinity (a node preferring the bucket closest to it) is future work. Deploy
this for resilience, not for latency.

## The operator contract

Whether the list spans two regions or two clouds, the rules are the same, and
they are short.

### Restore backups fleet-wide, never one bucket

If you ever restore from backup, restore *every* bucket to the same point in time.
Rolling a single bucket back to an older snapshot resurrects packages and
reservations you had deleted — pypiron trusts what the buckets currently hold, and
an old snapshot lies to it. Since the buckets are just files, version them
together (bucket versioning, or a coordinated snapshot) and restore them together.

### Change the bucket list with stop-migrate-restart

The ordered list is stamped into every bucket. A node refuses a different list at
startup, and if a recovered bucket reveals a different list at runtime, reads keep
working but uploads stop until you fix it. To add, remove, replace, or reorder
buckets:

1. Stop the pypiron fleet.
2. Run the migration once with the new complete list:

    ```bash
    PYPIRON_BUCKETS=s3://iron-a@us-east-1,gs://iron-b \
      pypiron buckets migrate
    ```

3. Start every node with that same list.

Migration **refuses while any bucket still has pending repairs**, so a file that
only made it to a bucket you are about to remove cannot be stranded. If it
refuses, let the fleet finish draining (or bring the lagging bucket back) and run
it again.

**Removing a bucket is checked hardest.** The bucket you drop must be **reachable
and free of pending repairs** at migration time — pypiron will not drop a bucket
it cannot prove is drained, because a stranded repair there could be a file's only
copy. Bring the bucket back, let it drain, then retry. Shrinking all the way to
one bucket is the exception: stop the fleet and restart with the single bucket —
there is no migration to run.

Never run two nodes with different lists.

## If two uploads collide

Two people can upload **different bytes under the same filename** only if they hit
different buckets during a partition — otherwise the second upload is rejected
immediately, exactly as with one bucket. When that rare collision happens, pypiron
keeps the **first** upload (the one that arrived earliest by the server's clock)
and quarantines the other. Nothing is deleted, nothing is silently overwritten,
and `pypiron_replication_freezes_total` fires so **you get paged**. If the two
arrived too close to call, pypiron quarantines *both* and still pages you. Either
way, resolve it by publishing under a new filename.

## Protect private names

A brand-new private name can exist on one side of a long partition before its
reservation reaches the other side. If you also proxy public PyPI, reserve the
private namespace on every node:

```bash
pypiron serve \
  --buckets s3://iron-a@us-east-1,s3://iron-b@eu-west-1 \
  --private-prefix acme \
  --proxy-upstream https://pypi.org
```

This reserves `acme` and `acme-*` for private uploads and stops the proxy from
filling those names from public PyPI. If your private names share no prefix, drop
`--private-prefix` and deny each exact private name in the `[mirror]` rules
instead.

Public packages fetched through the proxy are not copied between buckets — each
bucket re-fetches that replaceable cache from upstream on its own. Deleting a
proxy-cached file by hand is refused when you run more than one bucket.

## Limits

- Destroying (not just disconnecting) a bucket in the seconds after an upload that
  couldn't reach it can lose those last few writes; re-publish them.
- Already-issued download links live until their one-hour expiry. A client caught
  mid-switch may need one install retry.
- A CDN or already-cached client can still serve bytes it fetched before a
  collision was quarantined. No server can un-serve cached bytes.
- A quarantined collision needs a human: publish a new filename to move on.

See [Configuration](../reference/configuration.md#multiple-regions-and-clouds) for
every flag and
[multi-bucket metrics](../reference/configuration.md#multi-bucket-metrics) for what
to alert on.
