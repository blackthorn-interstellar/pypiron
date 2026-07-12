# Survive a bucket outage

Give pypiron a list of buckets instead of one. Every upload lands on all of them
before it returns, so losing a bucket — a region, or a whole cloud — never stops
installs and never loses an acknowledged upload. No promotion command, no DNS
change, no failover drill.

## Run it

List buckets in preference order, as URIs. The first is preferred:

```bash
pypiron serve \
  --buckets s3://iron-east@us-east-1,s3://iron-west@us-west-2
```

Or one environment variable:

```bash
export PYPIRON_BUCKETS=s3://iron-east@us-east-1,s3://iron-west@us-west-2
pypiron serve
```

Each entry is a URI with a scheme: `s3://name` (add `@region` for S3),
`gs://name`, or `az://container`. Mix clouds freely — a bucket in AWS and a
bucket in Google is a supported, first-class setup, and it buys you survival of
an entire cloud provider's outage:

```bash
pypiron serve \
  --buckets s3://iron-east@us-east-1,gs://iron-backup
```

Each backend uses its own native credentials (the AWS chain for S3, a
service-account key or Application Default Credentials for GCS, the account key
for Azure). A bucket with half its credentials configured refuses to start.

Give every node the same ordered list. A one-bucket list is the same as pointing
at that one bucket directly: no failover machinery runs.

## What you get

- **Uploads are durable everywhere at `200`.** The publish returns only once the
  file is on every reachable bucket. If one bucket is slow or down, the upload
  still succeeds and pypiron remembers to copy the file over when the bucket
  comes back.
- **Reads fail over on their own.** Each node serves from the first bucket it
  sees as healthy and switches within seconds when that bucket stops answering.
  Because every bucket already holds every file, the switch is seamless.
- **Deletes stay deleted.** A deleted filename never comes back, on any bucket,
  even if the delete happened while a bucket was unreachable.

## Failure and recovery

Each node watches every bucket and serves from the most-preferred healthy one.
Timeouts, connection failures, and 5xx responses count as an outage.
Authentication, permission, KMS, quota, and configuration errors raise alarms
instead — a misconfigured node stays on its preferred bucket rather than fleeing
to another.

Defaults you can tune:

- Leave a failing bucket after three failures in a row
  (`--bucket-leave-failures`).
- Return to a recovered, more-preferred bucket after five continuous healthy
  minutes (`--bucket-return-healthy-secs`). The long return window keeps a
  flapping region from bouncing traffic back and forth.
- A lagging bucket on upload gets 30 seconds to catch up before pypiron records
  a repair and returns (`--fanout-grace-secs`), so one slow bucket adds at most
  that much to a publish.

When a bucket recovers, the queued repairs drain automatically — right away when
the bucket comes back, and on a slow safety-net sweep otherwise. A full
comparison on a daily cadence (and at boot) catches anything a repair missed.
You do nothing.

Public packages fetched through the proxy are not copied between buckets — each
bucket re-fetches that replaceable cache from upstream on its own. Deleting a
proxy-cached file by hand is refused when you run more than one bucket.

## If two uploads collide

Two people can upload **different bytes under the same filename** only if they
hit different buckets during a partition — otherwise the second upload is
rejected immediately, exactly as with one bucket. When that rare collision
happens, pypiron keeps the **first** upload (the one that arrived earliest by the
server's clock) and quarantines the other. Nothing is deleted, nothing is
silently overwritten, and `pypiron_replication_freezes_total` fires so **you get
paged**. If the two arrived too close to call, pypiron quarantines *both* and
still pages you. Either way, resolve it by publishing under a new filename.

## Protect private names

A brand-new private name can exist on one side of a long partition before its
reservation reaches the other side. If you also proxy public PyPI, reserve the
private namespace on every node:

```bash
pypiron serve \
  --buckets s3://iron-east@us-east-1,s3://iron-west@us-west-2 \
  --private-prefix acme \
  --proxy-upstream https://pypi.org
```

This reserves `acme` and `acme-*` for private uploads and stops the proxy from
filling those names from public PyPI. If your private names share no prefix, drop
`--private-prefix` and deny each exact private name in the `[mirror]` rules
instead.

## Operator rules

Two rules the design depends on. Follow them and multi-bucket is hands-off;
break them and you can lose data.

**Restore backups fleet-wide, never one bucket.** If you ever restore from
backup, restore *every* bucket to the same point in time. Rolling a single bucket
back to an older snapshot resurrects packages and reservations you had deleted —
pypiron trusts what the buckets currently hold, and an old snapshot lies to it.
Since the buckets are just files, version them together (bucket versioning, or a
coordinated snapshot) and restore them together.

**Change the bucket list with a stop-migrate-restart.** The ordered list is
stamped into every bucket; a node refuses a different list at startup, and if a
recovered bucket reveals a different list at runtime, reads keep working but
uploads stop until you fix it. To add, remove, replace, or reorder buckets:

1. Stop the pypiron fleet.
2. Run the migration once with the new complete list:

    ```bash
    PYPIRON_BUCKETS=s3://iron-east@us-east-1,gs://iron-eu \
      pypiron buckets migrate
    ```

3. Start every node with that same list.

Migration **refuses to run while any bucket still has pending repairs**, so a
file that only made it to a bucket you're about to remove can't be stranded. If
it refuses, let the fleet finish draining (or bring the lagging bucket back) and
run it again. Never run two nodes with different lists.

Shrinking all the way down to one bucket is the exception: just stop the fleet
and restart with the single bucket — there's no migration to run.

## Limits

- Destroying (not just disconnecting) a bucket in the seconds after an upload
  that couldn't reach it can lose those last few writes; re-publish them.
- Already-issued download links live until their one-hour expiry. A client caught
  mid-switch may need one install retry.
- A CDN or already-cached client can still serve bytes it fetched before a
  collision was quarantined. No server can un-serve cached bytes.
- A quarantined collision needs a human: publish a new filename to move on.

See [Configuration](../reference/configuration.md#multiple-buckets) for every
flag and [multi-bucket metrics](../reference/configuration.md#multi-bucket-metrics)
for what to alert on.
