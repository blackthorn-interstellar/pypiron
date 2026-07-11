# Survive a bucket outage

Give pypiron two S3 buckets in different regions. Installs keep working when
either region dies, and publishing moves to the next bucket within seconds.
No promotion command or DNS change.

## Run it

List buckets in preference order. The first is preferred:

```bash
pypiron serve \
  --s3-bucket iron-east@us-east-1 \
  --s3-bucket iron-west@us-west-2
```

Or use one environment variable:

```bash
export PYPIRON_S3_BUCKETS=iron-east@us-east-1,iron-west@us-west-2
pypiron serve
```

`--s3-bucket` selects S3; `--storage s3` is optional. Give every node the same
ordered list. One bucket keeps the single-bucket behavior: no multi-bucket
probes, replication, or background work.

Multi-bucket mode is S3-only. All buckets use the same AWS credential chain and
the same optional `--s3-endpoint-url`; `@REGION` selects a region per bucket.
Use two different regions, not two names in the same failure domain.

## Failure and recovery

Each node serves from the first bucket it sees as healthy. Timeouts (including
HTTP 408), connection failures, and 5xx responses count as outages.
Authentication, permission, KMS, quota, and configuration errors raise alarms
and do not send a misconfigured node to another bucket.

Defaults:

- Leave a failing bucket after three consecutive availability failures.
- Probe every configured bucket once per one-second worker tick by reading its
  tiny topology stamp. Each probe has a one-second deadline, independent of S3
  SDK retries, and the health loop cannot be blocked by index or replication
  work.
- Multi-bucket S3 disables SDK retries. One-second health probes switch new
  requests and cancel background work on an ineligible bucket; an in-flight
  artifact transfer keeps the normal one-hour route bound so a large upload is
  never mistaken for an outage.
- Return to a recovered, more-preferred bucket after five continuous healthy
  minutes.

Tune those windows with `--bucket-leave-failures`,
`--bucket-return-healthy-secs`, and `--worker-interval-secs`. Shorter return
windows make a flapping region move traffic back and forth.

`--intent-grace-secs` is also the crash-recovery window for cross-bucket package
work. It must be from 3 through `9223372036854775807` seconds; keep it longer
than the slowest expected upload or replication step. Long replication rotates
fresh intent-family members and staged promotion heartbeats its CAS lock, so a
healthy operation does not become stale merely because it runs past one grace.

Private uploads, deletes, yanks, and project status changes copy to every other
bucket. Failed copies stay queued in the bucket that accepted the change and
drain after recovery. A full comparison on the reconcile interval catches
missing queue entries; the default `--audit-on-boot=true` also runs one at boot.

Public packages fetched through the proxy do not copy between buckets. Each
bucket can fetch that replaceable cache from upstream again.
Manual DELETE of a mirror-cached artifact is rejected in multi-bucket mode.
Cache eviction is unavailable in v1: private and mirror records share the same
`packages/` prefix, so a broad lifecycle rule can delete private truth. Cached
bytes are retained until the bucket itself is retired.

## Protect private names

A new private name can exist on one side of a long partition before its claim
reaches the other side. If proxying public PyPI, reserve the private namespace
on every node:

```bash
pypiron serve \
  --s3-bucket iron-east@us-east-1 \
  --s3-bucket iron-west@us-west-2 \
  --private-prefix acme \
  --proxy-upstream https://pypi.org
```

This reserves `acme` and `acme-*` for private uploads and prevents the proxy
from filling them from public PyPI. If private names do not share one prefix,
omit `--private-prefix` and deny every exact private name in the shared
`[mirror]` rules instead.

## Change the bucket list

The ordered names are stamped into every bucket. A node refuses a different
order at startup. If a recovered bucket reveals a different stamp at runtime,
reads continue and writes stop until an operator fixes the topology.
The write fence is sticky; restart the node after the stamps are repaired.
Each startup topology operation is bounded to one second. An unreachable bucket
can delay boot by up to that timeout for each operation attempted against it,
but cannot block startup indefinitely; a reachable fallback still allows boot.

To add, remove, replace, or reorder buckets:

1. Stop the pypiron fleet.
2. Run the migration once with the new complete list:

    ```bash
    PYPIRON_S3_BUCKETS=iron-east@us-east-1,iron-eu@eu-west-1 \
      pypiron buckets migrate
    ```

3. Start every node with that same list.

The command increments the topology generation and updates every reachable
configured bucket. It reports buckets that could not be reached; they are
checked when they return.

Shrinking from two or more buckets to one is the exception: stop the fleet and
restart it with the lone bucket. Single-bucket mode deliberately ignores
topology stamps, so `buckets migrate` has nothing to update.

## Cost model

Let:

- `N` = configured buckets.
- `H` = pypiron nodes.
- `B` = artifact bytes.
- `S` = per-file metadata bytes.
- `C` = optional core-metadata and provenance bytes.
- `M` = number of those optional companion objects (`0` to `2`).
- `W` = `--worker-interval-secs`.
- `R` = `--reconcile-interval-secs`.
- `T` = retained content-addressed `_staging/repl/` bytes across the buckets.

For private files:

| Cost | Formula |
| --- | --- |
| Stored bytes | about `N × (live private corpus) + T`; replication adds `(N - 1) × (live private corpus)`, and completed demotion/repair stages add retained `T` |
| Replication traffic per file upload | `(N - 1) × (B + S + C)`, plus small claim and marker documents |
| Added successful write mutations per destination and file | `M + 9`: replication marker; source and destination package intents; per-file metadata, optional companions, artifact, and destination index marker; both intent deletes; replication-marker delete |
| Long replication | four more mutations per heartbeat: the next source/destination `root~seq` intents, then commits closing the prior pair |
| First private file for a name | one extra origin-only job per destination: seven mutations when it wins the claim race (todo create/delete, source/destination intent create/delete, conditional origin claim); six if the file job already claimed it |
| Outage backlog | `D × E` markers while `D` destinations are unavailable; `E` is one per private upload/delete/yank/status event, plus one for each newly claimed private name |
| Healthy-path probe upper bound per fleet per day | `H × N × 86,400 / W` small topology-stamp GETs; a slow sweep never overlaps itself |
| Fast replication-scan upper bound | `2 × H × N × 86,400 / W` LISTs: every node scans each bucket's `_staging/repl/` recovery tree and `_repl/` markers |
| Warm-index scans | Normally `(N - 1) × 86,400 / W` `_dirty/` LISTs across bucket-local lease holders; the sloppy-lease fleet ceiling is `H × (N - 1) × 86,400 / W`. Unhealthy buckets are skipped; every node still attempts the small lease operation. |
| Full comparison | `H × 4 × (N - 1)` complete `packages/` LIST walks every `R` seconds; two sequential peer passes guarantee N-bucket dissemination. The default boot audit and bucket-selection changes add one run. |
| Local package-index visibility fence | two origin GETs: initial observation plus final exact-observation check; zero added read I/O with one bucket |
| Local artifact or companion visibility fence | two origin GETs, tombstone/frozen/mirror-quarantine HEADs, and a sidecar GET when the private claim or quarantine marker requires one. With one bucket, this fence adds no read I/O. |
| Proxy read paths | higher and dynamic: eligibility/claim checks, local marker filtering, and upstream fetch/fill or companion-passthrough work depend on cache state |

The mutation rows count the eager operation itself. The worker later batches
cleanup of paired dirty markers after rebuilding the destination view.

Every upload performs the tombstone HEAD that enforces the PyPI rule that a
deleted filename cannot be reused. Multi-bucket uploads also HEAD the `.frozen`
marker: it is written first during conflict handling, so a crash before
quarantine is still a recognizable, unreusable freeze.

The request counts above describe the no-race, healthy path with destinations
already private. A mirror-to-private demotion stages the whole package and costs
more. Large prefixes need additional LIST pages and conditional-write races need
retries. Full comparison intentionally runs on every node: the selected-bucket
lease holder may not share another node's working path to a warm bucket. It
performs per-package object reads after the fixed LIST walks, but no per-package
staging LIST. `pypiron_replication_marker_backlog` is a reachable-source lower
bound during an outage. Provider request metrics remain the billing authority.

Committed stage manifests may share content-addressed members. Promotion
deletes only its manifest; the members persist and count toward retained staging
storage `T`, becoming inert when unreferenced. There is no broad safe lifecycle
rule for `_staging/repl/`, because a live manifest may still reference any of
those shared objects. Each package also keeps a never-deleted
`_staging/repl/<pkg>/.promotion-lock` CAS sentinel. Its holder heartbeats every
third of the intent grace; recovery takes over only after one full unchanged-ETag
grace and no live holder intent family. Recovery sweeps forget observations for
packages whose committed manifests are gone.

## Limits

- Replication is asynchronous. Destroying the selected bucket immediately
  after an upload can lose the last few seconds of acknowledged writes.
- Two nodes writing different bytes under one filename on different sides of a
  partition can both return success. Reconciliation preserves both bodies,
  keeps each canonical record occupied behind its frozen marker and tombstone,
  suppresses the filename from every index and new direct server read, and raises
  `pypiron_replication_freezes_total`. Publish a new filename to resolve it.
- Already-issued signed download URLs live until their one-hour expiry. A
  client caught between bucket selections may need one install retry.
- A CDN or already-cached client may still serve bytes seen before a conflict
  froze that filename. Reconciliation cannot un-serve cached content.
- Public proxy caches are per bucket. The first request after a switch may
  fetch a public artifact from upstream again. Two mirror caches holding
  different bytes do not freeze; only conflicting private truth does.
- Changing bucket membership is an operator action. Run `pypiron buckets
  migrate` when the new topology still has multiple buckets; use the restart-only
  exception above when shrinking to one. Never deploy two different ordered
  lists.

See [Configuration](../reference/configuration.md#multiple-buckets) for every
flag and [multi-bucket metrics](../reference/configuration.md#multi-bucket-metrics)
for alert inputs.
