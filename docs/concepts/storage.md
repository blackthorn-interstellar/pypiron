---
description: Local disk by default. S3, GCS, or Azure for more than one node and a store that outlives the VM. Bucket URLs, credentials, sharing a bucket.
---

# Storage backends

An object-storage bucket buys you more than one node and a package store that
outlives any VM. Local disk is the default — no configuration, one node.

## Local disk

Artifacts live under `~/.pypiron/packages`. Move them with `--data-dir`. Disk
is single-node — right for a laptop, a CI box, or one server with a mounted
volume.

## Object storage

Point `--buckets` at a bucket that already exists (pypiron doesn't create
buckets):

```
pypiron serve --buckets s3://acme-pypiron
```

That is the whole setup. The file form is equivalent:

```toml
[serve]
buckets = ["s3://acme-pypiron"]
```

Every URI carries a scheme, and may carry an `@region`:

- `s3://name` or `s3://name@region` — S3 bucket.
- `gs://name` or `gs://name@region` — GCS bucket.
- `az://container` or `az://container@region` — Azure blob container.

`@region` labels the bucket's region. On S3 it also selects the signing and
endpoint region. S3-compatible stores (MinIO et al.) work through
`--s3-endpoint-url`.

On object storage, downloads can go straight from the bucket to the client: the
node serves the index, storage serves the wheel bytes. Run any number of nodes
against one bucket behind a load balancer — no node talks to another.

## Credentials

Each backend resolves its own native credentials:

- **S3** — the standard AWS chain: env vars, web identity, instance role, task
  role. On AWS there is nothing else to set.
- **GCS** — a service-account key (`--gcs-service-account-path`) lets the
  bucket serve downloads directly; without one, Application Default Credentials
  are used and downloads stream through the node.
- **Azure** — the account access key (`--azure-access-key`) lets the container
  serve downloads directly.

Credentials fail closed: a bucket whose backend is half-configured refuses to
start. When one endpoint or account isn't enough — two S3 accounts, AWS plus
MinIO — a bucket can carry its own endpoint and credentials:
[Configuration → Storage](../reference/configuration.md#storage).

## Share a bucket

By default pypiron owns the whole bucket: artifacts land in `packages/`,
indexes in `simple/`. Set `--storage-prefix pypi` and they move to
`pypi/packages/` and `pypi/simple/` instead, leaving the rest of the bucket
alone — pypiron neither reads nor writes outside its prefix. Two servers can
share one bucket by taking different prefixes. On disk the prefix is a
subdirectory of `--data-dir`.

The prefix is part of every key, so it is chosen once, when the bucket is first
populated. To adopt one later, move the objects (`aws s3 mv --recursive`,
`gcloud storage mv`) before restarting.

## Upgrades never corrupt the store

pypiron stamps nothing on your storage today. If a future release ever changes
the on-disk layout, it ships that change as a numbered storage format, and an
older binary pointed at a newer store refuses to start instead of writing the
old shape into it. Upgrades stay rolling: roll the new binaries out, then flip
the format once, with no downtime. If a stale binary comes back, the message it
prints says exactly what to deploy.

## More than one bucket

Several buckets across regions or clouds replicate automatically and ride out
the loss of any one — a region, or a whole cloud:
[Survive a region or cloud outage](../guides/multi-region.md).
