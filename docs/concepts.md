---
title: Core concepts
description: How pypiron hosts private packages, supplies public packages, stores files, controls access, blocks threats, and reports its health.
---

# Core concepts

**pypiron gives your team one Python package index for private packages and
approved public packages from PyPI.**

## Your private packages

Publish with uv, twine, poetry, hatch, or flit. Point the client at
`https://your-server/legacy/`; your build does not change.

Published filenames are immutable. pypiron refuses to replace a file, even
with identical bytes, so a version already in a lockfile cannot change. Yanking
a file hides it from normal resolution while keeping pinned installs working.

[Publish and install](guides/publish-install.md)

## Public packages

- **Cache on demand.** Set `--proxy-upstream https://pypi.org`. pypiron fetches
  a package when a client first asks for it, checks the upstream hash, stores
  it, and serves later installs from your storage.
- **Sync ahead of time.** `pypiron sync` copies an approved set of packages to
  your server. Use this for predictable builds and networks without internet
  access.

Both methods use the `[mirror]` rules in `pypiron.toml`. You can select package
names and versions, wheel tags, platforms, file types, sizes, release ages, and
pre-releases.

[Mirror selection](reference/configuration.md#mirror-selection) ·
[Air-gapped deployment](guides/air-gapped.md)

## A name is yours or PyPI's, never both

Each package name is private or public. It cannot be both.

The first private upload or public fetch claims the name. Deleting its files
does not release that claim, so a private package never falls through to PyPI.
To repurpose an empty name, an admin must release it explicitly.

`--private-prefix acme` reserves `acme` and `acme-*` for private packages,
including names you have not published yet. Name matching follows Python's
normalization rules, so `acme_foo`, `acme.foo`, and `acme-foo` are the same
name.

[Dependency-confusion protection](security.md#dependency-confusion)

## Where packages live

pypiron stores files in a local directory or in S3, Google Cloud Storage, or
Azure Blob Storage. There is no database beside the package store.

- **Local disk** is the default and supports one server process.
- **One bucket** lets any number of server nodes share the same packages.
- **Several buckets** add cross-region or cross-cloud replication and
  failover. Each bucket holds a complete copy after replication catches up.

Cloud credentials come from each provider's normal credential chain. Buckets
must already exist.

[Storage configuration](reference/configuration.md#storage) ·
[Multi-region deployment](guides/multi-region.md)

## Who can do what

With no credentials, reads are public and writes are disabled.

Credentials grant cumulative roles:

| Role | Access |
| --- | --- |
| Reader | Install packages. |
| Uploader | Install and publish. |
| Admin | Install, publish, mirror, yank, delete, and change project status. |

`--admin-pass` enables the default `admin` user. Uploader and reader
credentials require both a username and password; an incomplete pair makes
startup fail. For CI, a signing key enables five-minute install tokens. Username tags such as
`reader+billing-api` can attribute traffic to a project in access logs and,
when enabled, Prometheus metrics.

[Authentication options](reference/configuration.md#server) ·
[Install tokens](reference/configuration.md#install-tokens)

## What it keeps out

- **New public releases wait seven days by default.** This dependency cooldown
  gives maintainers and advisory services time to find a malicious release.
- **Known malware is refused.** The binary includes a block set for first boot,
  then checks the OSV PyPI advisory feed for updates.
- **Private names never resolve from public PyPI.** This blocks the common
  dependency-confusion path.
- **Optional approval rules limit public packages.** The proxy and sync command
  can fetch only the names and versions you allow.
- **Upstream files are hash-checked.** A corrupt or truncated download is not
  committed to storage.

In an analysis run in July 2026, OSV contained 17,043 malicious PyPI releases;
72% had a public advisory within the default seven-day cooldown.

[Security features and evidence](security.md)

## What it tells you

- `/health` reports that the process is alive; `/ready` reports that the node
  can serve reads.
- `/metrics` exposes Prometheus traffic, storage, replication, advisory, and
  health metrics. The endpoint is unauthenticated.
- `/stats/downloads` reports best-effort download counts when statistics are
  enabled.
- `/audit` lists public packages affected by known advisories, ranked by
  downloads over the last 30 days. It requires an admin credential.
- The web interface lets you search packages and inspect releases,
  dependencies, README content, and advisories.

[Health and maintenance](reference/configuration.md#health-and-maintenance) ·
[Endpoints](reference/configuration.md#endpoints) ·
[How pypiron is tested](testing.md)
