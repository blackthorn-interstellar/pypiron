---
description: A self-hosted PyPI server in Rust. 100x faster installs, private packages, an on-demand PyPI cache, and supply-chain defense built in.
---

<!-- Generated from README.md by dev/scripts/readme_to_index.py — edit README.md, not this file. -->

# <img src="assets/pypiron-logo-256.png" alt="pypiron logo" width="40" style="vertical-align: middle;"/> pypiron

[![CI](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/blackthorn-interstellar/pypiron/blob/master/LICENSE)
[![Docs](https://img.shields.io/badge/docs-pypiron-bf5a2e.svg)](https://pypiron.com/)

An ultra-fast, rock-solid PyPI server.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/install-throughput-dark.svg">
    <img src="assets/install-throughput.svg" alt="Max sustained install throughput" width="560">
  </picture>
</p>

- **[100× faster than any other PyPI server](compare/index.md).**
- **[72% of malware attacks blocked immediately](security.md).**
- **Effortlessly scales via cloud storage — no database!**
- **Supports [cross-region](guides/multi-region.md) and cross-cloud high availability.**
- **Works with local disk, AWS S3, GCP, and Azure.**
- **[Web GUI with dashboard, package pages, and search](assets/demo.gif).**
- **[Vulnerability audit](security.md) ranked by your org's installs.**
- **[Health checks](concepts/health-metrics.md) and Prometheus metrics built in.**
- **Comprehensively tested via [fuzzing](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#fuzzing), [chaos testing](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#chaos-and-crash-consistency), [deterministic simulation](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#deterministic-simulation-the-vopr), [model checking](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#machine-checked-models-stateright), [real clouds](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#real-cloud-backends), [perf](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#performance-testing), and [all 17 million files on PyPI](https://github.com/blackthorn-interstellar/pypiron/blob/master/src/corpus_check.rs).**


## Getting started

Run pypiron with uvx to get started quickly:

```bash
uvx pypiron serve --admin-pass "$ADMIN"
```

Or with Docker:

```bash
docker run -p 8080:8080 -e PYPIRON_ADMIN_PASS="$ADMIN" ghcr.io/blackthorn-interstellar/pypiron:latest
```

## Feature comparison

<table>
  <thead>
    <tr>
      <th align="left" colspan="2">Feature</th>
      <th>pypiron</th>
      <th>bandersnatch</th>
      <th>pypiserver</th>
      <th>pypicloud</th>
      <th>devpi</th>
      <th>proxpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td colspan="2"><a href="docs/guides/standard-cloud.md">Easy setup</a></td>
      <td align="center"><abbr title="Single binary, uvx, or Docker; hosts private packages, mirror sync, and proxy from one server.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">✅</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/compare/index.md">Fast</a></td>
      <td align="center"><abbr title="8,288 verified installs/s on 2 vCPU in the benchmark.">✅</abbr></td>
      <td align="center"><abbr title="77 installs/s as a static nginx-served mirror — NIC-bound on the same box.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts/package-sources.md">Private packages</a></td>
      <td align="center"><abbr title="Publish with twine or uv; admin/uploader/reader credentials plus short-lived install tokens.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts/package-sources.md">PyPI proxy</a></td>
      <td align="center"><abbr title="Caches public packages from PyPI on first install, behind the same URL as your private ones.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts/package-sources.md">Sync mirror</a></td>
      <td align="center"><abbr title="Mirrors a chosen subset of upstream: include/exclude by name, wheel tags, format, size, Python floor, and pre-release.">✅</abbr></td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/security.md">Cooldown</a></td>
      <td align="center"><abbr title="New releases wait 7 days by default, enforced at the server for every client; upload times are preserved for client-side exclude-newer.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/security.md">Malware blocking</a></td>
      <td align="center"><abbr title="Refuses any file the OSV advisory feed flags as malware — at upload, on proxy fill, and in listings; on by default.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/security.md">No dependency confusion</a></td>
      <td align="center"><abbr title="A name is yours or PyPI's, never both; a private name never falls through to upstream.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center"><abbr title="Privately uploaded names block upstream mirror lookups by default.">✅</abbr></td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/security.md">Vulnerability audit</a></td>
      <td align="center"><abbr title="/audit lists every hosted or proxied package a known advisory affects, ranked by your install counts.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts/storage.md">Scales, no database</a></td>
      <td align="center"><abbr title="Multi-node against S3, GCS, or Azure Blob; no database.">✅</abbr></td>
      <td align="center"><abbr title="Static mirror tree served by nginx or object storage; no database.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/guides/multi-region.md">Multi-region failover</a></td>
      <td align="center"><abbr title="One bucket list spanning regions and clouds (S3 + GCS + Azure); every upload lands on all of them before the ack, and reads fail over with zero data loss.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center"><abbr title="Master-to-replica streaming replication; each replica keeps a full copy.">✅</abbr></td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/assets/demo.gif">Web GUI</a></td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts/download-stats.md">Download stats</a></td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td rowspan="4"><a href="docs/concepts/storage.md">Storage</a></td>
      <td>Disk</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
    </tr>
    <tr>
      <td>AWS S3</td>
      <td align="center"><abbr title="Plus any S3-compatible store (MinIO, R2, Ceph, …).">✅</abbr></td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td>GCS</td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td>Azure Blob</td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
  </tbody>
</table>

[Full comparison](https://pypiron.com/compare/)


## Security

- **[Known malware never installs](security.md)** — the OSV malware feed is enforced within minutes, and a release cooldown covers the window before an advisory exists.
- **[Dependency confusion cannot start](security.md)** — a name is yours or PyPI's, never both.
- **[Only what you've approved installs](concepts/approval-lists.md)** — one approval list controls everything the server will serve.
- **[Air-gapped deploys](guides/air-gapped.md)** — sync outside, carry it in, serve with no upstream at all.
- **[Vulnerability audit](security.md)** — every affected package you host or proxy, ranked by your org's installs.

## Gauntlet testing

- **[Client compatibility testing](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#client-compatibility-matrix):** uv, pip, poetry, pdm, pipenv, hatch, flit, twine.
- **Tested against all of PyPI.** The parsers process [all 17 million files ever uploaded to PyPI](https://github.com/blackthorn-interstellar/pypiron/blob/master/src/corpus_check.rs) and match ground truth on each one.
- **Chaos testing.** We kill the server at every step of every write, kill fleet nodes mid-upload, and feed it truncated, corrupted, and hash-mismatched upstream responses. It converges to a consistent, installable state every time ([crash sweep](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_crash_consistency.py), [fleet chaos](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_fleet.py), [upstream faults](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_upstream.py)).
- **Exhaustive model checker.** A [model checker](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#machine-checked-models-stateright) enumerates every interleaving of writers, workers, crashes, and byte conflicts within its bounds, running the same decision functions the server ships.
- **Continuous deterministic simulation testing.** Deterministic simulation runs a whole multi-node fleet single-threaded on virtual time — on the order of a hundred thousand seeded crash/fault/restart schedules per night, every failure [reproducible from an 8-byte seed](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#deterministic-simulation-the-vopr) ([the simulator](https://github.com/blackthorn-interstellar/pypiron/blob/master/examples/vopr.rs)).
- **Fuzzed nightly.** Coverage-guided fuzzers hammer the parsers.
- **Security audited by all frontier models** — the same models that built it. All issues fixed.

[The full gauntlet](testing.md)

## Going further

- [Deploying](guides/standard-cloud.md) — a production server from standard cloud parts
- [Package sources](concepts/package-sources.md) — private hosting, caching proxy, sync mirror, and how they combine
- [Configuration](reference/configuration.md) — every flag and its `PYPIRON_*` env var
- [Comparison & benchmarks](compare/index.md) — every alternative, and how the numbers were measured
- [For AI agents](for-agents.md) — a decision guide, written for agents, by an agent

## Contributing — [Humans Need Not Apply](https://www.youtube.com/watch?v=7Pq-S557XQU)

pypiron was built by AI coding agents from Anthropic, OpenAI, SpaceXAI, and Moonshot — and that's how it stays. All development is done by AI coders, for security and consistency: human-developed code is a security risk, and we don't accept it. Humans are welcome to open issues and contribute documentation.

## License

MIT — see [LICENSE](https://github.com/blackthorn-interstellar/pypiron/blob/master/LICENSE).
