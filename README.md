# <img src="docs/assets/pypiron-logo-256.png" alt="pypiron logo" width="40" style="vertical-align: middle;"/> pypiron

[![CI](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-pypiron-bf5a2e.svg)](https://pypiron.com/)

An ultra-fast, rock-solid PyPI server.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/install-throughput-dark.svg">
    <img src="docs/assets/install-throughput.svg" alt="Max sustained install throughput: pypiron vs bandersnatch, pypiserver, pypicloud, devpi, proxpi" width="760">
  </picture>
</p>

- **[100× faster than any other PyPI server](docs/reference/benchmarks.md).**
- **72% of malware attacks blocked immediately.**
- **Effortlessly scales via cloud storage — no database!**
- **Supports [cross-region](docs/guides/multi-region.md) and cross-cloud high availability.**
- **Works with local disk, AWS S3, GCP, and Azure.**
- **Comprehensively tested via [fuzzing](dev/TESTING.md#fuzzing), [chaos testing](dev/TESTING.md#chaos-and-crash-consistency), [deterministic simulation](dev/TESTING.md#deterministic-simulation-the-vopr), [model checking](dev/TESTING.md#machine-checked-models-stateright), [real clouds](dev/TESTING.md#real-cloud-backends), [perf](dev/TESTING.md#performance-testing), and [all 17 million files on PyPI](src/corpus_check.rs).**


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

| Feature | pypiron | bandersnatch | pypiserver | pypicloud | devpi | proxpi |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Easy setup | <abbr title="Single binary, uvx, or Docker; hosts private packages, mirror sync, and proxy from one server.">✅</abbr> | — | <abbr title="Simple private package host over a local directory.">✅</abbr> | — | — | <abbr title="Simple caching proxy; no private uploads.">✅</abbr> |
| Fast | <abbr title="8,288 verified installs/s on 2 vCPU in the benchmark.">✅</abbr> | <abbr title="77 installs/s as a static nginx-served mirror — NIC-bound on the same box.">✅</abbr> | — | — | — | — |
| Private package hosting | <abbr title="Publish with twine or uv; admin/uploader/reader credentials plus short-lived install tokens.">✅</abbr> | — | <abbr title="Upload endpoint with htpasswd auth.">✅</abbr> | <abbr title="Upload endpoint with access-control lists.">✅</abbr> | <abbr title="Private indexes with per-index access control.">✅</abbr> | — |
| Caching PyPI proxy | <abbr title="Caches public packages from PyPI on first install, behind the same URL as your private ones.">✅</abbr> | — | — | <abbr title="Its fallback = cache mode stores and re-serves upstream packages.">✅</abbr> | <abbr title="Caches PyPI through its root/pypi mirror index.">✅</abbr> | <abbr title="A caching PyPI proxy is its core purpose.">✅</abbr> |
| Sync mirror | <abbr title="Mirrors a chosen subset of upstream: include/exclude by name, wheel tags, format, size, Python floor, and pre-release.">✅</abbr> | <abbr title="A full or filtered PyPI mirror is its core purpose.">✅</abbr> | — | — | — | — |
| Dependency cooldown | <abbr title="New releases wait 7 days by default, enforced at the server for every client; upload times are preserved for client-side exclude-newer.">✅</abbr> | — | — | — | — | — |
| Malware blocking | <abbr title="Refuses any file the OSV advisory feed flags as malware — at upload, on proxy fill, and in listings; on by default.">✅</abbr> | — | — | — | — | — |
| No dependency confusion | <abbr title="A name is yours or PyPI's, never both; a private name never falls through to upstream.">✅</abbr> | — | — | — | <abbr title="Privately uploaded names block upstream mirror lookups by default.">✅</abbr> | — |
| Vulnerability audit | <abbr title="/audit lists every hosted or proxied package a known advisory affects, ranked by your install counts.">✅</abbr> | — | — | — | — | — |
| Scalable without database | <abbr title="Multi-node against S3, GCS, or Azure Blob; no database.">✅</abbr> | <abbr title="Static mirror tree served by nginx or object storage; no database.">✅</abbr> | — | — | — | — |
| Multi-region / multi-cloud failover | <abbr title="One bucket list spanning regions and clouds (S3 + GCS + Azure); every upload lands on all of them before the ack, and reads fail over with zero data loss.">✅</abbr> | — | — | — | <abbr title="Master-to-replica streaming replication; each replica keeps a full copy.">✅</abbr> | — |
| Human-readable package pages | <abbr title="Dashboard, package search, download pages, project pages, and README rendering.">✅</abbr> | — | — | <abbr title="Has a web UI.">✅</abbr> | <abbr title="Web UI and README rendering via devpi-web.">✅</abbr> | — |
| Download stats | <abbr title="Built-in global and per-package download counters.">✅</abbr> | — | — | — | — | — |
| Disk-backed | <abbr title="Default local disk backend.">✅</abbr> | <abbr title="Writes a static mirror tree to disk.">✅</abbr> | <abbr title="Serves packages from local directories.">✅</abbr> | <abbr title="Supports filesystem package storage.">✅</abbr> | <abbr title="Default serverdir storage on local disk.">✅</abbr> | <abbr title="Disk-backed package cache.">✅</abbr> |
| Cloud-storage-backed | <abbr title="S3, S3-compatible, GCS, and Azure Blob.">✅</abbr> | <abbr title="S3-compatible mirror storage.">✅</abbr> | — | <abbr title="S3, GCS, and Azure Blob package storage.">✅</abbr> | — | — |

[Full comparison](https://pypiron.com/compare/)


## Testing

- **[Client compatibility testing](dev/TESTING.md#client-compatibility-matrix):** uv, pip, poetry, pdm, pipenv, hatch, flit, twine.
- **Tested against all of PyPI.** The parsers process [all 17 million files ever uploaded to PyPI](src/corpus_check.rs) and match ground truth on each one.
- **Chaos testing.** We kill the server at every step of every write, kill fleet nodes mid-upload, and feed it truncated, corrupted, and hash-mismatched upstream responses. It converges to a consistent, installable state every time ([crash sweep](tests/test_crash_consistency.py), [fleet chaos](tests/test_chaos_fleet.py), [upstream faults](tests/test_chaos_upstream.py)).
- **Exhaustive model checker.** A [model checker](dev/TESTING.md#machine-checked-models-stateright) enumerates every interleaving of writers, workers, crashes, and byte conflicts within its bounds, running the same decision functions the server ships.
- **Continuous deterministic simulation testing.** Deterministic simulation runs a whole multi-node fleet single-threaded on virtual time — on the order of a hundred thousand seeded crash/fault/restart schedules per night, every failure [reproducible from an 8-byte seed](dev/TESTING.md#deterministic-simulation-the-vopr) ([the simulator](examples/vopr.rs)).
- **Fuzzed nightly.** Coverage-guided fuzzers hammer the parsers.
- **Passed security audits by LLMs.** Fable 5 ran numerous security audits before it got nerfed. All issues fixed.

## Going further

- [Setup](docs/guides/setup.md) — private packages, public proxy, sync mirror, S3
- [Configuration](docs/reference/configuration.md) — every flag and its `PYPIRON_*` env var
- [Benchmarks](docs/reference/benchmarks.md) — how the numbers above were measured
- [For AI agents](docs/for-agents.md) — a decision guide, written for agents, by an agent

## License

MIT — see [LICENSE](LICENSE).
