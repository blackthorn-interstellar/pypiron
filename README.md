# <img src="docs/assets/pypiron-logo-256.png" alt="pypiron logo" width="40" style="vertical-align: middle;"/> pypiron

[![CI](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-pypiron-bf5a2e.svg)](https://pypiron.com/)

Host private packages and cache PyPI behind one ultra-fast index.

[Get started](#start-pypiron) · [Deploy on cloud storage](docs/guides/standard-cloud.md)

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/install-throughput-dark.svg">
    <img src="docs/assets/install-throughput.svg" alt="Max sustained install throughput" width="560">
  </picture>
</p>

- **[100× faster than other self-hosted PyPI servers.](docs/compare/index.md)** 8,288 installs/s on 2 vCPU.
- **[Blocks 72% of malicious releases by default.](docs/security.md)**
- **Local disk, S3, GCS, and Azure.**
- **Scale without a database or coordinator.** Add nodes to one bucket.
- **[Survives a region or cloud outage.](docs/guides/multi-region.md)**
- **[Real clients. Real clouds. All 17 million PyPI files.](docs/testing.md)**
- **[Web dashboard with package pages and search.](docs/assets/server-gui.png)**
- **[Health checks and Prometheus metrics built in.](docs/concepts.md#what-it-tells-you)**

<p align="center">
  <a href="docs/assets/server-gui.png">
    <img src="docs/assets/server-gui.png" alt="pypiron web dashboard" width="900">
  </a>
</p>


<a id="quickstart"></a>
<a id="getting-started"></a>

## Start pypiron

With [uv installed](https://docs.astral.sh/uv/getting-started/installation/),
replace `your-admin-password` and run:

```bash
PYPIRON_ADMIN_PASS='your-admin-password' uvx pypiron serve
```

pypiron is now running at `http://localhost:8080`. The admin username is
`admin`; the password is the one you chose.

Docker:

```bash
docker run -p 8080:8080 -e PYPIRON_ADMIN_PASS='your-admin-password' ghcr.io/blackthorn-interstellar/pypiron:latest
```

[Publish and install packages](docs/guides/publish-install.md) ·
[Deploy on cloud storage](docs/guides/standard-cloud.md) ·
[Migrate from another server](docs/guides/migrate.md)

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
      <td align="center"><abbr title="8,288 installs/s on 2 vCPU in the benchmark.">✅</abbr></td>
      <td align="center"><abbr title="77 installs/s as a static nginx-served mirror — NIC-bound on the same box.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts.md#your-private-packages">Private packages</a></td>
      <td align="center"><abbr title="Publish with twine or uv; admin/uploader/reader credentials plus short-lived install tokens.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts.md#public-packages">PyPI proxy</a></td>
      <td align="center"><abbr title="Caches public packages from PyPI on first install, behind the same URL as your private ones.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts.md#public-packages">Sync mirror</a></td>
      <td align="center"><abbr title="Mirrors a chosen subset of upstream: include/exclude by name, wheel tags, format, size, minimum Python, and pre-release.">✅</abbr></td>
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
      <td align="center"><abbr title="Blocks known PyPI malware during sync and proxy fetches, in listings, and on direct downloads; on by default.">✅</abbr></td>
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
      <td align="center"><abbr title="Lists public packages affected by known advisories, ranked by 30-day downloads.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts.md#where-packages-live">Scales, no database</a></td>
      <td align="center"><abbr title="Multi-node against S3, GCS, or Azure Blob; no database.">✅</abbr></td>
      <td align="center"><abbr title="Static mirror tree served by nginx or object storage; no database.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/guides/multi-region.md">Multi-region failover</a></td>
      <td align="center"><abbr title="One bucket list spans regions and clouds. Uploads replicate to healthy buckets, unavailable buckets catch up when they return, and reads fail over.">✅</abbr></td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center"><abbr title="Master-to-replica streaming replication; each replica keeps a full copy.">✅</abbr></td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/assets/server-gui.png">Web GUI</a></td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">✅</td>
      <td align="center">✅</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td colspan="2"><a href="docs/concepts.md#what-it-tells-you">Download stats</a></td>
      <td align="center">✅</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
      <td align="center">—</td>
    </tr>
    <tr>
      <td rowspan="4"><a href="docs/concepts.md#where-packages-live">Storage</a></td>
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

- **[Blocks known PyPI malware.](docs/security.md)** OSV blocking plus a release cooldown.
- **[Private names stay private.](docs/security.md)** They never fall through to public PyPI.
- **[Control which public packages install.](docs/concepts.md#what-it-keeps-out)** Set one approval list for every client.
- **[Air-gapped deployments.](docs/guides/air-gapped.md)** Serve approved packages without internet access.
- **[Vulnerability audit.](docs/security.md)** Affected public packages, ranked by downloads.

## Gauntlet testing

- **Eight real clients.** uv, pip, poetry, pdm, pipenv, hatch, flit, and twine.
- **Every PyPI file.** [All 17 million](src/corpus_check.rs), checked against ground truth.
- **Killed mid-write.** Crash sweeps, fleet chaos, and hostile upstreams.
- **Fuzzed nightly. Simulated and model-checked.**
- **Security-audited by frontier models.** The same models that built it.

[See the full testing gauntlet.](docs/testing.md)

## Going further

- [Publish and install](docs/guides/publish-install.md) — private packages and the PyPI cache
- [Deploy on cloud storage](docs/guides/standard-cloud.md) — S3, GCS, or Azure
- [Migrate](docs/guides/migrate.md) — move from pypicloud, devpi, Artifactory, or Nexus
- [How pypiron works](docs/concepts.md) — storage, caching, access, and recovery
- [Configuration](docs/reference/configuration.md) — every flag and its `PYPIRON_*` env var
- [Compare servers](docs/compare/index.md) — benchmarks and when to choose something else
- [For AI agents](docs/for-agents.md) — decide, configure, and verify

## Contributing — [Humans Need Not Apply](https://www.youtube.com/watch?v=7Pq-S557XQU)

pypiron was built by AI coding agents from Anthropic, OpenAI, SpaceXAI, and Moonshot — and that's how it stays. All development is done by AI coders, for security and consistency: human-developed code is a security risk, and we don't accept it. Humans are welcome to open issues and contribute documentation.

## License

MIT — see [LICENSE](LICENSE).
