# <img src="docs/assets/pypiron-logo-256.png" alt="pypiron logo" width="40" style="vertical-align: middle;"/> pypiron

[![CI](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/blackthorn-interstellar/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-pypiron-bf5a2e.svg)](https://pypiron.com/)

An ultra-fast Python package server, written in Rust.

pypiron aims to be the fastest, most reliable PyPI server (and mirror) available.

**Documentation:** <https://pypiron.com/>

<p align="center">
  <img src="docs/assets/install-throughput.png" alt="Max sustained install throughput: pypiron vs bandersnatch, pypiserver, pypicloud, devpi, proxpi" width="760">
</p>

- **5–90× faster than any PyPI server.** 3,026 installs/s on 2 vCPU.
- **So robust a single server could handle all of PyPI's traffic.** PyPI averages ~100,000 requests/s — about 7,700 installs/s — and one 8-vCPU c7i.2xlarge clears that at pypiron's measured ~1,500 installs/s per vCPU.
- **Supply-chain quarantine, on by default.** New releases wait 7 days. Most attacks surface first.
- **Private and public, one URL.** A name is yours or PyPI's, never both. No dependency confusion.
- **Scales to a fleet.** Point any number of nodes at one bucket. No coordination.
- **Works with everything.** uv, pip, poetry, pdm, twine, pipenv, hatch, flit.
- **Download stats built in.**

## Quickstart

```bash
# 1. Start a server (serves http://localhost:8080) — native binary…
uvx pypiron serve --admin-pass "$ADMIN"

# …or in a container (storage at /data, built-in healthcheck):
docker run -p 8080:8080 -e PYPIRON_ADMIN_PASS="$ADMIN" \
  ghcr.io/blackthorn-interstellar/pypiron:latest

# 2. Publish
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password "$ADMIN" dist/*

# 3. Install
uv add --index http://localhost:8080/simple/ acme-widgets
```

Only `--admin-pass` set: writes need the admin credential, reads stay public.
pip, twine, and poetry equivalents: <https://pypiron.com/#quickstart>.

## Going further

- [Setup](docs/guides/setup.md) — private packages, public proxy, sync mirror, S3
- [Configuration](docs/reference/configuration.md) — every flag and its `PYPIRON_*` env var
- [Benchmarks](docs/reference/benchmarks.md) — how the numbers above were measured

## Alternatives

[bandersnatch](https://github.com/pypa/bandersnatch),
[pypiserver](https://github.com/pypiserver/pypiserver),
[pypicloud](https://github.com/stevearc/pypicloud),
[devpi](https://www.devpi.net/),
[proxpi](https://github.com/EpicWink/proxpi).

## License

MIT — see [LICENSE](LICENSE).
