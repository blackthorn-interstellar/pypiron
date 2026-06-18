# Working in this repo

pypiron is a single-crate Rust PyPI server (index, upload, mirror, on-demand
proxy). Truth is files on disk/S3; indexes are regenerable views. One binary, no
database. The guiding bias is against complexity: the best code is no code.

## Before you finish
- Run `make check` (format, `cargo check`, clippy `-D warnings`, Rust unit tests)
  and fix everything it reports. A change isn't done until it passes.
- Run `make test` for the full suite (Rust unit + Python blackbox) when you touch
  HTTP, storage, the worker, sync, or the proxy. `make help` lists every target.

## Testing (see [docs/TESTING.md](docs/TESTING.md))
- Blackbox-first: the real binary, driven over HTTP by real `uv`/`pip`/`twine`.
  Add a blackbox test (`tests/*.py`) for any changed user-visible behavior.
- Rust unit tests (`#[cfg(test)]`) are for pure functions only — parsing,
  rendering, normalization. Anything touching I/O is tested blackbox.
- Prefer real clients and real packages over mocks; don't add a mock layer.
- S3 tests need Docker/MinIO and skip cleanly without it; the poetry/pdm/flit/
  hatch compat matrix runs via `make compat`, not on every change.

## Conventions
- Architecture and the storage-layout contract: [docs/DESIGN.md](docs/DESIGN.md)
  ([docs/VISION.md](docs/VISION.md) is the one-pager). Don't invent storage-tree
  or sidecar variants.
- Standards support is behavior verified against real clients, not spec-shaped
  output: [docs/STANDARDS.md](docs/STANDARDS.md).
- Every `--flag` is also a `PYPIRON_FLAG` env var; document new knobs in
  [docs/CONFIGURATION.md](docs/CONFIGURATION.md).
- Check [docs/ROADMAP.md](docs/ROADMAP.md) before adding features — respect the
  "rejected" list; don't re-litigate it.
- No `unwrap`/`expect`/`panic!` on a request or worker path; return errors with
  `anyhow` context. Catch specific errors, never a blanket match.
- Security is fail-closed: a half-configured credential refuses startup, secrets
  compare in constant time, private names never fall through to upstream.
- Storage mutations are write-to-tmp-then-rename on the same filesystem; keep
  them crash-safe.
- Don't add a dependency to avoid a few lines of code.

## Commits & releases
- Conventional commits; spell out `feature` (not `feat`). Bug-fix messages state
  the root cause and how it was addressed.
- The repo version stays `0.0.0`; real versions come from `vX.Y.Z` git tags and
  are stamped by CI. See [RELEASE.md](RELEASE.md).
