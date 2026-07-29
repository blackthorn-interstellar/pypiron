# Working in this repo

pypiron is a single-crate Rust PyPI server (index, upload, mirror, on-demand
proxy). Truth is files on disk/S3; indexes are regenerable views. One binary, no
database. The guiding bias is against complexity: the best code is no code.

## Where things live
```
src/        the crate (src/assets/ holds the bytes embedded in the binary)
tests/      Python blackbox suite — the real binary over HTTP
examples/   the vopr simulator + user-facing mirror config recipes
fuzz/       cargo-fuzz targets
docs/       the user manual (mkdocs site; docs/.overrides is theme, not a page)
dev/        everything contributor-facing: design/testing docs, bench/, ops/, scripts/
private/    a separate, gitignored git repo — see below
.local/     all regenerable local state (dev data dir, mkdocs build, tool caches)
```
Root-level files are the ones their tooling requires there. Keep it that way:
new scripts go in `dev/scripts/`, new contributor docs in `dev/`.

`private/` is its own git repo nested in the checkout and gitignored by this
one — unpublished strategy, competitive research, session history. Read and
edit it like any other file; commit with `git -C private`. Nothing there can
reach this repo's history. Anything a contributor needs to build, test, or
reason about pypiron belongs in `dev/` instead.

## Before you finish
- Run `make check` (format, `cargo check`, clippy `-D warnings`, Rust unit tests)
  and fix everything it reports. A change isn't done until it passes.
- Run `make test` for the full suite (Rust unit + Python blackbox) when you touch
  HTTP, storage, the worker, sync, or the proxy. `make help` lists every target.

## Builds stay in one target dir
Build into the repo's `target/`. Never point `--target-dir` / `CARGO_TARGET_DIR`
at `/tmp` or your session scratchpad. A fresh tree shares nothing with the
existing cache, costs ~600 MB for a bare debug build and grows past 10 GB once
incremental artifacts land, and it outlives the session that made it. Agents
spinning up a throwaway tree per attempt is how a dev machine's disk fills
overnight — it has happened here, 26 trees deep.

- Need to compare two commits (bisect, before/after benchmarks)? Use a git
  worktree, but keep the build in one reusable place:
  `CARGO_TARGET_DIR=$PWD/.local/target-alt cargo build`. `.local/` is gitignored
  and is where regenerable state belongs.
- Several agents sharing `target/` will serialize on cargo's lock. That is the
  cheap outcome — one wait beats a second full rebuild.
- Delete any scratch target dir when you're done with it. Don't leave it for
  whoever notices the disk is full.
- `target/` itself grows without bound; run `cargo clean` when it gets silly.
  Take its reported size with salt: `cargo clean --dry-run` and `du` both sum
  apparent file sizes, and on APFS cargo's uplifted artifacts are clones that
  share blocks — measured here, both cried 97 GiB where only 37 GB was real.
  You will reclaim less than the headline number says.

## Testing (see [dev/TESTING.md](dev/TESTING.md))
- Blackbox-first: the real binary, driven over HTTP by real `uv`/`pip`/`twine`.
  Add a blackbox test (`tests/*.py`) for any changed user-visible behavior.
- Rust unit tests (`#[cfg(test)]`) are for pure functions only — parsing,
  rendering, normalization. Anything touching I/O is tested blackbox.
- Prefer real clients and real packages over mocks; don't add a mock layer.
- S3 tests need Docker/MinIO and skip cleanly without it; the poetry/pdm/flit/
  hatch compat matrix runs via `make compat`, not on every change.

## Conventions
- Two doc trees: the user manual is the mkdocs-material site under `docs/`
  (published to GitHub Pages; build with `make docs`, preview with
  `make docs-serve`); contributor/architecture/benchmark docs live in `dev/`.
  Update the manual when you change user-visible behavior. Write it the house
  way — [dev/DOCS_STYLE.md](dev/DOCS_STYLE.md): for the user, not the builder
  (outcome over mechanism, no in-house jargon, happy-path first). A new manual
  page goes in BOTH `nav:` and the `llmstxt` plugin's `sections:` in
  mkdocs.yml — the second list doesn't track nav and a missing page fails
  silently. Every page carries a front-matter `description:` (≤160 chars).
- Architecture and the storage-layout contract: [dev/DESIGN.md](dev/DESIGN.md)
  ([dev/VISION.md](dev/VISION.md) is the one-pager). Don't invent storage-tree
  or sidecar variants.
- Standards support is behavior verified against real clients, not spec-shaped
  output.
- Every `--flag` is also a `PYPIRON_FLAG` env var; document new knobs in
  [docs/reference/configuration.md](docs/reference/configuration.md).
- Check `private/ROADMAP.md` (the nested private repo) before adding features —
  respect the "Not planned" list; don't re-litigate it.
- A shipped v1 with a named deferred phase gets a `private/ROADMAP.md` entry in
  the same session, or the deferral doesn't exist — deferrals without a roadmap
  home are silent deletions.
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
  are stamped by CI. See [RELEASE.md](dev/RELEASE.md).
