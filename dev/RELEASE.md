# Release Process

Versions are determined solely by git tags. `Cargo.toml` permanently says
`0.0.0`; when a `vX.Y.Z` tag is pushed, CI stamps the tag version into
`Cargo.toml`/`Cargo.lock` (`.github/stamp-version.sh`) before building, so the
wheels, sdist, and the binary's `--version` all pick it up. There is no
version-bump commit.

## Make a release

```bash
# Refresh the embedded Sigstore trust root used for offline PEP 740 verification.
# Rotates rarely; review the diff and commit any change before tagging.
./dev/scripts/fetch-trust-root.sh

make release-notes TO=HEAD
git tag v0.2.0
git push origin v0.2.0
```

That's it. The `make release-notes` step previews the GitHub Release body from
the commits since the last `vX.Y.Z` tag; CI regenerates the same notes from the
tag and uploads them with the Release. There is no checked-in changelog and no
version-bump commit.

CI runs fmt/clippy/tests, builds wheels for all platforms plus the sdist,
generates build-provenance attestations, and publishes to PyPI via trusted
publishing. Nothing is published if the tests fail.

### Embedded Sigstore trust root

pypiron verifies mirrored PEP 740 provenance offline against a Sigstore trust
root baked into the binary (`src/assets/trusted_root.json.gz`) — no TUF updater,
no runtime fetch. The root (Fulcio CA chain, Rekor and CT log keys, each with
validity windows) rotates rarely; `dev/scripts/fetch-trust-root.sh` re-embeds
the current one, and we run it at release time. This is **fail-safe by design**:
a bundle signed under a root *newer* than the one embedded — or in a log whose
key we don't yet carry — resolves to "not verified" and falls back to the
relayed + digest-bound labeling. It never fail-opens, so a stale embedded root
only shrinks verified coverage; it can never mislabel an unverified bundle as
verified.

The same per-target builds double as standalone binaries: CI pulls the compiled
executable out of each wheel (no second compile), and the `release-binaries` job
attaches them — `pypiron-<triple>.tar.gz`/`.zip` plus a `SHA256SUMS` manifest and
a provenance attestation — to the tag's **GitHub Release**. So one tag yields
three channels: PyPI wheels, GitHub Release binaries, and the GHCR image below.
A `workflow_dispatch` run builds the wheels and binary archives (as artifacts)
without creating a Release, for dry-running the matrix.

In parallel, `docker.yml` builds the multi-arch container image and pushes it to
GHCR — `ghcr.io/blackthorn-interstellar/pypiron:X.Y.Z`, `:X.Y`, and `:latest` —
with its own provenance attestation. Releases cover the full arch set (amd64,
arm64, arm/v7, ppc64le, s390x, riscv64, 386, arm/v6): each binary is
cross-compiled on an ordinary runner and `COPY`-ed into a minimal base (no QEMU —
the image stage runs no target-arch code), then the per-arch images are stitched
into one manifest. Ordinary pushes build just amd64+arm64 for a rolling `:master`
tag; every push also gets a `:sha-<short>` tag. No secrets or one-time setup are
needed: it authenticates with the built-in `GITHUB_TOKEN`.

Local and dev builds report version `0.0.0` — only tagged CI builds carry a
real version. `git describe --tags` tells you where a checkout sits relative
to releases.

## One-time setup: PyPI trusted publishing

The release job authenticates with OIDC; no API token is stored anywhere.
Configure once at https://pypi.org/manage/project/pypiron/settings/publishing/:

- Owner: `blackthorn-interstellar`
- Repository: `pypiron`
- Workflow: `ci.yml`
- Environment: (leave blank)

## Local build

```bash
make build-wheel        # wheel lands in target/wheels/
pip install target/wheels/pypiron-*.whl
pypiron --help
```

## Dry-running the pipeline

Trigger the workflow manually (`workflow_dispatch`) to build the full wheel
matrix without publishing — untagged builds carry version `0.0.0` and the
release job only runs for tags.
