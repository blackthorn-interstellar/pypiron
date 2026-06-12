# Release Process

Versions are determined solely by git tags. `Cargo.toml` permanently says
`0.0.0`; when a `vX.Y.Z` tag is pushed, CI stamps the tag version into
`Cargo.toml`/`Cargo.lock` (`.github/stamp-version.sh`) before building, so the
wheels, sdist, and the binary's `--version` all pick it up. There is no
version-bump commit.

## Make a release

```bash
git tag v0.2.0
git push origin v0.2.0
```

That's it. CI runs fmt/clippy/tests, builds wheels for all platforms plus the
sdist, generates build-provenance attestations, and publishes to PyPI via
trusted publishing. Nothing is published if the tests fail.

Local and dev builds report version `0.0.0` — only tagged CI builds carry a
real version. `git describe --tags` tells you where a checkout sits relative
to releases.

## One-time setup: PyPI trusted publishing

The release job authenticates with OIDC; no API token is stored anywhere.
Configure once at https://pypi.org/manage/project/pypiron/settings/publishing/:

- Owner: `brycedrennan`
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
