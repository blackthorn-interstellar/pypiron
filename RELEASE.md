# Release Process

## Prerequisites

Set up PyPI API token as a GitHub secret:
1. Get token from https://pypi.org/manage/account/token/
2. Add to GitHub: Settings → Secrets and variables → Actions
3. Create secret named `PYPI_API_TOKEN`

## Build Locally

```bash
# Install maturin (first time only)
make install-maturin

# Build wheel
make build-wheel

# Test it
pip install target/wheels/pypiron-*.whl
pypiron --help
```

## Make a Release

1. Update version in `Cargo.toml` and `pyproject.toml`
2. Commit and push
3. Create and push tag:
   ```bash
   git tag -a v0.1.0 -m "Release 0.1.0"
   git push origin v0.1.0
   ```
4. GitHub Actions will automatically build and publish to PyPI
