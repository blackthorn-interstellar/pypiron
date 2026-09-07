---
description: Start pypiron, publish a private package, and install private and public packages with uv, pip, or Poetry.
---

# Publish and install packages

This guide starts a local server, publishes a package, and installs it from a
fresh project. You need [uv](https://docs.astral.sh/uv/getting-started/installation/).

If a quickstart server is already using port 8080, stop it with ++ctrl+c++
before starting this walkthrough.

## Start the server

In one terminal:

```bash
mkdir pypiron-demo
cd pypiron-demo

export ADMIN='change-this-password'
PYPIRON_DATA_DIR=./data PYPIRON_ADMIN_PASS="$ADMIN" \
  uvx pypiron serve \
  --private-prefix hello \
  --proxy-upstream https://pypi.org
```

Leave it running. `--private-prefix hello` reserves `hello` and `hello-*` for
your uploads. The proxy supplies other packages from public PyPI.

In a second terminal, check the server:

```bash
cd pypiron-demo
export ADMIN='change-this-password'
curl -fsS http://localhost:8080/health
```

Success returns:

```json
{"status":"ok"}
```

## Build and publish a package

Create a small package for this walkthrough:

```bash
uv init --lib hello-pypiron
cd hello-pypiron
uv build
```

Publish the files in `dist/`:

```bash
export UV_PUBLISH_USERNAME=admin
export UV_PUBLISH_PASSWORD="$ADMIN"
uv publish --publish-url http://localhost:8080/legacy/ dist/*
```

Open [http://localhost:8080/project/hello-pypiron](http://localhost:8080/project/hello-pypiron)
to see the release.

## Install it

Create a separate project and add the private package plus a public one:

```bash
cd ..
uv init consumer
cd consumer
uv add --default-index http://localhost:8080/simple/ hello-pypiron six
uv run python -c 'import hello_pypiron, six; print(six.__version__)'
```

`uv add` saves pypiron as the project's default index. Keep pypiron as the
only index: the proxy already serves public packages, and adding PyPI as a
second index reopens dependency-confusion risk.

## Use pip or Poetry

pip:

```bash
uv venv --seed pip-demo
PIP_INDEX_URL=http://localhost:8080/simple/ \
  pip-demo/bin/python -m pip install hello-pypiron
```

In an existing Poetry project:

```bash
uvx --from poetry poetry source add \
  --priority=primary pypiron http://localhost:8080/simple/
uvx --from poetry poetry add --source pypiron hello-pypiron
```

To use Twine instead of the earlier `uv publish` command, run this from the
`hello-pypiron` package directory:

```bash
cd ../hello-pypiron
export TWINE_USERNAME=admin
export TWINE_PASSWORD="$ADMIN"
uvx twine upload \
  --repository-url http://localhost:8080/legacy/ dist/*
```

Do not run both publish commands for the same files: pypiron refuses to replace
an existing filename.

## Add production credentials

The walkthrough uses the admin password for setup. In production, give
publishers the uploader credential and set a reader credential only if installs
must require authentication. See [Configuration](../reference/configuration.md#server)
for the environment variables and [Core concepts](../concepts.md#who-can-do-what)
for the roles.

Next: [deploy on cloud storage](standard-cloud.md).
