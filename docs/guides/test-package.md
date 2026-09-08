---
description: Test your built Python wheel in a temporary pypiron registry before publishing it, locally or in GitHub Actions.
---

# Test a package before publishing it

**Build a wheel, publish it to a temporary registry, and test what a user
actually installs.** This catches missing package files and dependencies that
tests against your source checkout can miss.

You need Git, Bash, curl, Python 3, and
[uv](https://docs.astral.sh/uv/getting-started/installation/). The example uses
Python 3.12; uv downloads it if necessary. Internet access supplies the tools,
build dependencies, and public packages. Nothing is published to public PyPI.

## Run the example

Clone pypiron, then run the example:

```bash
git clone https://github.com/blackthorn-interstellar/pypiron.git
cd pypiron
bash dev/scripts/package-ci.sh \
  examples/package-ci pypiron-ci-example examples/package-ci/check_installed.py
```

The script starts the published pypiron 0.0.17 on an available loopback port,
uploads the example wheel, and installs it plus `six` from that server into a
new environment. It disables the installer's cache and runs the check with
Python's isolated mode, so the source checkout cannot satisfy the import.

Success ends with:

```text
PASS: installed wheel, packaged resource, and public dependency
```

The server stops even when a check fails. The final `Run files:` path contains
the temporary package store, environment, and server log for inspection.

## Test your own package

Copy
[`package-ci.sh`](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/scripts/package-ci.sh)
into your project's `dev/scripts/` directory. At your project root, write a
`check_installed.py` that imports your installed package and checks something
that needs its packaged files or dependencies. For example, replace
`your_package` with your Python import name:

```python
from your_package import some_function

assert some_function() == "expected result"
```

Run this from the project root, replacing `your-distribution-name` with the
`[project].name` in `pyproject.toml`:

```bash
bash dev/scripts/package-ci.sh . your-distribution-name check_installed.py
```

The check uses only the installed wheel, its declared dependencies, and the
Python standard library. It does not install your development dependencies or
run the project's full test suite. Keep your existing tests too.

Public dependencies pass through pypiron's default seven-day release cooldown.
The wheel uploaded for this test is private and is available immediately.
This example expects any other dependencies to be available from public PyPI;
it does not migrate an existing private registry.

## Run in GitHub Actions

After copying the script and adding your installed-package check, save this as
`.github/workflows/package-smoke.yml`. Replace `your-distribution-name` in the
last line:

```yaml
--8<-- "examples/package-ci/github-actions.yml"
```

The job needs no publishing secrets. It generates a temporary credential and
uploads only to the server running on that job's loopback interface.

[Publish to a persistent server](publish-install.md) ·
[Star pypiron on GitHub](https://github.com/blackthorn-interstellar/pypiron)
