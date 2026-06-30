# pypiron

An ultra-fast Python package server, written in Rust.

pypiron aims to be the fastest, most reliable PyPI server (and mirror) available.

![Max sustained install throughput](assets/install-throughput.svg#only-light)
![Max sustained install throughput](assets/install-throughput-dark.svg#only-dark)

- **5–90× faster than any PyPI server.** 3,026 installs/s on 2 vCPU. ([benchmarks](reference/benchmarks.md))
- **So robust a single server could handle all of PyPI's traffic.**
- **Supply-chain quarantine, on by default.** New releases wait 7 days. Most attacks surface first. ([how](concepts/supply-chain.md))
- **Private and public, one URL.** A name is yours or PyPI's, never both. No dependency confusion.
- **Scales to a fleet.** Point any number of nodes at one bucket. No coordination.
- **Works with everything.** uv, pip, poetry, pdm, twine, pipenv, hatch, flit.
- **Download stats built in.** ([details](concepts/download-stats.md))

## Quickstart

### Start a server

Serves `http://localhost:8080`:

=== "uv"

    ```bash
    uvx pypiron serve --admin-pass secret
    ```

=== "pip"

    ```bash
    pip install pypiron
    pypiron serve --admin-pass secret
    ```

=== "poetry"

    ```bash
    poetry add pypiron
    poetry run pypiron serve --admin-pass secret
    ```

=== "binary"

    ```bash
    # Linux x86_64 — see the releases page for other platforms
    curl -LO https://github.com/blackthorn-interstellar/pypiron/releases/latest/download/pypiron-x86_64-unknown-linux-musl.tar.gz
    tar xzf pypiron-x86_64-unknown-linux-musl.tar.gz
    ./pypiron serve --admin-pass secret
    ```

=== "docker"

    ```bash
    docker run -p 8080:8080 -e PYPIRON_ADMIN_PASS=secret \
      ghcr.io/blackthorn-interstellar/pypiron:latest
    ```

### Publish a package

=== "uv"

    ```bash
    uv publish --publish-url http://localhost:8080/legacy/ \
      --username admin --password secret dist/*
    ```

=== "twine"

    ```bash
    twine upload --repository-url http://localhost:8080/legacy/ \
      -u admin -p secret dist/*
    ```

=== "poetry"

    ```bash
    poetry config repositories.pypiron http://localhost:8080/legacy/
    poetry publish --repository pypiron -u admin -p secret
    ```

### Install a package
=== "uv"

    ```bash
    uv add --index http://localhost:8080/simple/ acme-widgets
    ```

=== "pip"

    ```bash
    pip install --extra-index-url http://localhost:8080/simple/ acme-widgets
    ```

=== "poetry"

    ```bash
    poetry source add pypiron http://localhost:8080/simple/
    poetry add acme-widgets
    ```

## Next steps

<div class="grid cards" markdown>

- :material-lightbulb: __How it works__ — why it's fast ([How it works](concepts/how-it-works.md))
- :material-server-network: __Deploy__ — production setups ([Deploy](guides/deploy.md))
- :material-cog: __Configuration__ — every flag ([Configuration](reference/configuration.md))

</div>
