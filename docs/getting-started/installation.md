# Installation

pypiron is one self-contained binary. No runtime dependencies, no database. Pick how you want to get it.

## From PyPI

The `pypiron` package on PyPI is a [maturin](https://www.maturin.rs/)-built binary wheel — installing it drops a single executable on your `PATH`.

=== "uvx"

    Run without installing anything:

    ```bash
    uvx pypiron serve
    ```

=== "uv tool"

    ```bash
    uv tool install pypiron
    pypiron serve
    ```

=== "pipx"

    ```bash
    pipx install pypiron
    pypiron serve
    ```

=== "pip"

    ```bash
    pip install pypiron
    pypiron serve
    ```

## Container image

```bash
docker run -p 8080:8080 ghcr.io/blackthorn-interstellar/pypiron:latest pypiron serve
```

The image runs unprivileged, defaults its storage to `/data`, and exposes port 8080. Mount a volume at `/data` to persist packages between runs, or point it at object storage instead. See [Production](../guides/production.md) for S3, multi-node, and TLS.

!!! note
    Started with no credentials, the server is read-only and reads are public. Set a password to enable uploads. See [Authentication](../concepts/authentication.md).

## Verify

```bash
pypiron --version
```

List every flag for the server:

```bash
pypiron serve --help
```

Bare `pypiron` prints help. The other subcommands are `sync`, `verify`, and `resync` ([CLI reference](../reference/cli.md)). Every `--flag` also has a `PYPIRON_*` environment variable ([Configuration](../reference/configuration.md)).

## Build from source

You need a recent Rust toolchain (1.88+).

```bash
git clone https://github.com/blackthorn-interstellar/pypiron
cd pypiron
cargo build --release
./target/release/pypiron --version
```

## Next steps

- [First steps](first-steps.md) — start the server, publish, install.
- [Configuration](../reference/configuration.md) — every flag and env var.
