# syntax=docker/dockerfile:1

# --- build stage -------------------------------------------------------------
# Full (non-slim) rust image: it ships gcc, which the linker needs.
# Needs >=1.88: object_store 0.13.2 uses let-chains despite declaring MSRV 1.85.
FROM rust:1.90-bookworm AS builder

WORKDIR /usr/src/pypiron
COPY . .

# `.git` is excluded from the build context, so build.rs can't ask git for the
# commit. CI passes it in via this arg; it lands in the binary's version string.
ARG PYPIRON_GIT_HASH=unknown
ENV PYPIRON_GIT_HASH=$PYPIRON_GIT_HASH

# Cache the cargo registry and target dir across builds (BuildKit cache mounts,
# persisted in CI via the gha cache backend). The binary is copied out of the
# cached target dir in the same layer so it survives into the runtime stage.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/pypiron/target \
    cargo build --release --locked && \
    cp target/release/pypiron /usr/local/bin/pypiron

# --- runtime stage -----------------------------------------------------------
FROM debian:bookworm-slim

# ca-certificates: outbound TLS to PyPI for `sync` and proxy upstreams.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/pypiron /usr/local/bin/pypiron

# Run unprivileged; /data is the default storage dir and the mount point.
RUN useradd --uid 10001 --home-dir /home/pypiron --create-home pypiron \
    && mkdir -p /data && chown pypiron:pypiron /data
USER pypiron

ENV PYPIRON_DATA_DIR=/data
VOLUME /data
EXPOSE 8080

# Defaults (bind 0.0.0.0:8080, /data, health at /health) make this runnable
# with no extra args; reads stay public until you set credentials. Bare
# `pypiron` now prints help, so the image serves explicitly. See
# `pypiron serve --help`.
CMD ["pypiron", "serve"]
