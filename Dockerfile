# syntax=docker/dockerfile:1

# --- build stage -------------------------------------------------------------
# Alpine's Rust is musl-native, so a plain `cargo build` yields a fully static
# binary (musl targets default to crt-static) — no cross-compile, no musl-tools,
# no glibc-version skew with the runtime. docker.yml builds each arch on its own
# native runner, so `uname -m` here is always the target arch.
# >=1.88 needed: object_store 0.13.2 uses let-chains despite declaring MSRV 1.85.
FROM rust:1.90-alpine AS builder

# ring (pulled in via rustls) compiles a little C, so we need a C toolchain.
RUN apk add --no-cache build-base

WORKDIR /usr/src/pypiron
COPY . .

# `.git` is excluded from the build context, so build.rs can't ask git for the
# commit. CI passes it in via this arg; it lands in the binary's version string.
ARG PYPIRON_GIT_HASH=unknown
ENV PYPIRON_GIT_HASH=$PYPIRON_GIT_HASH

# Cache the cargo registry and target dir across builds (BuildKit cache mounts,
# persisted in CI via the gha cache backend). The static binary is copied out of
# the cached target dir in the same layer so it survives into the runtime stage;
# /data is pre-created here because distroless has no shell to mkdir it later.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/pypiron/target \
    cargo build --release --locked && \
    cp target/release/pypiron /pypiron && \
    mkdir -p /pypiron-data

# --- runtime stage -----------------------------------------------------------
# distroless static: ~2 MB, ships CA certs (outbound TLS to PyPI for sync/proxy
# upstreams) and tzdata and a nonroot user, nothing else — no shell, no package
# manager. The static musl binary needs no libc, so it runs here and on any
# Linux. The :nonroot tag runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /pypiron /usr/local/bin/pypiron
COPY --from=builder --chown=65532:65532 /pypiron-data /data

ENV PYPIRON_DATA_DIR=/data
EXPOSE 8080

# Self-contained probe (no curl/wget/shell in distroless): the binary GETs its
# own /health and exits nonzero when unhealthy. It reads PYPIRON_BIND_ADDR, so a
# port override is followed without editing this line. Absolute path because
# HEALTHCHECK does not run through ENTRYPOINT. start-period covers cold start.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/pypiron", "healthcheck"]

# `pypiron` is the entrypoint, so `docker run IMAGE serve|sync|...` works and a
# bare `docker run IMAGE` serves. /data is the default disk store; mount a volume
# there for persistence (object-storage deployments never touch it). Reads stay
# public until you set credentials. See `docker run IMAGE serve --help`.
ENTRYPOINT ["/usr/local/bin/pypiron"]
CMD ["serve"]
