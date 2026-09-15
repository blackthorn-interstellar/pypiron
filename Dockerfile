# syntax=docker/dockerfile:1
#
# Runtime-only image: it just drops a prebuilt pypiron binary into a minimal
# base. NO `RUN` — so `docker buildx build --platform linux/<any-arch>` builds on
# an ordinary amd64 runner with NO QEMU (only RUN executes target-arch code).
# CI cross-compiles the per-arch binary, smoke-runs the image
# (dev/scripts/image-smoke.sh), and assembles the multi-arch manifest.
#
# Build context must contain: `pypiron` (the target-arch binary), a
# `ca-certificates.crt` bundle, and empty `data/` and `tmp/` dirs. Neither base
# has a shell to mkdir the dirs, and the upload/proxy spool defaults to /tmp —
# an image without it fails every upload and proxied fetch. The CA bundle is
# for the cloud-storage client (S3/GCS/Azure): it trusts only the system store,
# and without one it refuses to start. pypiron's own upstream client (proxy,
# sync, advisories) carries compiled-in roots and does not need it.
#
# BASE per arch (set by CI):
#   scratch                                 — fully static musl binaries; the
#       image IS the binary (amd64, arm64, arm/v7, 386, arm/v6).
#   gcr.io/distroless/base-nossl-debian13:nonroot — glibc and nothing else, for
#       the arches Rust has no tier-2 musl target for (ppc64le, s390x, riscv64).
ARG BASE=scratch
FROM ${BASE}

COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY pypiron /usr/local/bin/pypiron
COPY --chown=65532:65532 data /data
COPY --chown=65532:65532 tmp /tmp

# 65532 is the distroless nonroot uid; on scratch it's just an unprivileged
# numeric uid (the binary never does a passwd lookup).
ENV PYPIRON_DATA_DIR=/data
USER 65532:65532
EXPOSE 8080

# Self-contained probe (no curl/wget/shell here): the binary GETs its own
# /health. Absolute path because HEALTHCHECK does not run through ENTRYPOINT.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/pypiron", "healthcheck"]

# `pypiron` is the entrypoint, so `docker run IMAGE serve|sync|...` works and a
# bare run serves. Reads stay public until you set credentials.
ENTRYPOINT ["/usr/local/bin/pypiron"]
CMD ["serve"]
