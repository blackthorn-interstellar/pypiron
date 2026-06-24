# syntax=docker/dockerfile:1
#
# Runtime-only image: it just drops a prebuilt pypiron binary into a minimal
# base. NO `RUN` — so `docker buildx build --platform linux/<any-arch>` builds on
# an ordinary amd64 runner with NO QEMU (only RUN executes target-arch code).
# CI cross-compiles the per-arch binary and assembles the multi-arch manifest.
#
# Build context must contain: `pypiron` (the target-arch binary), a
# `ca-certificates.crt` bundle (outbound TLS to PyPI for sync/proxy), and an
# empty `data/` dir (distroless/scratch have no shell to mkdir it).
#
# BASE per arch (set by CI):
#   gcr.io/distroless/cc-debian13:nonroot  — glibc binaries (amd64, arm64,
#       arm/v7, ppc64le, s390x, riscv64). distroless ships certs + nonroot user.
#   scratch                                — fully static musl binaries for the
#       arches distroless has no image for (386, arm/v6).
ARG BASE=gcr.io/distroless/cc-debian13:nonroot
FROM ${BASE}

COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY pypiron /usr/local/bin/pypiron
COPY --chown=65532:65532 data /data

# 65532 is the distroless nonroot uid; on scratch it's just an unprivileged
# numeric uid (the binary never does a passwd lookup). SSL_CERT_FILE makes the
# cert bundle unambiguous on scratch, which has no default trust store.
ENV PYPIRON_DATA_DIR=/data \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
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
