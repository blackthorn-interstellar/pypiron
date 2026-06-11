FROM rust:1.85 AS builder

WORKDIR /usr/src/pypiron
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/pypiron/target/release/pypiron /usr/local/bin/pypiron

ENV PYPIRON_DATA_DIR=/data
VOLUME /data

EXPOSE 8080
CMD ["pypiron"]
