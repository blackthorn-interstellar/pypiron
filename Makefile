.PHONY: init init-rust init-python build dev run test test-rust test-python test-s3-real test-gcs-real perf compat check cargo-check af fmt lint audit coverage clean doc docs docs-serve docs-truth build-wheel release-notes fuzz fuzz-build vopr-soak help

SHELL := /bin/bash

.DEFAULT_GOAL := help

init: init-rust init-python  ## Setup development tools (both Rust and Python)

init-rust:  ## Setup Rust development tools
	rustup component add rustfmt
	rustup component add clippy
	cargo build

init-python:  ## Setup Python development environment
	uv sync --all-extras

build:  ## Build the project in release mode
	cargo build --release

dev:  ## Build the project in development mode
	cargo build

run:  ## Run a local dev server (./data, admin/secret, http://127.0.0.1:8080)
	cargo run --release -- \
		--bind-addr 127.0.0.1:8080 \
		--data-dir ./data \
		--admin-user admin \
		--admin-pass secret \
		--worker-interval-secs 1

test: test-rust test-python  ## Run all tests (perf/stress excluded)

test-rust:  ## Run Rust unit tests
	cargo test

test-python:  ## Run blackbox integration tests
	uv run -- pytest tests

test-s3-real:  ## Run the S3 blackbox suite against a REAL S3 bucket (set PYPIRON_TEST_S3_REAL_BUCKET + ambient AWS creds; the bucket is emptied around every test)
	# -n 0: the shared real bucket is wiped per test; parallel workers would corrupt each other.
	uv run -- pytest tests -m "s3 and not perf and not stress" -n 0

test-gcs-real:  ## Run the GCS round-trip against a REAL GCS bucket (set PYPIRON_TEST_GCS_REAL_BUCKET + ambient GCS creds; each test isolates under its own key prefix)
	uv run -- pytest tests -m "gcs and not perf and not stress"

perf:  ## Run performance benchmarks (builds release binary)
	# -n 0: xdist swallows -s and concurrent runs corrupt the timings.
	uv run -- pytest tests -m perf -s -n 0

compat:  ## Generate the client compatibility matrix
	# -n 0: compat results aggregate in-process; xdist workers can't feed the doc writer.
	uv run -- pytest tests -m "compat and not perf and not stress" --write-compat-doc -n 0

check: af cargo-check lint test-rust  ## Format, lint, and unit-test

cargo-check:  ## Check the project for compilation errors
	cargo check

af: fmt
fmt:  ## Format Rust (rustfmt) and Python (ruff: sort imports, then format)
	cargo fmt --all
	uv run -- ruff check --fix tests bench scripts
	uv run -- ruff format tests bench scripts

lint:  ## Run clippy and ruff lints
	cargo clippy --all-targets -- -D warnings
	uv run -- ruff check tests bench scripts

audit:  ## Scan Cargo.lock for security advisories (needs cargo-audit: cargo install cargo-audit)
	# Fails on any advisory except the two acknowledged below. Both are quick-xml
	# DoS bugs (quadratic attribute parsing; unbounded namespace-decl allocation),
	# reached only through object_store's S3/Azure/GCS XML-response parsing. That
	# XML is only ever parsed from the operator's own configured, trusted storage
	# backend's responses — never from client/request input — so neither DoS is
	# reachable from a pypiron request. The fix is quick-xml >= 0.41.0, but no
	# released object_store permits it yet: the latest (0.14.0) pins quick-xml to
	# ^0.40.1, still vulnerable. Bumping object_store therefore can't clear these.
	# Drop both ignores the moment an object_store release ships quick-xml >= 0.41.
	# Upstream (dev now lives in apache/arrow-rs-object-store):
	#   fix merged to main 2026-07-02  https://github.com/apache/arrow-rs-object-store/pull/785
	#   tracking issue (closed)       https://github.com/apache/arrow-rs-object-store/issues/787
	#   ships in 0.14.1, target Aug 2026  https://github.com/apache/arrow-rs-object-store/issues/761
	cargo audit --ignore RUSTSEC-2026-0194 --ignore RUSTSEC-2026-0195

coverage:  ## Rust unit-test line coverage summary (needs cargo-llvm-cov: cargo install cargo-llvm-cov)
	cargo llvm-cov --summary-only

clean:  ## Clean build artifacts
	cargo clean

doc:  ## Generate Rust API documentation
	cargo doc --no-deps

docs:  ## Build the user-facing docs site (mkdocs, strict)
	uv run --group docs -- mkdocs build --strict

docs-serve:  ## Live-preview the docs site at http://127.0.0.1:8000
	uv run --group docs -- mkdocs serve

docs-truth: dev  ## Advisory: flag/env/default drift between the CLI and configuration.md/config_template.toml (not in `check`)
	uv run -- python scripts/check_docs.py --bin target/debug/pypiron

build-wheel:  ## Build Python wheel (local smoke-testing; releases happen in CI via git tag)
	# Same as CI: rewrite the README's relative links/logo to absolute URLs so the
	# packaged metadata renders on PyPI, then restore the GitHub-relative file
	# (the trap runs even if the build fails).
	@cp README.md README.md.orig; \
	trap 'mv -f README.md.orig README.md' EXIT; \
	uv run -- python scripts/transform_readme.py --target pypi && \
	uv run -- maturin build --release

TO ?= HEAD
release-notes:  ## Preview release notes (TO=HEAD, optional FROM=vX.Y.Z TAG=vX.Y.Z)
	@uv run -- python scripts/release_notes.py $(if $(FROM),--from $(FROM),) --to $(TO) $(if $(TAG),--tag $(TAG),)

# Coverage-guided fuzzing of the input-parsing modules (needs nightly +
# `cargo install cargo-fuzz`). TARGET=fuzz_names|fuzz_wheel|fuzz_wheelzip|fuzz_render|
# fuzz_coremeta|fuzz_range, SECS overrides time.
FUZZ_TARGET ?= fuzz_render
FUZZ_SECS ?= 60
fuzz:  ## Run a fuzz target (FUZZ_TARGET=fuzz_render FUZZ_SECS=60)
	cargo +nightly fuzz run $(FUZZ_TARGET) -- -max_total_time=$(FUZZ_SECS)

fuzz-build:  ## Compile all fuzz targets (CI smoke test)
	cargo +nightly fuzz build

# Deterministic-simulation soak (dev/TESTING.md §"Deterministic simulation").
# Rotates through every topology (nodes 2-3, buckets 1-3, fault + crash-only),
# prints a progress heartbeat every minute, and on a failure logs the exact
# `--seed N` reproduce command and keeps exploring. Everything is also appended
# to vopr-soak.log so findings survive the terminal. Ctrl-C to stop. On macOS,
# `caffeinate -i make vopr-soak` keeps the box awake. VOPR_SECS timeboxes
# instead of running forever (exits non-zero if any seed failed).
VOPR_SECS ?=
vopr-soak:  ## Run the deterministic simulator continuously across rotating topologies (VOPR_SECS=n to timebox)
	set -o pipefail; cargo run --release --example vopr -- \
		$(if $(VOPR_SECS),--max-secs $(VOPR_SECS),--forever) \
		--rotate --recheck-every 500 --start-seed $$(date +%s) \
		2>&1 | tee -a vopr-soak.log

help:  ## Display this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
