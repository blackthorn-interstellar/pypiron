.PHONY: init init-rust init-python build dev run test test-rust test-python perf compat check cargo-check af fmt lint clean doc build-wheel fuzz fuzz-build help

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

perf:  ## Run performance benchmarks (builds release binary)
	uv run -- pytest tests -m perf -s

compat:  ## Generate the client compatibility matrix
	uv run -- pytest tests -m "compat and not perf and not stress" --write-compat-doc

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

clean:  ## Clean build artifacts
	cargo clean

doc:  ## Generate documentation
	cargo doc --no-deps

build-wheel:  ## Build Python wheel (local smoke-testing; releases happen in CI via git tag)
	# Same as CI: rewrite the README's relative links/logo to absolute URLs so the
	# packaged metadata renders on PyPI, then restore the GitHub-relative file
	# (the trap runs even if the build fails).
	@cp README.md README.md.orig; \
	trap 'mv -f README.md.orig README.md' EXIT; \
	uv run -- python scripts/transform_readme.py --target pypi && \
	uv run -- maturin build --release

# Coverage-guided fuzzing of the input-parsing modules (needs nightly +
# `cargo install cargo-fuzz`). TARGET=fuzz_names|fuzz_wheel|fuzz_wheelzip|fuzz_render|
# fuzz_coremeta|fuzz_range, SECS overrides time.
FUZZ_TARGET ?= fuzz_render
FUZZ_SECS ?= 60
fuzz:  ## Run a fuzz target (FUZZ_TARGET=fuzz_render FUZZ_SECS=60)
	cargo +nightly fuzz run $(FUZZ_TARGET) -- -max_total_time=$(FUZZ_SECS)

fuzz-build:  ## Compile all fuzz targets (CI smoke test)
	cargo +nightly fuzz build

help:  ## Display this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
