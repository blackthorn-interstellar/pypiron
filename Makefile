.PHONY: init init-rust init-python build dev run test test-rust test-python perf compat check cargo-check af fmt lint clean doc publish build-wheel release-build help

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
fmt:  ## Format the code using rustfmt
	cargo fmt --all

lint:  ## Run clippy lints
	cargo clippy --all-targets -- -D warnings

clean:  ## Clean build artifacts
	cargo clean

doc:  ## Generate documentation
	cargo doc --no-deps

build-wheel:  ## Build Python wheel package
	uv run -- maturin build --release

release-build:  ## Build wheel with optimizations for release
	uv run -- maturin build --release --strip

publish:  ## Publish package to pypi.org
	uv run -- maturin publish

help:  ## Display this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
