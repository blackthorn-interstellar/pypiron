.PHONY: build test check fmt lint clean help test-integration test-e2e test-stress build-wheel build-sdist install-maturin release-build init-rust init-python test-rust test-python

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

build-wheel:  ## Build Python wheel package
	maturin build --release

release-build:  ## Build wheel with optimizations for release
	maturin build --release --strip

dev:  ## Build the project in development mode
	cargo build

test: test-rust test-python  ## Run all tests (both Rust and Python)

test-rust:  ## Run Rust unit tests
	cargo test

test-python:  ## Run Python tests
	uv run -- pytest tests/

test-simple: dev  ## Run simple integration tests (bash + pytest; S3 test may require Docker/MinIO)
	./tests/test_simple.sh

test-integration: dev  ## Run integration tests (bash + pytest; S3 test may require Docker/MinIO)
	./tests/test_simple.sh
	./tests/test_simple_s3.sh
	pytest -q tests/e2e -m "not stress"

check: af cargo-check lint

cargo-check:  ## Check the project for compilation errors
	cargo check

af: fmt
fmt:  ## Format the code using rustfmt
	cargo fmt --all

lint:  ## Run clippy lints
	cargo clippy -- -D warnings

clean:  ## Clean build artifacts
	cargo clean

doc:  ## Generate documentation
	cargo doc --no-deps


watch-test:  ## Run tests in watch mode
	cargo watch -x test

publish:  ## Publish package to pypi.org
	maturin publish

help:  ## Display this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' 