.PHONY: build test check fmt lint clean help test-integration build-wheel build-sdist install-maturin release-build

SHELL := /bin/bash

.DEFAULT_GOAL := help

build:  ## Build the project in release mode
	cargo build --release

build-wheel:  ## Build Python wheel package
	maturin build --release

release-build:  ## Build wheel with optimizations for release
	maturin build --release --strip

dev:  ## Build the project in development mode
	cargo build

test:  ## Run unit tests
	cargo test

test-integration:  ## Run integration tests (requires Docker)
	./test_simple.sh

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

init:  ## Setup development tools
	rustup component add rustfmt
	rustup component add clippy
	cargo build
	uv sync --all-extras

watch-test:  ## Run tests in watch mode
	cargo watch -x test

publish:  ## Publish package to pypi.org
	maturin publish

help:  ## Display this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' 