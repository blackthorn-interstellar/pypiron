.PHONY: build test check fmt lint clean help test-integration

SHELL := /bin/bash
CARGO := cargo

.DEFAULT_GOAL := help

build:  ## Build the project in release mode
	$(CARGO) build --release

dev:  ## Build the project in development mode
	$(CARGO) build

test: test-integration
	$(CARGO) test

check:  ## Check the project for compilation errors
	$(CARGO) check

af: fmt
fmt:  ## Format the code using rustfmt
	$(CARGO) fmt --all

lint:  ## Run clippy lints
	$(CARGO) clippy -- -D warnings

clean:  ## Clean build artifacts
	$(CARGO) clean

doc:  ## Generate documentation
	$(CARGO) doc --no-deps

init:  ## Setup development tools
	rustup component add rustfmt
	rustup component add clippy
	pip install -r integration_tests/requirements.txt
	$(CARGO) build

watch-test:  ## Run tests in watch mode
	$(CARGO) watch -x test

help:  ## Display this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' 