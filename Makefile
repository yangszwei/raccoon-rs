.DEFAULT_GOAL := help

.PHONY: pre-commit check coverage format run test help

pre-commit: check test

check:
	cargo fmt --all -- --check --config group_imports=StdExternalCrate
	cargo clippy --all-targets --all-features -- -D warnings
	cargo check --all-features

coverage:
	cargo llvm-cov --workspace --all-features --html --open

format:
	cargo fmt --all -- --config group_imports=StdExternalCrate

run:
	cargo run -p raccoon

test:
	cargo test --all-features

help:
	@echo "Available commands:"
	@echo "  pre-commit    - Run all checks and tests (run this before committing)"
	@echo "  check         - Run formatting, linting, and type checking"
	@echo "  coverage      - Generate & open HTML coverage report for the workspace"
	@echo "  run           - Run the application"
	@echo "  test          - Run the test suite"
	@echo "  help          - Show this help message"
