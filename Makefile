.DEFAULT_GOAL := help

.PHONY: pre-commit check coverage e2e format run test help

pre-commit: check test

check:
	cargo fmt --all -- --check --config group_imports=StdExternalCrate
	cargo clippy --all-targets --all-features -- -D warnings
	cargo check --all-features

coverage:
	cargo llvm-cov --workspace --all-features --html --open

e2e:
	cargo build -p raccoon
	: "$${DICOM_FILE:?Set DICOM_FILE=/path/to/file.dcm before running make e2e}"
	RACCOON_BIN="$$(pwd)/target/debug/raccoon" cargo test --manifest-path tests/e2e/Cargo.toml --test dimse -- --ignored --nocapture --test-threads=1

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
	@echo "  e2e           - Run ignored DCMTK DIMSE integration tests"
	@echo "  run           - Run the application"
	@echo "  test          - Run the test suite"
	@echo "  help          - Show this help message"
