set default-list := true

# Run lint and tests (run this before committing)
[parallel]
pre-commit: lint test

# Format Rust code
format:
	cargo fmt --all -- --config group_imports=StdExternalCrate

# Check formatting and run Clippy
lint:
	cargo fmt --all --check -- --config group_imports=StdExternalCrate
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run the test suite
test:
	cargo nextest run --workspace --all-features
	cargo test --doc --workspace --all-features

# Generate and open an HTML coverage report
coverage:
	cargo llvm-cov nextest --workspace --all-features --open

# Run the application
run:
	cargo run --package bairdi
