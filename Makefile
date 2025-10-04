.PHONY: build test bench demo clean fmt clippy ci

# Build the project
build:
	cargo build --release

# Run all tests
test:
	cargo test --all

# Run Python test suite
test-python: build
	@echo "=== Running Python Test Suite ==="
	python tests/run_all_tests.py

# Run quick tests (no slow/performance tests)
test-quick: build
	@echo "=== Running Quick Tests ==="
	cargo test --all
	python tests/run_all_tests.py --quick

# Run unit tests only
test-unit:
	@echo "=== Running Unit Tests ==="
	cargo test --lib
	python tests/run_all_tests.py --unit

# Run integration tests
test-integration: build
	@echo "=== Running Integration Tests ==="
	cargo test --test '*'
	python tests/run_all_tests.py --integration

# Run SQL compatibility tests
test-sql: build
	@echo "=== Running SQL Tests ==="
	python tests/run_all_tests.py --sql

# Organize legacy Python tests
test-organize:
	@echo "=== Organizing Legacy Tests ==="
	python tests/run_all_tests.py --organize

# Generate test coverage report
test-coverage:
	@echo "=== Generating Coverage Report ==="
	cargo tarpaulin --out Html
	python tests/run_all_tests.py --coverage

# Run benchmarks
bench:
	cargo bench --all

# Run the demo scenario
demo: build
	@echo "=== DriftDB Demo ==="
	@echo "Initializing database..."
	./target/release/driftdb init ./demo-data
	@echo "Creating orders table..."
	./target/release/driftdb sql -d ./demo-data -e 'CREATE TABLE orders (pk=id, INDEX(status, customer_id))'
	@echo "Ingesting 10k orders..."
	./target/release/driftdb ingest -d ./demo-data --table orders --file examples/demo/seed_orders.jsonl
	@echo "\n=== Running demo queries ==="
	@echo "\n1. Find paid orders:"
	./target/release/driftdb select -d ./demo-data --table orders --where 'status="paid"' --limit 3
	@echo "\n2. Time travel query (as of sequence 5000):"
	./target/release/driftdb select -d ./demo-data --table orders --where 'customer_id="CUST-007"' --as-of "@seq:5000" --limit 3
	@echo "\n3. Show drift history for order ORD-2025-000123:"
	./target/release/driftdb drift -d ./demo-data --table orders --key "ORD-2025-000123"
	@echo "\n4. Creating snapshot..."
	./target/release/driftdb snapshot -d ./demo-data --table orders
	@echo "\n5. Compacting table..."
	./target/release/driftdb compact -d ./demo-data --table orders
	@echo "\n6. Query after snapshot (faster):"
	./target/release/driftdb select -d ./demo-data --table orders --where 'status="paid"' --limit 3
	@echo "\n=== Demo complete! ==="

# Clean build artifacts and demo data
clean:
	cargo clean
	rm -rf ./demo-data

# Format code
fmt:
	cargo fmt --all

# Run clippy linter
clippy:
	cargo clippy --all -- -D warnings

# CI pipeline (format check, clippy, tests)
ci: fmt clippy test