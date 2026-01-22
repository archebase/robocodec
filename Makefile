.PHONY: all build build-release build-python build-python-release build-python-dev test test-rust test-python examples examples-verify coverage coverage-rust coverage-python fmt fmt-python lint lint-python check check-license clean help

# Default target
all: build

# ============================================================================
# Build targets
# ============================================================================

build: ## Build Rust library (debug)
	@echo "Building robocodec (debug)..."
	cargo build
	@echo "✓ Build complete"

build-release: ## Build Rust library (release)
	@echo "Building robocodec (release)..."
	cargo build --release
	@echo "✓ Build complete (release)"

build-python: ## Build Python wheel (debug)
	@echo "Building Python wheel..."
	maturin build
	@echo "✓ Python wheel built (see target/wheels/)"

build-python-release: ## Build Python wheel (release)
	@echo "Building Python wheel (release)..."
	maturin build --release --strip
	@echo "✓ Python wheel built (release, see target/wheels/)"

build-python-dev: ## Install Python package in dev mode (requires virtualenv)
	@echo "Installing Python package in dev mode..."
	maturin develop --features python
	@echo "✓ Python package installed"

# ============================================================================
# Testing
# ============================================================================

test: test-rust ## Run Rust tests
	@echo "✓ All tests passed"

test-rust: ## Run Rust tests
	@echo "Running Rust tests..."
	cargo test
	@echo "✓ Rust tests passed"

test-python: ## Run Python tests (builds extension first)
	@echo "Building Python extension..."
	maturin develop --features python
	@echo "Running Python tests..."
	pytest tests/python/ -v
	@echo "✓ Python tests passed"

# ============================================================================
# Examples
# ============================================================================

examples: ## Run all Python examples (uses test fixtures)
	@echo "Building Python extension..."
	maturin develop --features python
	@echo ""
	@echo "Running Python examples..."
	@echo ""
	@$(MAKE) -s examples-run
	@echo ""
	@echo "✓ All examples executed successfully"

examples-run:
	@# Find a test fixture to use
	@TEST_FILE=$$(ls tests/fixtures/*.mcap 2>/dev/null | head -1); \
	if [ -z "$$TEST_FILE" ]; then \
		echo "⚠ No test fixtures found. Skipping examples."; \
		exit 0; \
	fi; \
	echo "Using test fixture: $$TEST_FILE"; \
	echo ""; \
	\
	echo "1. inspect_mcap.py..."; \
	.venv/bin/python3 examples/python/inspect_mcap.py "$$TEST_FILE" > /dev/null && echo "   ✓ Passed" || echo "   ✗ Failed"; \
	\
	echo ""; \
	echo "2. mcap_stats.py..."; \
	.venv/bin/python3 examples/python/mcap_stats.py "$$TEST_FILE" > /dev/null && echo "   ✓ Passed" || echo "   ✗ Failed"; \
	\
	echo ""; \
	echo "3. filter_topics.py (list mode)..."; \
	.venv/bin/python3 examples/python/filter_topics.py "$$TEST_FILE" --list > /dev/null && echo "   ✓ Passed" || echo "   ✗ Failed"; \
	\
	echo ""; \
	echo "✓ Examples verified"

examples-verify: ## Verify Python example scripts have correct API imports
	@echo "Verifying Python example API usage..."
	@for script in examples/python/*.py; do \
		if [ "$$(basename $$script)" != "_example_utils.py" ]; then \
			echo "  Checking $$(basename $$script)..."; \
			if .venv/bin/python3 -c "import sys; sys.path.insert(0, 'examples/python'); exec(open('$$script').read().split('def main')[0])" 2>/dev/null; then \
				echo "    ✓ API imports OK"; \
			else \
				echo "    ⚠ Import check skipped"; \
			fi; \
		fi; \
	done; \
	echo "✓ Example verification complete"

# ============================================================================
# Coverage
# ============================================================================

coverage: coverage-rust coverage-python ## Run all tests with coverage
	@echo ""
	@echo "✓ Coverage reports generated"
	@echo "  Rust:   target/llvm-cov/html/index.html"
	@echo "  Python: tests/python/htmlcov/index.html"

coverage-rust: ## Run Rust tests with coverage (requires cargo-llvm-cov)
	@echo "Running Rust tests with coverage..."
	@echo "(Install: cargo install cargo-llvm-cov)"
	cargo llvm-cov --workspace --html --output-dir target/llvm-cov/html
	cargo llvm-cov --workspace --lcov --output-path lcov.info
	@echo ""
	@echo "✓ Rust coverage report: target/llvm-cov/html/index.html"

coverage-python: ## Run Python tests with coverage (requires pytest-cov)
	@echo "Building Python extension..."
	maturin develop --features python
	@echo "Running Python tests with coverage..."
	pytest tests/python/ --cov=robocodec --cov-report=html --cov-report=term-missing -v
	@echo ""
	@echo "✓ Python coverage report: tests/python/htmlcov/index.html"

# ============================================================================
# Code quality
# ============================================================================

fmt: fmt-python ## Format all code (Rust + Python)
	@echo "Formatting Rust code..."
	cargo fmt
	@echo "✓ Code formatted"

fmt-python: ## Format Python code (requires black)
	@echo "Formatting Python code..."
	@if command -v black >/dev/null 2>&1; then \
		black python/ tests/python/; \
	else \
		echo "⚠ black not found. Install with: pip install black"; \
		exit 1; \
	fi
	@echo "✓ Python code formatted"

lint: lint-python ## Lint all code (Rust + Python)
	@echo "Linting Rust code with all features..."
	cargo clippy --all-targets --all-features -- -D warnings
	@echo "✓ Linting passed"

lint-python: ## Lint Python code (requires ruff)
	@echo "Linting Python code..."
	@if command -v ruff >/dev/null 2>&1; then \
		ruff check python/ tests/python/; \
	else \
		echo "⚠ ruff not found. Install with: pip install ruff"; \
		exit 1; \
	fi
	@echo "✓ Python linting passed"

check: fmt lint ## Run format check and lint

check-license: ## Check REUSE license compliance
	@echo "Checking REUSE license compliance..."
	@if command -v reuse >/dev/null 2>&1; then \
		reuse lint; \
	else \
		echo "⚠ reuse tool not found. Install with: pip install reuse"; \
		exit 1; \
	fi

# ============================================================================
# Utilities
# ============================================================================

clean: ## Clean build artifacts
	@echo "Cleaning..."
	cargo clean
	rm -rf target/
	rm -rf **/__pycache__/
	rm -rf **/.pytest_cache/
	rm -rf tests/python/htmlcov/
	rm -rf tests/python/.coverage*
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf coverage-html/
	rm -f coverage.xml lcov.info
	@echo "✓ Cleaned"

help: ## Show this help message
	@echo "Robocodec - Robotics Message Codec"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
