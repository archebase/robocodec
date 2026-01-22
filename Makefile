.PHONY: all build build-release build-python build-python-release build-python-dev test test-rust test-python coverage coverage-rust coverage-python clippy fmt lint check check-license clean help

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

fmt: ## Format all code
	@echo "Formatting code..."
	cargo fmt
	@echo "✓ Code formatted"

lint: ## Lint all code
	@echo "Linting with all features..."
	cargo clippy --all-targets --all-features -- -D warnings
	@echo "✓ Linting passed"

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
