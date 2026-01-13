.PHONY: test test-unit test-integration test-cov install-dev clean

# Install development dependencies
install-dev:
	uv sync --extra dev

# Run all tests
test:
	uv run pytest

# Run unit tests only
test-unit:
	uv run pytest -m unit

# Run integration tests only
test-integration:
	uv run pytest -m integration

# Run tests with coverage
test-cov:
	uv run pytest --cov=src --cov-report=html --cov-report=term

# Clean test artifacts
clean:
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf __pycache__
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
