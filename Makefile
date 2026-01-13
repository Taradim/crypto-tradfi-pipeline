.PHONY: test test-unit test-integration test-cov install-dev clean setup-env

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

# Airflow: Configure AIRFLOW_UID in .env.airflow and create .env symlink
setup-env:
	@if [ ! -f .env.airflow ]; then \
		echo "Error: .env.airflow not found. Please create it from env.airflow.example"; \
		exit 1; \
	fi
	@if grep -q "^AIRFLOW_UID=" .env.airflow; then \
		sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env.airflow; \
		echo "Updated AIRFLOW_UID to $$(id -u) in .env.airflow"; \
	else \
		echo "AIRFLOW_UID=$$(id -u)" >> .env.airflow; \
		echo "Added AIRFLOW_UID=$$(id -u) to .env.airflow"; \
	fi
	@if [ -f .env ]; then \
		echo ".env already exists, skipping symlink creation"; \
	else \
		ln -s .env.airflow .env; \
		echo "Created .env symlink from .env.airflow"; \
	fi
