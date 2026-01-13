# Test Suite

This directory contains unit and integration tests for the data pipeline project.

## Structure

```
tests/
├── __init__.py
├── unit/              # Unit tests
│   ├── test_base_pipeline.py
│   └── test_coingecko_pipeline.py
└── fixtures/          # Mock data and fixtures
    └── mock_data.py
```

## Running Tests

### Install test dependencies

```bash
uv sync --extra dev
```

### Run all tests

```bash
uv run pytest
```

### Run specific test file

```bash
uv run pytest tests/unit/test_base_pipeline.py
```

### Run with coverage

```bash
uv run pytest --cov=src --cov-report=html
```

### Run only unit tests

```bash
uv run pytest -m unit
```

### Run only integration tests

```bash
uv run pytest -m integration
```

## Test Markers

- `@pytest.mark.unit`: Unit tests (fast, no external dependencies)
- `@pytest.mark.integration`: Integration tests (may require external services)
- `@pytest.mark.slow`: Slow running tests

## Writing Tests

### Example Unit Test

```python
import pytest
from unittest.mock import MagicMock, patch

@pytest.mark.unit
class TestMyClass:
    def test_my_method(self):
        # Test implementation
        pass
```

### Using Fixtures

```python
from tests.fixtures.mock_data import mock_coingecko_response, mock_dataframe

def test_with_mock_data():
    data = mock_coingecko_response()  # Loads from saved JSON file
    df = mock_dataframe()  # Converts to DataFrame
    # Use in tests
```

### Generating Fixtures

Fixtures are generated from real API responses:

```bash
uv run tests/fixtures/generate_mock_data.py
```

This creates `tests/fixtures/coingecko_response.json` with real API data.

## Mocking External Services

- **S3**: Use `moto` library (already configured in tests)
- **APIs**: Use `pytest-mock` to mock `requests.Session`
- **Config**: Use `monkeypatch` to modify environment variables
