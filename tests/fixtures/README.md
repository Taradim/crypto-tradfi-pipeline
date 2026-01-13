# Test Fixtures

This directory contains mock data and fixtures for testing.

## Generating Fixtures

### CoinGecko Response

To generate a real API response fixture:

```bash
uv run tests/fixtures/generate_mock_data.py
```

This will:
1. Make a real API call to CoinGecko
2. Save the response to `coingecko_response.json`
3. Use this file in tests for realistic data

## Files

- `mock_data.py`: Functions to load mock data
- `generate_mock_data.py`: Script to generate fixtures from real API
- `coingecko_response.json`: Generated fixture (gitignored, can be regenerated)

## Usage in Tests

```python
from tests.fixtures.mock_data import mock_coingecko_response, mock_dataframe

def test_my_function():
    data = mock_coingecko_response()
    df = mock_dataframe()
    # Use in tests
```

## Regenerating Fixtures

If the API response format changes, regenerate the fixture:

```bash
uv run tests/fixtures/generate_mock_data.py
```
