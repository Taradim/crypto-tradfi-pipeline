"""Mock data fixtures for testing.

Loads real API responses saved as JSON files for realistic test data.
"""

import json
from pathlib import Path
from typing import Any

import pandas as pd

# Path to fixtures directory
FIXTURES_DIR = Path(__file__).parent
COINGECKO_RESPONSE_FILE = FIXTURES_DIR / "coingecko_response.json"
DEFILLAMA_RESPONSE_FILE = FIXTURES_DIR / "defillama_response.json"


def mock_coingecko_response() -> list[dict[str, Any]]:
    """Load CoinGecko API response from saved JSON file.

    If the file doesn't exist, returns a minimal fallback dataset.
    To generate the file, run: uv run tests/fixtures/generate_mock_data.py

    Returns:
        List of dictionaries representing cryptocurrency market data.
    """
    if COINGECKO_RESPONSE_FILE.exists():
        with open(COINGECKO_RESPONSE_FILE) as f:
            return json.load(f)
    else:
        # Fallback minimal data if file doesn't exist
        return [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 45000.0,
                "market_cap": 850000000000,
                "market_cap_rank": 1,
                "total_volume": 25000000000,
                "high_24h": 46000.0,
                "low_24h": 44000.0,
                "circulating_supply": 19500000.0,
                "total_supply": 19500000.0,
                "last_updated": "2026-01-12T18:00:00.000Z",
            },
            {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "current_price": 3000.0,
                "market_cap": 360000000000,
                "market_cap_rank": 2,
                "total_volume": 15000000000,
                "high_24h": 3100.0,
                "low_24h": 2900.0,
                "circulating_supply": 120000000.0,
                "total_supply": 120000000.0,
                "last_updated": "2026-01-12T18:00:00.000Z",
            },
        ]


def mock_defillama_response() -> list[dict[str, Any]]:
    """Load DeFiLlama API response from saved JSON file.

    If the file doesn't exist, returns a minimal fallback dataset.
    To generate the file, run: uv run python -c "from src.pipelines.defillama import DeFiLlamaPipeline; import json; pipeline = DeFiLlamaPipeline(endpoint='protocols'); data = pipeline.extract(); sample = data[:5]; print(json.dumps(sample, indent=2))" > tests/fixtures/defillama_response.json

    Returns:
        List of dictionaries representing DeFi protocol data.
    """
    if DEFILLAMA_RESPONSE_FILE.exists():
        with open(DEFILLAMA_RESPONSE_FILE) as f:
            return json.load(f)
    else:
        # Fallback minimal data if file doesn't exist
        return [
            {
                "id": "1",
                "name": "Test Protocol",
                "slug": "test-protocol",
                "tvl": 1000000.0,
                "chain": "Ethereum",
                "symbol": "TEST",
                "category": "DEX",
                "change_1d": 1.5,
                "change_7d": -2.3,
            },
        ]


def mock_dataframe() -> pd.DataFrame:
    """Create a mock DataFrame for testing.

    Returns:
        pandas DataFrame with sample cryptocurrency data.
    """
    return pd.DataFrame(mock_coingecko_response())
