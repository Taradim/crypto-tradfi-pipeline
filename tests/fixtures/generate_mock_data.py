"""Generate mock data from real API responses.

This script makes a real API call to CoinGecko and saves the response
as a JSON fixture file for use in tests.
"""

import json
import sys
from pathlib import Path

import requests

# Add project root to Python path to import src modules
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.monitoring import get_logger, setup_logging  # noqa: E402

# Setup structured logging (same as the rest of the project)
setup_logging()
logger = get_logger(__name__)

# Path to fixtures directory
FIXTURES_DIR = Path(__file__).parent
MOCK_DATA_FILE = FIXTURES_DIR / "coingecko_response.json"


def generate_coingecko_fixture() -> None:
    """Fetch real data from CoinGecko API and save as fixture."""
    logger.info("Fetching real data from CoinGecko API...")

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,  # Just a few records for testing
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "7d,14d,30d,200d,1y",
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Save to JSON file
        with open(MOCK_DATA_FILE, "w") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(
            f"Successfully saved {len(data)} records to {MOCK_DATA_FILE}",
        )
        logger.info(f"Fixture file size: {MOCK_DATA_FILE.stat().st_size} bytes")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from CoinGecko: {e}")
        raise


if __name__ == "__main__":
    generate_coingecko_fixture()
