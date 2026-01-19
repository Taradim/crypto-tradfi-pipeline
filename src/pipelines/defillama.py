from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import Config
from src.pipelines.base import BasePipeline


class DeFiLlamaPipeline(BasePipeline):
    """Pipeline for DeFiLlama data."""

    def __init__(self, endpoint: str = "protocols", *args: Any, **kwargs: Any) -> None:
        """Initialize DeFiLlama pipeline.

        Args:
            endpoint: API endpoint to fetch ('protocols', 'tvl', 'chains', etc.)
            *args: Additional positional arguments for BasePipeline
            **kwargs: Additional keyword arguments for BasePipeline
        """
        super().__init__("defillama", *args, **kwargs)
        self.base_url = "https://api.llama.fi"
        self.api_key = Config.DEFILLAMA_API_KEY
        self.endpoint = endpoint
        self.session = requests.Session()

        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

        # Retry configuration for rate limiting and server errors
        self.session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504],
                )
            ),
        )
        self.timeout = 30  # Timeout for requests (in seconds)

    def extract(self) -> list[dict[str, Any]]:
        """Extract data from DeFiLlama API.

        Fetches data from the specified DeFiLlama endpoint.
        Common endpoints: 'protocols', 'tvl', 'chains', 'yields', etc.

        Returns:
            List of dictionaries containing DeFiLlama data.

        Raises:
            requests.exceptions.RequestException: If API request fails
        """
        url = f"{self.base_url}/{self.endpoint}"

        try:
            self.logger.debug(
                "defillama_fetching_data",
                endpoint=self.endpoint,
                url=url,
            )

            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            # Handle different response formats
            if isinstance(data, list):
                all_data = data
            elif isinstance(data, dict):
                # Some endpoints return a dict with data inside
                if "protocols" in data:
                    all_data = data["protocols"]
                elif "tvl" in data:
                    all_data = [data]  # Wrap single dict in list
                else:
                    all_data = [data]  # Wrap single dict in list
            else:
                self.logger.warning(
                    "defillama_unexpected_response_format",
                    endpoint=self.endpoint,
                    data_type=type(data).__name__,
                )
                all_data = []

            self.logger.info(
                "defillama_extraction_successful",
                endpoint=self.endpoint,
                records_count=len(all_data),
            )

            return all_data

        except requests.exceptions.RequestException as e:
            self.logger.error(
                "defillama_extraction_failed",
                endpoint=self.endpoint,
                url=url,
                error=str(e),
                exc_info=True,
            )
            raise

    def transform(self, data: pd.DataFrame | dict[str, Any] | list[dict[str, Any]]) -> pd.DataFrame:
        """Transform raw data into a pandas DataFrame.

        Converts various input formats. PyArrow (used for Parquet) can natively
        handle complex nested structures (lists, dicts) without JSON conversion.

        Args:
            data: Raw data from extract() method

        Returns:
            pandas DataFrame with the transformed data
        """
        # Use the base transform to convert to DataFrame
        # PyArrow will handle complex types natively when writing to Parquet
        return super().transform(data)
