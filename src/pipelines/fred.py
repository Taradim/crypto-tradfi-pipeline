from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import Config
from src.pipelines.base import BasePipeline


class FREDPipeline(BasePipeline):
    """Pipeline for FRED (Federal Reserve Economic Data)."""

    def __init__(
        self,
        series_ids: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize FRED pipeline.

        Args:
            series_ids: List of FRED series IDs to fetch (e.g., ['GDP', 'UNRATE', 'CPIAUCSL'])
                       If None, uses default list of common economic indicators
            start_date: Start date in YYYY-MM-DD format (defaults to 1 year ago)
            end_date: End date in YYYY-MM-DD format (defaults to today)
            *args: Additional positional arguments for BasePipeline
            **kwargs: Additional keyword arguments for BasePipeline
        """
        super().__init__("fred", *args, **kwargs)
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
        self.api_key = Config.FRED_API_KEY

        # Default series if none provided
        self.series_ids = series_ids or ["GDP", "UNRATE", "CPIAUCSL", "FEDFUNDS", "DGS10"]

        # Default dates
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

        self.start_date = start_date
        self.end_date = end_date

        self.session = requests.Session()

        if not self.api_key:
            self.logger.warning("fred_api_key_missing", message="FRED_API_KEY not configured")

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
        """Extract economic data from FRED API.

        Fetches observations for the specified series IDs.

        Returns:
            List of dictionaries containing FRED observations.

        Raises:
            ValueError: If API key is not configured
            requests.exceptions.RequestException: If API request fails
        """
        if not self.api_key:
            raise ValueError("FRED_API_KEY must be configured to use FRED API")

        all_data: list[dict[str, Any]] = []

        for series_id in self.series_ids:
            try:
                self.logger.debug(
                    "fred_fetching_series",
                    series_id=series_id,
                    start_date=self.start_date,
                    end_date=self.end_date,
                )

                params = {
                    "series_id": series_id,
                    "api_key": self.api_key,
                    "file_type": "json",
                    "observation_start": self.start_date,
                    "observation_end": self.end_date,
                }

                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                data = response.json()

                observations = data.get("observations", [])

                # Add series_id to each observation
                for obs in observations:
                    obs["series_id"] = series_id

                all_data.extend(observations)

                self.logger.info(
                    "fred_series_fetched",
                    series_id=series_id,
                    observations_count=len(observations),
                )

            except requests.exceptions.RequestException as e:
                self.logger.error(
                    "fred_series_failed",
                    series_id=series_id,
                    error=str(e),
                    exc_info=True,
                )
                # Continue with other series instead of failing completely
                continue

        if not all_data:
            raise ValueError("No data extracted for any series")

        self.logger.info(
            "fred_extraction_successful",
            series_ids=self.series_ids,
            total_observations=len(all_data),
        )

        return all_data
