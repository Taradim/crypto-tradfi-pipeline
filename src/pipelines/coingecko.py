from __future__ import annotations

import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import Config
from src.pipelines.base import BasePipeline


class CoinGeckoPipeline(BasePipeline):
    """Pipeline for CoinGecko data."""

    def __init__(self, num_pages: int = 2, *args: Any, **kwargs: Any) -> None:
        """Initialize CoinGecko pipeline.

        Args:
            num_pages: Number of pages to fetch (default: 2)
            *args: Additional positional arguments for BasePipeline
            **kwargs: Additional keyword arguments for BasePipeline
        """
        super().__init__("coingecko", *args, **kwargs)
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = Config.COINGECKO_API_KEY
        self.num_pages = num_pages
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({"x-cg-pro-api-key": self.api_key})

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
        """Extract cryptocurrency market data from CoinGecko.

        Fetches market data for top cryptocurrencies sorted by market cap.
        Retrieves data from the number of pages specified in __init__ (default: 2).

        Returns:
            List of dictionaries containing market data for each cryptocurrency.
        """
        endpoint = "/coins/markets"
        url = f"{self.base_url}{endpoint}"

        all_data: list[dict[str, Any]] = []
        per_page = 250

        for page in range(1, self.num_pages + 1):
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "sparkline": False,
                "price_change_percentage": "7d,14d,30d,200d,1y",
            }

            try:
                self.logger.debug(
                    "coingecko_fetching_page",
                    page=page,
                    endpoint=endpoint,
                )

                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                page_data = response.json()

                if not isinstance(page_data, list):
                    self.logger.warning(
                        "coingecko_unexpected_response_format",
                        page=page,
                        data_type=type(page_data).__name__,
                    )
                    continue

                all_data.extend(page_data)

                self.logger.info(
                    "coingecko_page_fetched",
                    page=page,
                    status_code=response.status_code,
                    records_count=len(page_data),
                    total_records=len(all_data),
                )

                # Rate limiting: wait between requests (except for last page)
                if page < self.num_pages:
                    time.sleep(1)  # 1 second delay between pages

            except requests.exceptions.RequestException as e:
                self.logger.error(
                    "coingecko_extraction_failed",
                    page=page,
                    url=url,
                    params=params,
                    error=str(e),
                    exc_info=True,
                )
                raise

        self.logger.info(
            "coingecko_extraction_successful",
            endpoint=endpoint,
            total_pages=self.num_pages,
            total_records=len(all_data),
        )

        return all_data
