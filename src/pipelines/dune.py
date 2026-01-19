from __future__ import annotations

import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import Config
from src.pipelines.base import BasePipeline


class DunePipeline(BasePipeline):
    """Pipeline for Dune Analytics data."""

    def __init__(self, query_id: int | None = None, *args: Any, **kwargs: Any) -> None:
        """Initialize Dune Analytics pipeline.

        Args:
            query_id: Dune query ID to execute (optional)
            *args: Additional positional arguments for BasePipeline
            **kwargs: Additional keyword arguments for BasePipeline
        """
        super().__init__("dune", *args, **kwargs)
        self.base_url = "https://api.dune.com/api/v1"
        self.api_key = Config.DUNE_API_KEY
        self.query_id = query_id
        self.session = requests.Session()

        if not self.api_key:
            raise ValueError("DUNE_API_KEY must be configured to use Dune Analytics API")
        self.session.headers.update({"x-dune-api-key": self.api_key})

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
        """Extract data from Dune Analytics.

        Executes a Dune query and retrieves the results.
        If no query_id is provided, returns an empty list.

        Returns:
            List of dictionaries containing query results.

        Raises:
            ValueError: If query_id is not provided or API key is not configured
            requests.exceptions.RequestException: If API request fails
        """
        if not self.api_key:
            raise ValueError("DUNE_API_KEY must be configured to use Dune Analytics API")

        if not self.query_id:
            self.logger.warning(
                "dune_no_query_id", message="No query_id provided. Returning empty results."
            )
            return []

        # Execute query
        execute_url = f"{self.base_url}/query/{self.query_id}/execute"

        try:
            self.logger.debug(
                "dune_executing_query",
                query_id=self.query_id,
                endpoint=execute_url,
            )

            response = self.session.post(execute_url, timeout=self.timeout)
            response.raise_for_status()
            execution_data = response.json()

            execution_id = execution_data.get("execution_id")
            if not execution_id:
                raise ValueError(f"No execution_id returned: {execution_data}")

            self.logger.info(
                "dune_query_executed",
                query_id=self.query_id,
                execution_id=execution_id,
            )

            # Wait for query to complete
            status_url = f"{self.base_url}/execution/{execution_id}/status"
            max_wait_time = 300  # 5 minutes max wait
            wait_interval = 2  # Check every 2 seconds
            elapsed_time = 0

            while elapsed_time < max_wait_time:
                status_response = self.session.get(status_url, timeout=self.timeout)
                status_response.raise_for_status()
                status_data = status_response.json()

                state = status_data.get("state")
                if state == "QUERY_STATE_COMPLETED":
                    break
                elif state == "QUERY_STATE_FAILED":
                    error_msg = status_data.get("error", "Unknown error")
                    raise RuntimeError(f"Query execution failed: {error_msg}")

                time.sleep(wait_interval)
                elapsed_time += wait_interval

            if elapsed_time >= max_wait_time:
                raise TimeoutError(f"Query execution timed out after {max_wait_time} seconds")

            # Get results
            results_url = f"{self.base_url}/execution/{execution_id}/results"
            results_response = self.session.get(results_url, timeout=self.timeout)
            results_response.raise_for_status()
            results_data = results_response.json()

            rows = results_data.get("result", {}).get("rows", [])

            self.logger.info(
                "dune_extraction_successful",
                query_id=self.query_id,
                execution_id=execution_id,
                rows_count=len(rows),
            )

            return rows

        except requests.exceptions.RequestException as e:
            self.logger.error(
                "dune_extraction_failed",
                query_id=self.query_id,
                url=execute_url,
                error=str(e),
                exc_info=True,
            )
            raise
