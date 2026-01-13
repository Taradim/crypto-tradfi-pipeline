"""Centralized configuration for the data pipeline.

Manages environment variables and application configuration.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Centralized configuration for the application."""

    # AWS Configuration
    AWS_REGION: str = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Path = Path(os.getenv("LOG_FILE", "logs/pipeline.log"))

    # Data Sources API Keys
    COINGECKO_API_KEY: str = os.getenv("COINGECKO_API_KEY", "")
    DUNE_API_KEY: str = os.getenv("DUNE_API_KEY", "")
    DEFILLAMA_API_KEY: str = os.getenv("DEFILLAMA_API_KEY", "")
    YAHOO_FINANCE_API_KEY: str = os.getenv("YAHOO_FINANCE_API_KEY", "")
    FRED_API_KEY: str = os.getenv("FRED_API_KEY", "")

    # Pipeline Configuration
    DATA_SOURCES: list[str] = [
        "coingecko",
        "dune",
        "defillama",
        "yahoo_finance",
        "fred",
    ]

    LAYERS: list[str] = ["bronze", "silver", "gold"]

    # File Format Defaults
    DEFAULT_FILE_FORMAT: str = "parquet"
    DEFAULT_LAYER: str = "bronze"

    @classmethod
    def validate(cls) -> None:
        """Validate that required configuration values are present.

        Raises:
            ValueError: If a required configuration value is missing or empty.

        Example:
            >>> Config.validate()  # Raises ValueError if S3_BUCKET_NAME is not set
        """
        if not cls.S3_BUCKET_NAME:
            raise ValueError("S3_BUCKET_NAME must be defined in environment variables")
