"""Utilities for S3 partitioning by date and source."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

FileFormat = Literal["parquet", "json"]
Layer = Literal["bronze", "silver", "gold"]


def generate_s3_path(
    source: str,
    date: datetime | None = None,
    filename: str | None = None,
    format: FileFormat = "parquet",
    layer: Layer = "bronze",
) -> str:
    """Generate S3 path with partitioning structure.

    Structure: {layer}/{source}/{YYYY-MM-DD}/{filename}.{ext}

    Args:
        source: Data source name (e.g., 'coingecko', 'defillama', 'yahoo_finance')
        date: Date for partitioning (defaults to now if None)
        filename: Custom filename (auto-generated if None)
        format: File format ('parquet' or 'json')
        layer: Data layer ('bronze', 'silver', or 'gold')

    Returns:
        S3 path string (without bucket prefix)

    Example:
        >>> generate_s3_path('coingecko', datetime(2025, 1, 17), 'markets', 'parquet', 'bronze')
        'bronze/coingecko/2025-01-17/markets.parquet'
    """
    if date is None:
        date = datetime.utcnow()

    date_str = date.strftime("%Y-%m-%d")

    if filename is None:
        timestamp = date.strftime("%Y%m%d_%H%M%S")
        filename = f"{source}_{timestamp}"

    # Ensure filename has correct extension
    if not filename.endswith(f".{format}"):
        filename = f"{filename}.{format}"

    return f"{layer}/{source}/{date_str}/{filename}"


def parse_s3_path(s3_path: str) -> dict[str, str]:
    """Parse S3 path to extract components.

    Args:
        s3_path: S3 path (e.g., 'bronze/coingecko/2025-01-17/markets.parquet')

    Returns:
        Dictionary with keys: layer, source, date, filename, format
    """
    parts = s3_path.strip("/").split("/")

    if len(parts) < 4:
        raise ValueError(f"Invalid S3 path format: {s3_path}. Expected: layer/source/date/filename")

    layer = parts[0]
    source = parts[1]
    date_str = parts[2]
    filename = parts[3]

    # Extract format from filename
    if "." in filename:
        name, ext = filename.rsplit(".", 1)
        file_format = ext
    else:
        name = filename
        file_format = "unknown"

    return {
        "layer": layer,
        "source": source,
        "date": date_str,
        "filename": name,
        "format": file_format,
        "full_path": s3_path,
    }
