"""Read and display the latest parquet file from S3.

This script finds the most recent parquet file in the S3 bucket and displays
its contents as a DataFrame.
"""

import io
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import boto3  # noqa: E402
import pandas as pd  # noqa: E402

from src.config import Config  # noqa: E402
from src.monitoring import get_logger, setup_logging  # noqa: E402

# Setup logging
setup_logging()
logger = get_logger("scripts.read_parquet")


def find_latest_parquet_file() -> str | None:
    """Find the most recent parquet file in S3 bronze layer.

    Returns:
        str: S3 key of the most recent parquet file, or None if not found
    """
    s3_client = boto3.client("s3", region_name=Config.AWS_REGION)
    bucket_name = Config.S3_BUCKET_NAME

    # List all objects in bronze/coingecko/
    prefix = "bronze/coingecko/"
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        logger.warning("no_files_found", bucket=bucket_name, prefix=prefix)
        return None

    # Filter parquet files and sort by last modified
    parquet_files = [
        obj for obj in response["Contents"] if obj["Key"].endswith(".parquet")
    ]

    if not parquet_files:
        logger.warning("no_parquet_files_found", bucket=bucket_name, prefix=prefix)
        return None

    # Sort by last modified (most recent first)
    latest_file = max(parquet_files, key=lambda x: x["LastModified"])

    return latest_file["Key"]


def read_parquet_from_s3(s3_key: str) -> pd.DataFrame:
    """Read parquet file from S3.

    Args:
        s3_key: S3 key (path) of the parquet file

    Returns:
        pd.DataFrame: DataFrame containing the parquet data
    """
    s3_client = boto3.client("s3", region_name=Config.AWS_REGION)
    bucket_name = Config.S3_BUCKET_NAME

    # Download parquet file to memory
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    parquet_data = response["Body"].read()

    # Read parquet from bytes
    df = pd.read_parquet(io.BytesIO(parquet_data))

    return df


def main() -> None:
    """Main function to read and display latest parquet file."""
    Config.validate()

    logger.info(
        "searching_latest_parquet",
        bucket=Config.S3_BUCKET_NAME,
        prefix="bronze/coingecko/",
    )

    # Find latest file
    latest_key = find_latest_parquet_file()

    if not latest_key:
        logger.error("no_parquet_file_found")
        return

    logger.info("found_latest_file", s3_key=latest_key)

    # Read parquet file
    logger.info("reading_parquet_file", s3_key=latest_key)
    df = read_parquet_from_s3(latest_key)

    file_size_kb = df.memory_usage(deep=True).sum() / 1024
    logger.info(
        "file_loaded_successfully",
        rows=len(df),
        columns=len(df.columns),
        file_size_kb=round(file_size_kb, 2),
        s3_key=latest_key,
    )

    # Display all data
    logger.info("displaying_data_content")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", 50)
    logger.info("data_content", data=df.head(10).to_string())

    # Display summary statistics
    logger.info("displaying_summary_statistics")
    logger.info("summary_statistics", statistics=df.describe().to_string())

    # Display column info
    logger.info("displaying_column_information")
    logger.info("column_dtypes", dtypes=df.dtypes.to_string())


if __name__ == "__main__":
    main()
