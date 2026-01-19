"""Base pipeline class for data ingestion.

Provides common functionality for extracting, transforming, and loading data
from various sources into S3 following the medallion architecture.
"""

from __future__ import annotations

import io
import json
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.config import Config
from src.monitoring import bind_context, get_logger
from src.utils.partitioning import FileFormat, Layer, generate_s3_path


class BasePipeline(ABC):
    """Base class for all data ingestion pipelines.

    This abstract class provides the common structure and functionality for
    all data pipelines. Subclasses must implement the `extract()` method
    to define how data is fetched from their specific source.

    The pipeline follows the ETL pattern:
    - Extract: Fetch data from source (implemented by subclasses)
    - Transform: Convert to DataFrame (default implementation provided)
    - Load: Upload to S3 (default implementation provided)

    Attributes:
        source_name: Name of the data source (e.g., 'coingecko', 'dune')
        bucket_name: S3 bucket name for data storage
        aws_region: AWS region for S3 operations
        logger: Structured logger instance
        s3_client: Boto3 S3 client
        pipeline_id: Unique identifier for the current pipeline run
    """

    def __init__(
        self,
        source_name: str,
        bucket_name: str | None = None,
        aws_region: str | None = None,
    ) -> None:
        """Initialize the pipeline.

        Args:
            source_name: Name of the data source (e.g., 'coingecko', 'dune')
            bucket_name: S3 bucket name (defaults to Config.S3_BUCKET_NAME)
            aws_region: AWS region (defaults to Config.AWS_REGION)

        Raises:
            ValueError: If bucket_name is not provided and not in Config
        """
        self.source_name = source_name
        self.bucket_name = bucket_name or Config.S3_BUCKET_NAME
        self.aws_region = aws_region or Config.AWS_REGION

        if not self.bucket_name:
            raise ValueError("bucket_name must be defined")

        self.logger = get_logger(f"pipeline.{source_name}")
        self.s3_client = boto3.client("s3", region_name=self.aws_region)
        self.pipeline_id: str | None = None

    @abstractmethod
    def extract(self) -> pd.DataFrame | dict[str, Any] | list[dict[str, Any]]:
        """Extract data from the source.

        This method must be implemented by each specific pipeline subclass.
        It should fetch data from the API or data source and return it in
        a format that can be converted to a pandas DataFrame.

        Returns:
            Raw data from the source. Can be:
            - pd.DataFrame: Already formatted data
            - dict[str, Any]: Single record
            - list[dict[str, Any]]: Multiple records

        Raises:
            Exception: Any exception that occurs during data extraction
        """
        pass

    def transform(self, data: pd.DataFrame | dict[str, Any] | list[dict[str, Any]]) -> pd.DataFrame:
        """Transform raw data into a pandas DataFrame.

        Converts various input formats (dict, list of dicts, or DataFrame)
        into a standardized pandas DataFrame. Subclasses can override this
        method to add custom transformation logic.

        Args:
            data: Raw data from extract() method

        Returns:
            pandas DataFrame with the transformed data

        Raises:
            ValueError: If data type is not supported
        """
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        elif isinstance(data, list):
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    def load_to_s3(
        self,
        data: pd.DataFrame,
        layer: Layer = "bronze",
        date: datetime | None = None,
        filename: str | None = None,
        format: FileFormat = "parquet",
    ) -> str:
        """Load data to S3 bucket.

        Converts the DataFrame to the specified format (parquet or json)
        and uploads it to S3 following the medallion architecture path structure.

        Args:
            data: DataFrame to upload
            layer: Data lake layer ('bronze', 'silver', or 'gold')
            date: Date for partitioning (defaults to current UTC time)
            filename: Custom filename (auto-generated if None)
            format: File format ('parquet' or 'json')

        Returns:
            S3 path where the file was uploaded

        Raises:
            ValueError: If format is not 'parquet' or 'json'
            ClientError: If S3 upload fails
        """
        if date is None:
            date = datetime.now(UTC)

        # Generate S3 path
        s3_path = generate_s3_path(
            source=self.source_name,
            date=date,
            filename=filename,
            format=format,
            layer=layer,
        )

        # Convert DataFrame to bytes based on format
        if format == "parquet":
            buffer = io.BytesIO()
            # Try to convert with PyArrow, but fallback to JSON for problematic columns
            try:
                table = pa.Table.from_pandas(data, preserve_index=False)
                pq.write_table(table, buffer)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
                # If conversion fails due to complex/mixed types, convert problematic columns to JSON
                self.logger.debug(
                    "parquet_conversion_fallback",
                    error=str(e),
                    message="Converting complex types to JSON strings for Parquet compatibility",
                )
                # Convert complex types to JSON strings
                data_copy = data.copy()
                for col in data_copy.columns:
                    if data_copy[col].dtype == "object":
                        sample = data_copy[col].dropna()
                        if len(sample) > 0:
                            first_val = sample.iloc[0]
                            if isinstance(first_val, (dict, list)):
                                # Convert entire column to JSON strings
                                data_copy[col] = data_copy[col].apply(
                                    lambda x: json.dumps(x, default=str)
                                    if isinstance(x, (dict, list))
                                    else x
                                )
                # Retry conversion
                table = pa.Table.from_pandas(data_copy, preserve_index=False)
                pq.write_table(table, buffer)
            body = buffer.getvalue()
            content_type = "application/parquet"
        elif format == "json":
            json_str = data.to_json(orient="records", date_format="iso")
            body = json_str.encode("utf-8") if json_str else b"[]"
            content_type = "application/json"
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'parquet' or 'json'")

        # Upload to S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_path,
                Body=body,
                ContentType=content_type,
            )

            self.logger.info(
                "data_uploaded_to_s3",
                s3_path=s3_path,
                bucket=self.bucket_name,
                layer=layer,
                rows=len(data),
                format=format,
            )

            return s3_path

        except ClientError as e:
            self.logger.error(
                "s3_upload_failed",
                s3_path=s3_path,
                bucket=self.bucket_name,
                error=str(e),
                exc_info=True,
            )
            raise

    def run(
        self,
        layer: Layer = "bronze",
        date: datetime | None = None,
        filename: str | None = None,
        format: FileFormat = "parquet",
    ) -> str:
        """Run the complete pipeline (extract, transform, load).

        Orchestrates the entire ETL process:
        1. Extract data from source
        2. Transform to DataFrame
        3. Load to S3

        All steps are logged with structured logging and context binding.

        Args:
            layer: Data lake layer ('bronze', 'silver', or 'gold')
            date: Date for partitioning (defaults to current UTC time)
            filename: Custom filename (auto-generated if None)
            format: File format ('parquet' or 'json')

        Returns:
            S3 path where the file was uploaded

        Raises:
            Exception: Any exception that occurs during the pipeline execution
        """
        # Generate unique pipeline ID
        if date is None:
            date = datetime.now(UTC)
        self.pipeline_id = f"{self.source_name}_{date.strftime('%Y%m%d_%H%M%S')}"

        with bind_context(
            pipeline_id=self.pipeline_id,
            source=self.source_name,
            layer=layer,
            operation="data_ingestion",
        ):
            pipeline_start_time = time.time()

            self.logger.info(
                "pipeline_started",
                source=self.source_name,
                layer=layer,
            )

            try:
                # Extract
                extract_start_time = time.time()
                raw_data = self.extract()
                extract_time_ms = (time.time() - extract_start_time) * 1000

                self.logger.info(
                    "data_extracted",
                    source=self.source_name,
                    data_type=type(raw_data).__name__,
                    extract_time_ms=round(extract_time_ms, 2),
                )

                # Transform
                transform_start_time = time.time()
                df = self.transform(raw_data)
                transform_time_ms = (time.time() - transform_start_time) * 1000

                self.logger.info(
                    "data_transformed",
                    source=self.source_name,
                    rows=len(df),
                    columns=list(df.columns),
                    transform_time_ms=round(transform_time_ms, 2),
                )

                # Load
                load_start_time = time.time()
                s3_path = self.load_to_s3(
                    data=df,
                    layer=layer,
                    date=date,
                    filename=filename,
                    format=format,
                )
                load_time_ms = (time.time() - load_start_time) * 1000

                # Calculate total execution time
                total_time_ms = (time.time() - pipeline_start_time) * 1000

                # Calculate file size (approximate for parquet)
                file_size_bytes = len(df) * len(df.columns) * 8  # Rough estimate

                self.logger.info(
                    "pipeline_completed",
                    source=self.source_name,
                    s3_path=s3_path,
                    rows=len(df),
                    columns_count=len(df.columns),
                    extract_time_ms=round(extract_time_ms, 2),
                    transform_time_ms=round(transform_time_ms, 2),
                    load_time_ms=round(load_time_ms, 2),
                    total_time_ms=round(total_time_ms, 2),
                    file_size_bytes=file_size_bytes,
                    success=True,
                )

                return s3_path

            except Exception as e:
                total_time_ms = (time.time() - pipeline_start_time) * 1000
                self.logger.error(
                    "pipeline_failed",
                    source=self.source_name,
                    error=str(e),
                    total_time_ms=round(total_time_ms, 2),
                    success=False,
                    exc_info=True,
                )
                raise
