"""Synchronize dbt models with AWS Glue Catalog.

This script reads Parquet files from S3 (Silver/Gold layers) and registers
them as tables in AWS Glue Catalog, enabling querying via Athena.

Usage:
    python scripts/glue/sync_glue_catalog.py [--database DATABASE] [--dry-run]

Environment variables required:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_DEFAULT_REGION
    - S3_BUCKET_NAME
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import Config  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Mapping from PyArrow types to Glue/Athena types
PYARROW_TO_GLUE_TYPE = {
    "int8": "tinyint",
    "int16": "smallint",
    "int32": "int",
    "int64": "bigint",
    "uint8": "smallint",
    "uint16": "int",
    "uint32": "bigint",
    "uint64": "bigint",
    "float": "float",
    "float16": "float",
    "float32": "float",
    "double": "double",
    "float64": "double",
    "bool": "boolean",
    "boolean": "boolean",
    "string": "string",
    "large_string": "string",
    "utf8": "string",
    "large_utf8": "string",
    "binary": "binary",
    "large_binary": "binary",
    "date32": "date",
    "date64": "date",
    "date32[day]": "date",
    "timestamp[us]": "timestamp",
    "timestamp[ns]": "timestamp",
    "timestamp[ms]": "timestamp",
    "timestamp[s]": "timestamp",
    "timestamp[us, tz=UTC]": "timestamp",
    "timestamp[ns, tz=UTC]": "timestamp",
}


def get_glue_type(pyarrow_type: str) -> str:
    """Convert PyArrow type to Glue/Athena type.

    Args:
        pyarrow_type: PyArrow type string.

    Returns:
        Glue/Athena compatible type string.
    """
    type_str = str(pyarrow_type).lower()

    # Direct mapping
    if type_str in PYARROW_TO_GLUE_TYPE:
        return PYARROW_TO_GLUE_TYPE[type_str]

    # Handle parameterized types
    if type_str.startswith("timestamp"):
        return "timestamp"
    if type_str.startswith("date"):
        return "date"
    if type_str.startswith("decimal"):
        return type_str  # Glue supports decimal(p,s) syntax
    if type_str.startswith("list"):
        return "array<string>"  # Simplified
    if type_str.startswith("struct"):
        return "string"  # Simplified - store as JSON string

    # Default fallback
    logger.warning(f"Unknown type '{pyarrow_type}', defaulting to 'string'")
    return "string"


def get_schema_from_s3_parquet(
    s3_client: Any,
    bucket: str,
    key: str,
) -> list[dict[str, str]]:
    """Read Parquet schema from S3 file.

    Args:
        s3_client: Boto3 S3 client.
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        List of column definitions with Name and Type.
    """
    import io

    # Download file to memory
    response = s3_client.get_object(Bucket=bucket, Key=key)
    parquet_data = response["Body"].read()

    # Read schema
    parquet_file = pq.ParquetFile(io.BytesIO(parquet_data))
    schema = parquet_file.schema_arrow

    columns = []
    for field in schema:
        glue_type = get_glue_type(field.type)
        columns.append({
            "Name": field.name,
            "Type": glue_type,
        })

    return columns


def create_or_update_glue_table(
    glue_client: Any,
    database: str,
    table_name: str,
    s3_location: str,
    columns: list[dict[str, str]],
    description: str = "",
    dry_run: bool = False,
) -> bool:
    """Create or update a Glue table.

    Args:
        glue_client: Boto3 Glue client.
        database: Glue database name.
        table_name: Table name.
        s3_location: S3 location of the data.
        columns: List of column definitions.
        description: Table description.
        dry_run: If True, only log what would be done.

    Returns:
        True if successful, False otherwise.
    """
    table_input = {
        "Name": table_name,
        "Description": description,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Compressed": True,
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "compressionType": "snappy",
            "typeOfData": "file",
        },
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would create/update table '{database}.{table_name}'")
        logger.info(f"  Location: {s3_location}")
        logger.info(f"  Columns: {[c['Name'] for c in columns]}")
        return True

    try:
        # Try to get existing table
        glue_client.get_table(DatabaseName=database, Name=table_name)

        # Table exists, update it
        logger.info(f"Updating existing table '{database}.{table_name}'")
        glue_client.update_table(
            DatabaseName=database,
            TableInput=table_input,
        )
        return True

    except glue_client.exceptions.EntityNotFoundException:
        # Table doesn't exist, create it
        logger.info(f"Creating new table '{database}.{table_name}'")
        glue_client.create_table(
            DatabaseName=database,
            TableInput=table_input,
        )
        return True

    except ClientError as e:
        logger.error(f"Failed to create/update table '{table_name}': {e}")
        return False


def ensure_database_exists(
    glue_client: Any,
    database: str,
    dry_run: bool = False,
) -> bool:
    """Ensure Glue database exists, create if not.

    Args:
        glue_client: Boto3 Glue client.
        database: Database name.
        dry_run: If True, only log what would be done.

    Returns:
        True if database exists or was created.
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would ensure database '{database}' exists")
        return True

    try:
        glue_client.get_database(Name=database)
        logger.info(f"Database '{database}' already exists")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        logger.info(f"Creating database '{database}'")
        glue_client.create_database(
            DatabaseInput={
                "Name": database,
                "Description": "Data pipeline portfolio - dbt models",
            }
        )
        return True
    except ClientError as e:
        logger.error(f"Failed to create database '{database}': {e}")
        return False


def sync_layer_to_glue(
    s3_client: Any,
    glue_client: Any,
    bucket: str,
    layer: str,
    database: str,
    dry_run: bool = False,
) -> int:
    """Sync all Parquet files in a layer to Glue.

    Args:
        s3_client: Boto3 S3 client.
        glue_client: Boto3 Glue client.
        bucket: S3 bucket name.
        layer: Layer name (silver, gold).
        database: Glue database name.
        dry_run: If True, only log what would be done.

    Returns:
        Number of tables synced.
    """
    prefix = f"{layer}/"
    tables_synced = 0

    try:
        # List objects in the layer
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response:
            logger.warning(f"No files found in s3://{bucket}/{prefix}")
            return 0

        for obj in response["Contents"]:
            key = obj["Key"]

            # Only process .parquet files
            if not key.endswith(".parquet"):
                continue

            # Extract table name from filename
            filename = key.split("/")[-1]
            table_name = filename.replace(".parquet", "")

            logger.info(f"Processing {layer}/{table_name}...")

            # Get schema from Parquet file
            try:
                columns = get_schema_from_s3_parquet(s3_client, bucket, key)
            except Exception as e:
                logger.error(f"Failed to read schema from {key}: {e}")
                continue

            # S3 location for Glue (directory, not file for partitioned data)
            # For single files, we point to the file's parent directory
            s3_location = f"s3://{bucket}/{layer}/"

            # Create/update Glue table
            description = f"dbt {layer} model - {table_name}"
            success = create_or_update_glue_table(
                glue_client=glue_client,
                database=database,
                table_name=table_name,
                s3_location=f"s3://{bucket}/{key}",  # Point to exact file
                columns=columns,
                description=description,
                dry_run=dry_run,
            )

            if success:
                tables_synced += 1

    except ClientError as e:
        logger.error(f"Failed to list objects in s3://{bucket}/{prefix}: {e}")

    return tables_synced


def sync_glue_catalog(
    database: str = "data_pipeline_portfolio",
    dry_run: bool = False,
) -> dict[str, int]:
    """Synchronize all dbt models to Glue Catalog.

    Args:
        database: Glue database name.
        dry_run: If True, only log what would be done.

    Returns:
        Dictionary with counts of tables synced per layer.
    """
    # Validate config
    Config.validate()

    bucket = Config.S3_BUCKET_NAME
    region = Config.AWS_REGION

    logger.info(f"Syncing Glue Catalog for bucket '{bucket}' in region '{region}'")
    logger.info(f"Target database: {database}")

    # Initialize AWS clients
    s3_client = boto3.client("s3", region_name=region)
    glue_client = boto3.client("glue", region_name=region)

    # Ensure database exists
    if not ensure_database_exists(glue_client, database, dry_run):
        return {"silver": 0, "gold": 0}

    results = {}

    # Sync each layer
    for layer in ["silver", "gold"]:
        logger.info(f"\n--- Syncing {layer.upper()} layer ---")
        count = sync_layer_to_glue(
            s3_client=s3_client,
            glue_client=glue_client,
            bucket=bucket,
            layer=layer,
            database=database,
            dry_run=dry_run,
        )
        results[layer] = count
        logger.info(f"Synced {count} tables from {layer} layer")

    return results


def main() -> int:
    """Main entry point for CLI usage."""
    parser = argparse.ArgumentParser(
        description="Sync dbt models to AWS Glue Catalog"
    )
    parser.add_argument(
        "--database",
        default="data_pipeline_portfolio",
        help="Glue database name (default: data_pipeline_portfolio)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show what would be done, don't make changes",
    )
    parser.add_argument(
        "--layer",
        choices=["silver", "gold", "all"],
        default="all",
        help="Which layer to sync (default: all)",
    )

    args = parser.parse_args()

    try:
        results = sync_glue_catalog(
            database=args.database,
            dry_run=args.dry_run,
        )

        total = sum(results.values())
        logger.info(f"\n=== Sync complete: {total} tables ===")
        for layer, count in results.items():
            logger.info(f"  {layer}: {count} tables")

        return 0 if total > 0 else 1

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
