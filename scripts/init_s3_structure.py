"""Initialize S3 bucket structure with Bronze/Silver/Gold layers.

This script creates marker files to establish the folder structure in S3.
In S3, folders don't exist - they're just prefixes. This script creates
empty marker files to make the structure visible in the AWS Console.
"""

from __future__ import annotations

import sys
from pathlib import Path

import boto3

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import Config  # noqa: E402
from src.monitoring import bind_context, get_logger  # noqa: E402


def init_s3_structure() -> None:
    """Initialize S3 bucket structure with Bronze/Silver/Gold layers."""
    # Validate config (loads .env.airflow and .env via src/config.py)
    Config.validate()
    bucket_name = Config.S3_BUCKET_NAME

    # Initialize logger with context
    logger = get_logger()
    with bind_context(
        pipeline_id="init_s3_structure",
        bucket_name=bucket_name,
        operation="s3_structure_initialization",
    ):
        # Initialize S3 client
        s3_client = boto3.client("s3", region_name=Config.AWS_REGION)

        # Use layers and sources from centralized config
        layers = Config.LAYERS
        sources = Config.DATA_SOURCES

        logger.info(
            "initializing_s3_structure",
            bucket_name=bucket_name,
            layers=layers,
            sources=sources,
            total_markers=len(layers) * len(sources),
        )

        created_count = 0
        errors = []

        for layer in layers:
            for source in sources:
                # Create a marker file to establish the folder structure
                marker_path = f"{layer}/{source}/.gitkeep"
                try:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=marker_path,
                        Body=b"",
                        ContentType="text/plain",
                    )
                    logger.debug(
                        "marker_file_created",
                        marker_path=marker_path,
                        layer=layer,
                        source=source,
                    )
                    created_count += 1
                except Exception as e:
                    error_msg = str(e)
                    errors.append({"path": marker_path, "error": error_msg})
                    logger.error(
                        "marker_file_creation_failed",
                        marker_path=marker_path,
                        layer=layer,
                        source=source,
                        error=error_msg,
                        exc_info=True,
                    )

        # Log summary
        logger.info(
            "s3_structure_initialized",
            created_count=created_count,
            error_count=len(errors),
            total_expected=len(layers) * len(sources),
        )

        if errors:
            logger.warning(
                "s3_structure_initialization_errors",
                errors=errors,
                error_count=len(errors),
            )


if __name__ == "__main__":
    init_s3_structure()
