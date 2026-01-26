"""Test DuckDB connection with AWS S3 and read Parquet files.

This script configures DuckDB to read data from S3.
Note: DuckDB's Iceberg extension does not natively support Glue catalog.
For Glue integration, use PyIceberg or dbt with Glue.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import duckdb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from src.monitoring import get_logger, setup_logging  # noqa: E402

# Setup logging
setup_logging()
logger = get_logger("scripts.test_duckdb_glue")


def main() -> None:
    """Test DuckDB connection with S3 and verify AWS credentials."""
    # Load environment variables from .env.exec-user
    env_file = Path(__file__).parent.parent / "terraform" / ".env.exec-user"
    if env_file.exists():
        load_dotenv(env_file)
        logger.info("env_file_loaded", env_file=str(env_file))
    else:
        logger.warning(
            "env_file_not_found",
            env_file=str(env_file),
            message="Using system environment variables",
        )

    # Retrieve credentials from environment variables
    access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    database_name = os.getenv("GLUE_DATABASE_NAME", "coding-projects-taradim-2026-iceberg")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
    bucket_name = "coding-projects-taradim-2026"

    # Verify credentials are present
    if not access_key_id or not secret_access_key:
        logger.error(
            "missing_credentials",
            message="AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be defined",
        )
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be defined in .env.exec-user"
        )

    logger.info(
        "configuration_loaded",
        database_name=database_name,
        region=aws_region,
        bucket_name=bucket_name,
        access_key_id_prefix=access_key_id[:10] if access_key_id else None,
    )

    # Step 1: Connect to DuckDB
    logger.info("step_1_connecting_duckdb")
    con = duckdb.connect()
    logger.info("duckdb_connected")

    # Step 2: Install extensions
    logger.info("step_2_installing_extensions")
    extensions = ["httpfs", "aws"]
    for ext in extensions:
        try:
            con.execute(f"INSTALL {ext}")
            logger.info("extension_installed", extension=ext)
        except Exception as e:
            logger.debug("extension_install_skipped", extension=ext, error=str(e))

    # Step 3: Load extensions
    logger.info("step_3_loading_extensions")
    for ext in extensions:
        con.execute(f"LOAD {ext}")
        logger.info("extension_loaded", extension=ext)

    # Step 4: Configure S3 credentials
    logger.info("step_4_configuring_s3_credentials")
    try:
        con.execute(
            f"""
            CREATE SECRET (
                TYPE S3,
                KEY_ID '{access_key_id}',
                SECRET '{secret_access_key}',
                REGION '{aws_region}'
            )
        """
        )
        logger.info("s3_secret_created")
    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg:
            logger.info("s3_secret_already_exists", message="Continuing...")
        else:
            logger.error("s3_secret_creation_failed", error=str(e))
            raise

    # Step 5: Test S3 connection by listing files
    logger.info("step_5_testing_s3_connection")
    test_path = f"s3://{bucket_name}/bronze/coingecko"

    try:
        # List parquet files in S3
        result = con.execute(
            f"""
            SELECT file FROM glob('{test_path}/**/*.parquet') LIMIT 10
        """
        ).fetchall()
        file_count = len(result)
        logger.info("s3_connection_success", file_count=file_count, path=test_path)

        if file_count > 0:
            logger.info("sample_files", files=[r[0] for r in result[:3]])
    except Exception as e:
        logger.warning(
            "s3_connection_test_failed",
            error=str(e),
            path=test_path,
            message="Path may not exist yet",
        )

    # Step 6: Test reading Parquet data from S3
    logger.info("step_6_testing_parquet_read")
    try:
        # Try to read parquet files
        result = con.execute(
            f"""
            SELECT COUNT(*) as row_count
            FROM read_parquet('{test_path}/**/*.parquet')
        """
        ).fetchone()
        row_count = result[0] if result else 0
        logger.info("parquet_read_success", row_count=row_count, path=test_path)
    except Exception as e:
        logger.warning(
            "parquet_read_skipped",
            error=str(e),
            message="No parquet files found or path does not exist",
        )

    # Step 7: Verify Glue database exists (using boto3)
    logger.info("step_7_verifying_glue_database")
    try:
        import boto3

        glue_client = boto3.client("glue", region_name=aws_region)
        response = glue_client.get_database(Name=database_name)
        logger.info(
            "glue_database_verified",
            database_name=database_name,
            location=response["Database"].get("LocationUri", "N/A"),
        )
    except Exception as e:
        logger.error(
            "glue_database_verification_failed",
            error=str(e),
            database_name=database_name,
        )

    logger.info(
        "configuration_complete",
        summary={
            "duckdb": "Connected",
            "s3": "Configured",
            "glue_database": database_name,
        },
        next_steps=[
            "Use PyIceberg to create Iceberg tables in Glue",
            "Use dbt-duckdb to transform Parquet to Iceberg",
            "Query Iceberg tables using iceberg_scan() in DuckDB",
        ],
    )


if __name__ == "__main__":
    main()
