"""Query Parquet files from S3 using DuckDB.

This module provides a simple and elegant way to query Parquet files stored in S3
using DuckDB. It automatically discovers all Parquet files in a folder and allows
you to query them as if they were a single table.

The script follows Modern Data Stack 2026 practices:
- Uses DuckDB as the compute engine (lightweight, ARM64 compatible)
- Automatic file discovery (no manual registration needed)
- Simple SQL interface for data exploration

Example:
    python scripts/query_parquet_s3.py \\
        --source coingecko \\
        --query "SELECT COUNT(*) FROM coingecko_bronze"
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import duckdb  # noqa: E402

from src.config import Config  # noqa: E402
from src.monitoring import bind_context, get_logger, setup_logging  # noqa: E402

# Setup logging
setup_logging()
logger = get_logger("scripts.query_parquet")


class DuckDBS3Config(BaseSettings):
    """Configuration for DuckDB S3 access.

    Loads AWS credentials from environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=[".env.airflow", ".env"],
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    aws_access_key_id: Annotated[
        str,
        Field(
            description="AWS access key ID for S3 access",
            alias="AWS_ACCESS_KEY_ID",
        ),
    ]

    aws_secret_access_key: Annotated[
        str,
        Field(
            description="AWS secret access key for S3 access",
            alias="AWS_SECRET_ACCESS_KEY",
        ),
    ]

    aws_region: str = Field(
        default="eu-west-1",
        description="AWS region for S3 operations",
        alias="AWS_DEFAULT_REGION",
    )


class DuckDBQueryEngine:
    """DuckDB query engine for Parquet files in S3.

    Provides a simple interface to query Parquet files stored in S3 using DuckDB.
    Automatically configures S3 access and creates views for easy querying.

    Example:
        >>> engine = DuckDBQueryEngine()
        >>> engine.query("coingecko", "SELECT COUNT(*) FROM coingecko_bronze")
    """

    def __init__(self, config: DuckDBS3Config | None = None) -> None:
        """Initialize DuckDB query engine.

        Args:
            config: DuckDB S3 configuration (default: loads from environment)

        Raises:
            ValidationError: If AWS credentials are missing from environment
        """
        if config is None:
            try:
                self.config = DuckDBS3Config()
            except ValidationError as e:
                raise ValueError(
                    "AWS credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) "
                    "must be defined in environment variables"
                ) from e
        else:
            self.config = config
        self._connection: duckdb.DuckDBPyConnection | None = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection with S3 support.

        Returns:
            Configured DuckDB connection
        """
        if self._connection is None:
            self._connection = self._setup_connection()
        return self._connection

    def _setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Setup DuckDB connection with S3 extensions.

        Returns:
            Configured DuckDB connection

        Raises:
            ValueError: If AWS credentials are missing
        """
        con = duckdb.connect()

        # Install and load extensions
        logger.info("installing_duckdb_extensions")
        extensions = ["httpfs", "aws"]
        for ext in extensions:
            try:
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")
                logger.info("extension_loaded", extension=ext)
            except Exception as e:
                logger.warning("extension_install_skipped", extension=ext, error=str(e))

        # Configure S3 credentials
        logger.info("configuring_s3_credentials", region=self.config.aws_region)

        try:
            con.execute(
                f"""
                CREATE SECRET (
                    TYPE S3,
                    KEY_ID '{self.config.aws_access_key_id}',
                    SECRET '{self.config.aws_secret_access_key}',
                    REGION '{self.config.aws_region}'
                )
            """
            )
            logger.info("s3_secret_created")
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg:
                logger.info("s3_secret_already_exists")
            else:
                logger.error("s3_secret_creation_failed", error=str(e))
                raise

        return con

    def create_view(
        self,
        source: str,
        layer: str = "bronze",
        view_name: str | None = None,
    ) -> str:
        """Create a DuckDB view that reads all Parquet files from an S3 folder.

        The view automatically discovers all Parquet files matching the pattern:
        s3://bucket/{layer}/{source}/**/*.parquet

        Args:
            source: Data source name (e.g., 'coingecko', 'defillama')
            layer: Data layer ('bronze', 'silver', or 'gold')
            view_name: Name for the view (default: {source}_{layer})

        Returns:
            Name of the created view

        Raises:
            ValueError: If configuration is invalid
        """
        Config.validate()

        if view_name is None:
            view_name = f"{source}_{layer}"

        bucket_name = Config.S3_BUCKET_NAME
        s3_path = f"s3://{bucket_name}/{layer}/{source}/**/*.parquet"

        logger.info(
            "creating_parquet_view",
            view_name=view_name,
            s3_path=s3_path,
        )

        # Create view that reads all Parquet files (automatic discovery)
        self.connection.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{s3_path}')
        """
        )

        # Test the view to get row count
        result = self.connection.execute(
            f"SELECT COUNT(*) as total_rows FROM {view_name}"
        ).fetchone()
        row_count = result[0] if result else 0

        logger.info(
            "view_created",
            view_name=view_name,
            total_rows=row_count,
            s3_path=s3_path,
        )

        return view_name

    def execute_query(self, query: str) -> duckdb.DuckDBPyConnection:
        """Execute a SQL query and return the connection for result fetching.

        Args:
            query: SQL query to execute

        Returns:
            DuckDB connection with query result ready to fetch
        """
        logger.info("executing_query", query=query)
        return self.connection.execute(query)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("connection_closed")


def query_parquet_files(
    source: str,
    query: str | None = None,
    layer: str = "bronze",
) -> None:
    """Query Parquet files from S3 using DuckDB.

    This function creates a view that automatically discovers all Parquet files
    in the specified S3 folder and executes the provided query.

    Args:
        source: Data source name (e.g., 'coingecko', 'defillama')
        query: SQL query to execute (default: SELECT * LIMIT 10)
        layer: Data layer ('bronze', 'silver', or 'gold')

    Raises:
        ValueError: If configuration is invalid or query fails
    """
    Config.validate()

    logger.info("querying_parquet_files", source=source, layer=layer)

    engine = DuckDBQueryEngine()

    try:
        # Create view for the source
        view_name = engine.create_view(source, layer)

        # Prepare query
        if query is None:
            query = f"SELECT * FROM {view_name} LIMIT 10"
        else:
            # Replace placeholder or ensure view name is used
            query = query.replace("{view}", view_name)
            if view_name not in query:
                logger.warning(
                    "query_may_not_use_view",
                    view_name=view_name,
                    message="Make sure your query uses the correct table/view name",
                )

        # Execute query
        result_cursor = engine.execute_query(query)
        result_df = result_cursor.fetchdf()

        logger.info(
            "query_completed",
            rows_returned=len(result_df),
            columns=list(result_df.columns),
        )

        # Also print for user visibility
        print("\n" + "=" * 80)
        print(f"Query Results ({len(result_df)} rows):")
        print("=" * 80)
        print(result_df.to_string())
        print("=" * 80)
        print(f"\nView '{view_name}' is available for further queries.")
        print(f"Example: SELECT * FROM {view_name} WHERE ...")

    finally:
        engine.close()


def main() -> None:
    """Main function to query Parquet files from S3.

    Example usage:
        python scripts/query_parquet_s3.py --source coingecko
        python scripts/query_parquet_s3.py --source coingecko --query "SELECT COUNT(*) FROM coingecko_bronze"
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Query Parquet files from S3 using DuckDB. "
        "Automatically discovers all Parquet files in the specified folder."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Data source name (e.g., 'coingecko', 'defillama', 'yahoo_finance')",
    )
    parser.add_argument(
        "--layer",
        default="bronze",
        choices=["bronze", "silver", "gold"],
        help="Data layer (default: bronze)",
    )
    parser.add_argument(
        "--query",
        help="SQL query to execute (default: SELECT * LIMIT 10). "
        "Use {view} placeholder or the actual view name (e.g., coingecko_bronze).",
    )

    args = parser.parse_args()

    with bind_context(
        pipeline_id="query_parquet_s3",
        source=args.source,
        layer=args.layer,
        operation="parquet_query",
    ):
        try:
            query_parquet_files(
                source=args.source,
                query=args.query,
                layer=args.layer,
            )
        except Exception as e:
            logger.error("script_failed", error=str(e), exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
