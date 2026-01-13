"""Airflow DAG for CoinGecko cryptocurrency market data pipeline.

This DAG orchestrates the extraction, transformation, and loading of cryptocurrency
market data from CoinGecko API into S3 following the medallion architecture.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.config import Config
from src.monitoring import setup_logging
from src.pipelines.coingecko import CoinGeckoPipeline


def validate_config_task() -> None:
    """Validate configuration before pipeline execution.

    Raises:
        ValueError: If required configuration is missing.
    """
    Config.validate()


def run_coingecko_pipeline_task(**context: dict) -> str:
    """Execute the CoinGecko data pipeline.

    This task:
    1. Extracts market data from CoinGecko API
    2. Transforms the data into a structured format
    3. Loads the data to S3 in bronze layer

    Args:
        **context: Airflow task context (contains dag_run, execution_date, etc.)

    Returns:
        str: S3 path where the data was uploaded

    Raises:
        Exception: If pipeline execution fails.
    """
    # Setup logging
    setup_logging()

    # Get num_pages from Airflow variables or use default
    num_pages = context.get("dag_run").conf.get("num_pages", 2) if context.get("dag_run") else 2

    # Create and execute pipeline
    pipeline = CoinGeckoPipeline(num_pages=num_pages)
    s3_path = pipeline.run(layer="bronze")

    return s3_path


# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# Create the DAG
dag = DAG(
    "crypto_market_pipeline",
    default_args=default_args,
    description="Orchestrates CoinGecko cryptocurrency market data ingestion",
    schedule="0 2 * * *",  # Daily at 2:00 UTC
    catchup=False,
    tags=["crypto", "coingecko", "data-ingestion"],
)

# Task 1: Validate configuration
validate_config = PythonOperator(
    task_id="validate_config",
    python_callable=validate_config_task,
    dag=dag,
)

# Task 2: Run CoinGecko pipeline
run_pipeline = PythonOperator(
    task_id="run_coingecko_pipeline",
    python_callable=run_coingecko_pipeline_task,
    dag=dag,
)

# Define task dependencies
validate_config >> run_pipeline
