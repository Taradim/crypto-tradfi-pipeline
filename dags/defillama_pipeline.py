"""Airflow DAG for DeFiLlama protocol data pipeline.

This DAG orchestrates the extraction, transformation, and loading of DeFi protocol
data from DeFiLlama API into S3 following the medallion architecture.
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
from src.pipelines.defillama import DeFiLlamaPipeline


def validate_config_task() -> None:
    """Validate configuration before pipeline execution.

    Raises:
        ValueError: If required configuration is missing.
    """
    Config.validate()


def run_defillama_pipeline_task(**context: dict) -> str:
    """Execute the DeFiLlama data pipeline.

    This task:
    1. Extracts protocol data from DeFiLlama API
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

    # Get endpoint from Airflow variables or use default
    endpoint = "protocols"
    if context.get("dag_run") and context.get("dag_run").conf:
        endpoint = context.get("dag_run").conf.get("endpoint", "protocols")

    # Create and execute pipeline
    pipeline = DeFiLlamaPipeline(endpoint=endpoint)
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
    "defillama_pipeline",
    default_args=default_args,
    description="Orchestrates DeFiLlama protocol data ingestion",
    schedule="0 */12 * * *",  # Every 12 hours (UTC) - DeFiLlama updates less frequently
    catchup=False,
    tags=["defi", "defillama", "data-ingestion", "protocols"],
)

# Task 1: Validate configuration
validate_config = PythonOperator(
    task_id="validate_config",
    python_callable=validate_config_task,
    dag=dag,
)

# Task 2: Run DeFiLlama pipeline
run_pipeline = PythonOperator(
    task_id="run_defillama_pipeline",
    python_callable=run_defillama_pipeline_task,
    dag=dag,
)

# Define task dependencies
validate_config >> run_pipeline
