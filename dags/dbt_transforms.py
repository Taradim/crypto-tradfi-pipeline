"""Airflow DAG for dbt transformations (Bronze -> Silver -> Gold).

This DAG orchestrates dbt transformations to convert raw Bronze layer data
into cleaned Silver and business-ready Gold layers.

Glue Catalog registration is handled automatically by dbt-duckdb via the
glue_register=true config option in each model.

The DAG can be triggered:
- On schedule (after typical ingestion times)
- Manually via Airflow UI
- Via TriggerDagRunOperator from ingestion DAGs

Configuration options (via dag_run.conf):
- full_refresh: bool - Run dbt with --full-refresh flag
- select: str - dbt selector expression for specific models
- test_select: str - dbt selector for specific tests
- skip_tests: bool - Skip dbt test step (not recommended)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Add project root to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.utils.alerting import task_failure_callback
from dags.utils.dbt_runner import (
    run_dbt_gold,
    run_dbt_silver,
    run_dbt_tests,
    setup_dbt_environment,
)
from src.config import Config

if TYPE_CHECKING:
    from airflow.models import DagRun


def validate_config_task() -> None:
    """Validate configuration before dbt execution.

    Ensures S3 bucket and AWS credentials are properly configured.

    Raises:
        ValueError: If required configuration is missing.
    """
    Config.validate()
    # Also validate dbt environment setup
    setup_dbt_environment()


def run_silver_task(**context: Any) -> str:
    """Execute dbt transformations for Silver layer.

    Transforms Bronze (raw) data into cleaned Silver layer.
    Also registers tables in AWS Glue Catalog via glue_register=true.

    Args:
        **context: Airflow task context.

    Returns:
        str: dbt command output.
    """
    return run_dbt_silver(**context)


def run_gold_task(**context: Any) -> str:
    """Execute dbt transformations for Gold layer.

    Transforms Silver data into business-ready Gold layer.
    Also registers tables in AWS Glue Catalog via glue_register=true.

    Args:
        **context: Airflow task context.

    Returns:
        str: dbt command output.
    """
    return run_dbt_gold(**context)


def run_tests_task(**context: Any) -> str:
    """Execute dbt tests for data quality validation.

    Args:
        **context: Airflow task context.

    Returns:
        str: dbt test output.
    """
    # Check if tests should be skipped
    dag_run: DagRun | None = context.get("dag_run")
    if dag_run and dag_run.conf:
        if dag_run.conf.get("skip_tests", False):
            return "Tests skipped via dag_run.conf.skip_tests=True"

    return run_dbt_tests(**context)


# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2024, 1, 1),
    "on_failure_callback": task_failure_callback,
}

# Create the DAG
# Schedule: 30 minutes after ingestion DAGs (which run at 0, 6, 12, 18 UTC)
# This gives time for ingestion to complete before transformations start
dag = DAG(
    "dbt_transforms",
    default_args=default_args,
    description="Orchestrates dbt transformations: Bronze -> Silver -> Gold (with auto Glue sync)",
    schedule="30 0,6,12,18 * * *",  # 00:30, 06:30, 12:30, 18:30 UTC
    catchup=False,
    tags=["dbt", "transformations", "silver", "gold", "data-quality", "glue"],
    doc_md=__doc__,
)

# Task 1: Validate configuration
validate_config = PythonOperator(
    task_id="validate_config",
    python_callable=validate_config_task,
    dag=dag,
)

# Task 2: Run Silver layer transformations (writes to S3 + registers in Glue)
run_silver = PythonOperator(
    task_id="dbt_run_silver",
    python_callable=run_silver_task,
    dag=dag,
)

# Task 3: Run Gold layer transformations (writes to S3 + registers in Glue)
run_gold = PythonOperator(
    task_id="dbt_run_gold",
    python_callable=run_gold_task,
    dag=dag,
)

# Task 4: Run dbt tests for data quality
run_tests = PythonOperator(
    task_id="dbt_test",
    python_callable=run_tests_task,
    dag=dag,
)

# Define task dependencies
# Silver depends on Bronze (handled by ingestion DAGs)
# Gold depends on Silver
# Tests run after all transformations
# Note: Glue registration is automatic via dbt-duckdb glue_register=true
validate_config >> run_silver >> run_gold >> run_tests
