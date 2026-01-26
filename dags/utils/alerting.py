"""Airflow alerting utilities with Discord notifications.

Provides callbacks for task failure notifications.
"""

import os
import sys
from pathlib import Path
from typing import Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from airflow import DAG  # noqa: E402
from airflow.models import TaskInstance  # noqa: E402

from scripts.alerts.discord_notifier import send_discord_alert  # noqa: E402


def task_failure_callback(context: dict[str, Any]) -> None:
    """Callback function called when a task fails.

    Sends a Discord notification with task failure details.

    Args:
        context: Airflow task context containing dag, task, etc.
    """
    dag: DAG = context["dag"]
    task_instance: TaskInstance = context["task_instance"]

    dag_id = dag.dag_id
    task_id = task_instance.task_id
    run_id = context["dag_run"].run_id
    execution_date = (
        context["execution_date"].isoformat() if context.get("execution_date") else "N/A"
    )

    # Build log URL (adjust base URL based on your Airflow setup)
    # For local: http://localhost:8080
    # For Raspberry Pi: http://<pi-ip>:8080
    airflow_base_url = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    log_url = None

    if context.get("execution_date"):
        log_url = (
            f"{airflow_base_url}/log?"
            f"dag_id={dag_id}&"
            f"task_id={task_id}&"
            f"execution_date={context['execution_date']}&"
            f"try_number={task_instance.try_number}"
        )

    # Get error message from exception if available
    error_message = None
    if context.get("exception"):
        error_message = str(context["exception"])

    # Send Discord alert
    send_discord_alert(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        execution_date=execution_date,
        log_url=log_url,
        error_message=error_message,
    )
