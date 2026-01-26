"""Discord webhook notifier for Airflow alerts.

Sends formatted messages to Discord when Airflow tasks fail.
"""

import os
import sys
from pathlib import Path
from typing import Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from src.monitoring import get_logger, setup_logging  # noqa: E402

# Setup logging
setup_logging()
logger = get_logger("alerts.discord_notifier")

# Load environment variables
load_dotenv(".env.airflow")
load_dotenv(".env")


def send_discord_alert(
    dag_id: str,
    task_id: str,
    run_id: str,
    execution_date: str,
    log_url: str | None = None,
    error_message: str | None = None,
) -> bool:
    """Send a Discord alert with embed format.

    Args:
        dag_id: DAG identifier
        task_id: Task identifier
        run_id: Airflow run ID
        execution_date: Execution date timestamp
        log_url: URL to Airflow logs (optional)
        error_message: Error message if available (optional)

    Returns:
        bool: True if message sent successfully, False otherwise
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        logger.warning("discord_webhook_not_configured", message="DISCORD_WEBHOOK_URL not set")
        return False

    # Build embed message
    embed = {
        "title": "Airflow Task Failed",
        "color": 15158332,  # Red color
        "fields": [
            {"name": "DAG", "value": dag_id, "inline": True},
            {"name": "Task", "value": task_id, "inline": True},
            {"name": "Run ID", "value": run_id, "inline": False},
            {"name": "Execution Date", "value": execution_date, "inline": False},
        ],
        "timestamp": execution_date,
    }

    # Add log URL if available
    if log_url:
        embed["fields"].append(
            {
                "name": "Logs",
                "value": f"[View Logs]({log_url})",
                "inline": False,
            }
        )

    # Add error message if available
    if error_message:
        # Truncate long error messages
        error_preview = error_message[:500] + "..." if len(error_message) > 500 else error_message
        embed["fields"].append(
            {
                "name": "Error",
                "value": f"```{error_preview}```",
                "inline": False,
            }
        )

    payload = {"embeds": [embed]}

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(
            "discord_alert_sent",
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
        )
        return True
    except requests.exceptions.RequestException as e:
        logger.error(
            "discord_alert_failed",
            dag_id=dag_id,
            task_id=task_id,
            error=str(e),
        )
        return False
