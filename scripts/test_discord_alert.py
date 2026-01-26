"""Test Discord alert notification manually.

This script sends a test alert to Discord to verify the webhook configuration.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.alerts.discord_notifier import send_discord_alert  # noqa: E402
from src.monitoring import get_logger, setup_logging  # noqa: E402

# Setup logging
setup_logging()
logger = get_logger("scripts.test_discord_alert")


def main() -> None:
    """Send a test Discord alert."""
    logger.info("starting_discord_alert_test")

    # Test data
    dag_id = "test_dag"
    task_id = "test_task"
    run_id = "test_run_123"
    execution_date = datetime.now(timezone.utc).isoformat()
    log_url = "http://localhost:8080/log?dag_id=test_dag&task_id=test_task&execution_date=2026-01-26T10:00:00Z"
    error_message = "This is a test error message to verify Discord alerting is working correctly."

    logger.info(
        "sending_test_alert",
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
    )

    # Send the alert
    success = send_discord_alert(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        execution_date=execution_date,
        log_url=log_url,
        error_message=error_message,
    )

    if success:
        logger.info("test_alert_sent_successfully", message="Check your Discord channel")
    else:
        logger.error("test_alert_failed", message="Check logs for details")


if __name__ == "__main__":
    main()
