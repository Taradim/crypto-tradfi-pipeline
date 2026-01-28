"""DBT runner utility for Airflow tasks.

Provides functions to execute dbt commands within Airflow tasks,
handling environment variables and working directory setup.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.models import DagRun

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"

# Add project root to path for imports
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import Config  # noqa: E402


class DbtRunnerError(Exception):
    """Exception raised when dbt command fails."""

    def __init__(self, command: str, return_code: int, output: str) -> None:
        self.command = command
        self.return_code = return_code
        self.output = output
        super().__init__(f"dbt command failed: {command} (exit code {return_code})")


def setup_dbt_environment() -> dict[str, str]:
    """Setup environment variables required for dbt execution.

    Returns:
        dict: Environment variables with AWS and S3 configuration.

    Raises:
        ValueError: If required AWS credentials are missing.
    """
    # Validate config (will raise if S3_BUCKET_NAME is not set)
    Config.validate()

    # Build environment with required variables
    env = os.environ.copy()
    env["S3_BUCKET_NAME"] = Config.S3_BUCKET_NAME
    env.setdefault("AWS_DEFAULT_REGION", Config.AWS_REGION)

    # Verify AWS credentials are present
    aws_access_key = env.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = env.get("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be defined. "
            "Set them in .env.airflow or .env"
        )

    return env


def run_dbt_command(
    command: str,
    select: str | None = None,
    exclude: str | None = None,
    full_refresh: bool = False,
    target: str = "dev",
    vars_dict: dict[str, Any] | None = None,
) -> str:
    """Execute a dbt command and return the output.

    Args:
        command: dbt command to run (run, test, build, compile, etc.)
        select: dbt selector expression (e.g., "silver.*" or "+gold_crypto_prices")
        exclude: dbt exclude expression
        full_refresh: If True, run with --full-refresh flag
        target: dbt target profile to use (default: dev)
        vars_dict: Dictionary of variables to pass to dbt via --vars

    Returns:
        str: Command output (stdout + stderr combined)

    Raises:
        DbtRunnerError: If dbt command exits with non-zero code.
        ValueError: If environment setup fails.
    """
    # Setup environment
    env = setup_dbt_environment()

    # Build command arguments
    # In Docker (Pi), dbt is installed globally via pip, so we call it directly
    # Locally with uv, we'd use "uv run dbt" but for container compatibility
    # we use the direct "dbt" command which works in both environments
    cmd = ["dbt", command]

    if select:
        cmd.extend(["--select", select])

    if exclude:
        cmd.extend(["--exclude", exclude])

    if full_refresh:
        cmd.append("--full-refresh")

    cmd.extend(["--target", target])

    if vars_dict:
        import json

        cmd.extend(["--vars", json.dumps(vars_dict)])

    # Execute command
    # Use dbt directly (installed via pip in container) instead of "uv run dbt"
    # This ensures compatibility with both local dev (uv) and Docker (pip)
    result = subprocess.run(
        cmd,
        cwd=DBT_PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )

    output = result.stdout + result.stderr

    if result.returncode != 0:
        raise DbtRunnerError(
            command=" ".join(cmd),
            return_code=result.returncode,
            output=output,
        )

    return output


def run_dbt_silver(**context: Any) -> str:
    """Run dbt transformations for Silver layer models.

    Transforms Bronze (raw) data into cleaned and standardized Silver layer.

    Args:
        **context: Airflow task context.

    Returns:
        str: dbt command output.
    """
    # Check if full_refresh is requested via dag_run conf
    full_refresh = False
    dag_run: DagRun | None = context.get("dag_run")
    if dag_run and dag_run.conf:
        full_refresh = dag_run.conf.get("full_refresh", False)

    output = run_dbt_command(
        command="run",
        select="silver.*",
        full_refresh=full_refresh,
    )

    return output


def run_dbt_gold(**context: Any) -> str:
    """Run dbt transformations for Gold layer models.

    Transforms Silver data into business-ready Gold layer analytics.

    Args:
        **context: Airflow task context.

    Returns:
        str: dbt command output.
    """
    # Check if full_refresh is requested via dag_run conf
    full_refresh = False
    dag_run: DagRun | None = context.get("dag_run")
    if dag_run and dag_run.conf:
        full_refresh = dag_run.conf.get("full_refresh", False)

    output = run_dbt_command(
        command="run",
        select="gold.*",
        full_refresh=full_refresh,
    )

    return output


def run_dbt_tests(**context: Any) -> str:
    """Run dbt tests for data quality validation.

    Executes all dbt tests defined in schema.yml.

    Args:
        **context: Airflow task context.

    Returns:
        str: dbt test output.
    """
    # Allow selecting specific tests via dag_run conf
    select = None
    dag_run: DagRun | None = context.get("dag_run")
    if dag_run and dag_run.conf:
        select = dag_run.conf.get("test_select")

    output = run_dbt_command(
        command="test",
        select=select,
    )

    return output


def run_dbt_build(**context: Any) -> str:
    """Run dbt build (run + test) for all models.

    Executes both transformations and tests in dependency order.

    Args:
        **context: Airflow task context.

    Returns:
        str: dbt build output.
    """
    full_refresh = False
    select = None

    dag_run: DagRun | None = context.get("dag_run")
    if dag_run and dag_run.conf:
        full_refresh = dag_run.conf.get("full_refresh", False)
        select = dag_run.conf.get("select")

    output = run_dbt_command(
        command="build",
        select=select,
        full_refresh=full_refresh,
    )

    return output
