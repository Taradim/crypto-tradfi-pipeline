"""Run dbt with environment variables loaded from .env.airflow and .env.

This script uses the centralized src/config.py module to load environment
variables consistently across all scripts. The Config import triggers
load_dotenv() for .env.airflow and .env, then we export the required
variables so the dbt subprocess sees them.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# Project root (parent of scripts/)
project_root = Path(__file__).resolve().parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from src.config import Config  # noqa: E402


def main() -> int:
    """Load env via Config and run dbt with those variables exported."""
    # Validate config (will raise if S3_BUCKET_NAME is not set)
    Config.validate()

    # Export required variables so dbt subprocess sees them
    # (src/config.py already called load_dotenv, so os.getenv works)
    os.environ["S3_BUCKET_NAME"] = Config.S3_BUCKET_NAME
    os.environ.setdefault("AWS_DEFAULT_REGION", Config.AWS_REGION)

    # AWS credentials must come from environment (loaded by dotenv in config.py)
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_access_key or not aws_secret_key:
        print("ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be defined")
        print("Set them in .env.airflow or .env")
        return 1

    dbt_args = sys.argv[1:] if len(sys.argv) > 1 else ["run"]
    dbt_dir = project_root / "dbt"
    return subprocess.run(
        ["uv", "run", "dbt"] + dbt_args,
        cwd=dbt_dir,
        env=os.environ,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
