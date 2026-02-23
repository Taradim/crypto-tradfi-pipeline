# Scripts

Standalone utilities for the data pipeline. Run from the project root with `uv run python scripts/<script>.py` or equivalent.

## Overview

| Script | Purpose |
|--------|---------|
| `run_dbt_with_env.py` | Run dbt with env loaded from `.env.airflow` / `.env` |
| `init_s3_structure.py` | Create bronze/silver/gold folder structure in S3 |
| `query_parquet_s3.py` | Query Parquet files in S3 using DuckDB |
| `read_latest_parquet.py` | Display the latest Parquet file from S3 bronze |
| `test_discord_alert.py` | Test Discord webhook for alerting |
| `test_duckdb_glue.py` | Test DuckDB + Glue Catalog integration |

## Usage

### run_dbt_with_env.py

Loads environment via `src.config.Config` and runs dbt. Use this instead of calling `dbt` directly when env vars are in `.env.airflow`.

```bash
uv run python scripts/run_dbt_with_env.py run
uv run python scripts/run_dbt_with_env.py test
uv run python scripts/run_dbt_with_env.py debug
```

### init_s3_structure.py

Creates empty marker files in S3 to establish the medallion layer structure. Run once after creating the bucket.

```bash
uv run python scripts/init_s3_structure.py
```

### query_parquet_s3.py

Query Parquet data in S3 with DuckDB. Supports `--source`, `--layer`, and `--query`.

```bash
uv run python scripts/query_parquet_s3.py --source coingecko --query "SELECT COUNT(*) FROM coingecko_bronze"
```

### read_latest_parquet.py

Finds and displays the most recent Parquet file in the bronze layer.

```bash
uv run python scripts/read_latest_parquet.py
```

### test_discord_alert.py

Sends a test message to the configured Discord webhook. Requires `DISCORD_WEBHOOK_URL`.

```bash
uv run python scripts/test_discord_alert.py
```

## Subdirectories

| Path | Purpose |
|------|---------|
| `alerts/` | Discord notifier and alert utilities |
