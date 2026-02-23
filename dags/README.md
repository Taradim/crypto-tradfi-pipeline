# Airflow DAGs

Apache Airflow DAGs that orchestrate data ingestion and transformation pipelines.

## DAGs

| DAG | Description |
|-----|-------------|
| `crypto_market_pipeline` | CoinGecko market data ingestion into S3 (bronze layer) |
| `defillama_pipeline` | DeFiLlama protocol TVL and metadata ingestion |
| `yahoo_finance_pipeline` | Yahoo Finance OHLCV data ingestion |
| `dbt_transforms` | dbt transformations: bronze -> silver -> gold |

## Structure

```
dags/
├── crypto_market_pipeline.py   # CoinGecko DAG
├── defillama_pipeline.py      # DeFiLlama DAG
├── yahoo_finance_pipeline.py  # Yahoo Finance DAG
├── dbt_transforms.py          # dbt run/test orchestration
└── utils/
    ├── alerting.py            # Discord webhook on task failure
    └── dbt_runner.py          # dbt execution with env setup for Airflow
```

## DAG Discovery

DAGs are loaded from this directory by the Airflow scheduler (every 30s by default). Ensure:
- Valid Python syntax: `python -m py_compile dags/crypto_market_pipeline.py`
- Required imports resolve (Config, pipelines, etc.)

## Utils

### alerting.py

Sends Discord notifications when tasks fail. Requires `DISCORD_WEBHOOK_URL` in `.env.airflow`.

### dbt_runner.py

Runs dbt commands from Airflow tasks with the correct environment (AWS credentials, S3_BUCKET_NAME, DBT_PROFILES_DIR). Used by `dbt_transforms` DAG.

## Triggering dbt_transforms

Via `dag_run.conf`:
- `full_refresh`: Run dbt with `--full-refresh`
- `select`: dbt selector (e.g. `silver.*`, `crypto_prices`)
- `test_select`: Run tests for specific models
- `skip_tests`: Skip dbt test step (not recommended)
