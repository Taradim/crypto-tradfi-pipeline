# dbt Project - Data Pipeline Portfolio

This dbt project standardizes transformations for the data pipeline portfolio using DuckDB.

## Structure

```
dbt/
├── models/
│   ├── silver/           # Silver layer models (read from S3 bronze)
│   │   └── silver_coingecko.sql
│   └── gold/             # Gold layer models (business-ready analytics)
│       └── gold_crypto_prices.sql
├── macros/               # Reusable SQL macros
│   └── configure_s3.sql
├── tests/                # Custom tests
├── dbt_project.yml       # dbt project configuration
├── schema.yml            # Model documentation and tests
└── profiles.example.yml  # Example profiles configuration (copy to profiles.yml)
```

## Setup

### 1. Install dependencies

```bash
# dbt-duckdb is already in pyproject.toml
uv sync
```

### 2. Configure profiles.yml

Copy the example and update with your credentials:

```bash
cp dbt/profiles.example.yml dbt/profiles.yml
# Edit dbt/profiles.yml with your settings
```

The `profiles.yml` file is already in `.gitignore` and should not be committed.

### 3. Set environment variables

dbt does **not** load `.env` or `.env.airflow` itself. It only reads `env_var()` from the **process environment** (the shell that started dbt).

**Option A – Use the wrapper script (recommended):**

```bash
# From project root: loads .env.airflow and .env via src/config.py then runs dbt
uv run python scripts/run_dbt_with_env.py run

# Other dbt commands:
uv run python scripts/run_dbt_with_env.py test
uv run python scripts/run_dbt_with_env.py debug
```

**Option B – Export variables manually in the shell:**

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=eu-west-1
export S3_BUCKET_NAME=your-bucket-name
cd dbt && uv run dbt run
```

### 4. Test connection

```bash
cd dbt
dbt debug
```

## Usage

### Run models

```bash
# Run all models
dbt run

# Run specific model
dbt run --select silver_coingecko
dbt run --select gold_crypto_prices

# Run silver models only
dbt run --select silver.*

# Run gold models only
dbt run --select gold.*
```

### Run tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select silver_coingecko
```

### Generate documentation

```bash
# Generate and serve documentation
dbt docs generate
dbt docs serve
```

## Models

### Silver Layer Models (`silver/`)

- **silver_coingecko**: Reads raw data from S3 bronze layer and standardizes
  - Renames columns to snake_case
  - Standardizes data types
  - Adds ingestion metadata
  - Applies basic data quality checks

### Gold Layer Models (`gold/`)

- **gold_crypto_prices**: Enriched cryptocurrency prices with derived metrics
  - Calculates volume-to-market-cap ratio
  - Categorizes trends (uptrend, downtrend, etc.)
  - Categorizes market cap (large, mid, small, micro)
  - Calculates supply circulation percentage

## Configuration

### S3 Access

S3 access is configured automatically via the `configure_s3()` macro in `on-run-start` hook. It uses:
- `AWS_ACCESS_KEY_ID` environment variable
- `AWS_SECRET_ACCESS_KEY` environment variable
- `AWS_DEFAULT_REGION` environment variable (defaults to `eu-west-1`)

### Variables

- `s3_bucket_name`: S3 bucket name (from `S3_BUCKET_NAME` env var)

## Testing

Tests are defined in `schema.yml`:
- `not_null`: Ensures required columns are not null
- `unique`: Ensures primary keys are unique
- `accepted_range`: Validates numeric ranges
- `accepted_values`: Validates categorical values

## Materialization

- **Silver models**: Materialized as `view` (lightweight, always fresh)
- **Gold models**: Materialized as `table` (persisted for performance)

## Next Steps

1. Add more silver models (yahoo_finance, defillama, etc.)
2. Create additional gold models for different analytics use cases
3. Integrate with Airflow for scheduled runs (see Étape 4)
