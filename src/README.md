# Source Code

Core Python logic for the data pipeline: configuration, data ingestion, and monitoring.

## Structure

```
src/
├── config.py              # Centralized config (env vars, bucket, API keys)
├── pipelines/             # ETL pipelines (extract -> transform -> load)
│   ├── base.py            # BasePipeline abstract class
│   ├── coingecko.py       # CoinGecko API -> S3
│   ├── defillama.py       # DeFiLlama API -> S3
│   ├── yahoo_finance.py   # Yahoo Finance -> S3
│   ├── fred.py            # FRED API (optional)
│   └── dune.py            # Dune Analytics (optional)
├── monitoring/            # Structured logging and context
│   ├── logging.py        # structlog setup
│   ├── context.py        # bind_context for pipeline tracking
│   └── README.md         # Monitoring documentation
└── utils/
    └── partitioning.py   # S3 path generation (layer, source, date)
```

## Configuration

`config.py` loads `.env.airflow` and `.env`. Key attributes:
- `AWS_REGION`, `S3_BUCKET_NAME`
- API keys: `COINGECKO_API_KEY`, `DEFILLAMA_API_KEY`, etc.
- `DATA_SOURCES`, `LAYERS` (bronze, silver, gold)

## Pipelines

All pipelines extend `BasePipeline` and implement:
- `extract()`: Fetch from source API
- `transform()`: Optional; default converts to DataFrame
- `load()`: Upload Parquet to S3 (inherited from base)

Pipelines follow the medallion architecture and use `generate_s3_path()` for consistent S3 keys.

## Monitoring

See [`monitoring/README.md`](monitoring/README.md) for structured logging, context binding, and log querying.
