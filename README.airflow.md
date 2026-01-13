# Airflow Deployment on Raspberry Pi

This guide explains how to deploy Apache Airflow on a Raspberry Pi to orchestrate the cryptocurrency data pipeline.

## Prerequisites

- Raspberry Pi (4GB RAM minimum recommended)
- Docker and Docker Compose installed
- Python 3.11+ (for local development)
- AWS credentials configured (or IAM role)

## Quick Start

### 1. Set up environment variables

```bash
# Copy the example environment file
cp env.airflow.example .env.airflow

# Edit .env.airflow with your configuration
nano .env.airflow
```

Required variables:
- `S3_BUCKET_NAME`: Your S3 bucket name
- `AWS_ACCESS_KEY_ID`: AWS access key (or use IAM role)
- `AWS_SECRET_ACCESS_KEY`: AWS secret key (or use IAM role)
- `COINGECKO_API_KEY`: CoinGecko API key (optional but recommended)

### 2. Set Airflow UID

```bash
# On Raspberry Pi (Linux)
export AIRFLOW_UID=$(id -u)

# Or set a fixed value
export AIRFLOW_UID=50000
```

### 3. Initialize Airflow

```bash
# Start services and initialize database
docker-compose up airflow-init
```

### 4. Start Airflow

```bash
# Start all services in background
docker-compose up -d

# View logs
docker-compose logs -f
```

### 5. Access Airflow UI

Open your browser and navigate to:
```
http://raspberry-pi-ip:8080
```

Default credentials:
- Username: `airflow`
- Password: `airflow`

**Important**: Change the default password in production!

## Services

The docker-compose setup includes:

- **postgres**: PostgreSQL database for Airflow metadata
- **airflow-webserver**: Web UI (port 8080)
- **airflow-scheduler**: Scheduler that runs DAGs
- **airflow-init**: Initialization service (runs once)

## DAGs Location

DAGs are automatically loaded from the `dags/` directory:
- `dags/crypto_market_pipeline.py` - CoinGecko data pipeline

## Monitoring

### View logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Check service status

```bash
docker-compose ps
```

### Restart services

```bash
docker-compose restart
```

## Troubleshooting

### DAGs not appearing

1. Check DAGs are in `dags/` directory
2. Check logs: `docker-compose logs airflow-scheduler`
3. Verify Python syntax: `python -m py_compile dags/crypto_market_pipeline.py`

### Permission errors

```bash
# Fix permissions
sudo chown -R $AIRFLOW_UID:0 dags logs
```

### Out of memory

Raspberry Pi with less than 4GB RAM may struggle. Options:
- Use a lighter Airflow executor (SequentialExecutor instead of LocalExecutor)
- Reduce number of concurrent tasks
- Add swap space

### AWS credentials

If using IAM role on Raspberry Pi (recommended for security):
1. Remove `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from `.env.airflow`
2. Configure IAM role on Raspberry Pi
3. Airflow will use instance profile credentials

## Production Considerations

1. **Change default password**: Update `AIRFLOW_WWW_USER_PASSWORD` in `.env.airflow`
2. **Use secrets management**: Store API keys in Airflow Variables/Connections instead of env files
3. **Enable SSL**: Use reverse proxy (nginx) with SSL for web UI
4. **Backup database**: Regularly backup PostgreSQL volume
5. **Monitor resources**: Use `htop` or `docker stats` to monitor CPU/memory

## Stopping Airflow

```bash
# Stop services
docker-compose down

# Stop and remove volumes (⚠️ deletes database)
docker-compose down -v
```

## Updating DAGs

DAGs are automatically reloaded by the scheduler (every 30 seconds by default). Just update files in `dags/` directory and they will appear in the UI.

## Architecture

```
┌─────────────────────────────────────────┐
│         Raspberry Pi                    │
│                                         │
│  ┌──────────────┐  ┌──────────────┐     │
│  │   Airflow    │  │  PostgreSQL  │     │
│  │  Webserver   │  │   Database   │     │
│  │  (Port 8080) │  │              │     │
│  └──────────────┘  └──────────────┘     │
│         │                  │            │
│         └────────┬─────────┘            │
│                  │                      │
│         ┌────────▼─────────┐            │
│         │  Airflow         │            │
│         │  Scheduler       │            │
│         │  (Runs DAGs)     │            │
│         └────────┬─────────┘            │
│                  │                      │
└──────────────────┼──────────────────────┘
                   │
         ┌─────────▼─────────┐
         │   CoinGecko API   │
         │   AWS S3          │
         └───────────────────┘
```

## Next Steps

1. Configure DAG schedule in `dags/crypto_market_pipeline.py`
2. Set up monitoring/alerting
3. Configure CI/CD for DAG deployment
4. Add more data sources/pipelines
