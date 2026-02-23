# Crypto & TradFi Data Pipeline

Data pipeline that ingests crypto markets (CoinGecko, DeFiLlama, Yahoo Finance), stores data on S3, and transforms it with dbt. Orchestrated by Apache Airflow.

---

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Raspberry Pi Deployment](#raspberry-pi-deployment)
- [Useful Commands](#useful-commands)
- [Troubleshooting](#troubleshooting)
- [Trade-offs](#trade-offs)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  Webserver   │  │  Scheduler   │  │  PostgreSQL      │   │
│  │  (port 8080) │  │  (DAGs)      │  │  (metadata)      │   │
│  └──────┬───────┘  └──────┬───────┘  └──────────────────┘   │
│         │                 │                                 │
│         └────────┬────────┘                                 │
│                  ▼                                          │
│         DAGs (CoinGecko, dbt transforms...)                 │
└──────────────────┼──────────────────────────────────────────┘
                   │
    ┌──────────────┼──────────────┐
    ▼              ▼              ▼
┌─────────┐  ┌──────────┐  ┌────────────┐
│CoinGecko│  │ DeFiLlama│  │ AWS S3     │
│ API     │  │ API      │  │ (bronze/   │
│         │  │          │  │  silver/   │
│         │  │          │  │  gold)     │
└─────────┘  └──────────┘  └────────────┘
```

Data is organized in a **medallion architecture**: bronze (raw) -> silver (cleaned) -> gold (analytics).

---

## Project Structure

```
.
├── dags/               # Airflow DAGs
│   ├── crypto_market_pipeline.py   # CoinGecko pipeline
│   ├── dbt_transforms.py           # dbt transformations
│   └── utils/
├── dbt/                # dbt models (silver, gold)
│   ├── models/
│   ├── macros/
│   └── README.md       # Detailed dbt documentation
├── src/                # Python pipeline code
│   └── pipelines/
├── scripts/            # Utilities (dbt with env, S3 init, etc.)
├── terraform/          # AWS infrastructure (S3, IAM, Glue)
├── docker-compose.yml  # Airflow + PostgreSQL
├── env.airflow.example # Environment variables template
└── .env.airflow        # Secrets (create locally, not versioned)
```

---

## Quick Start

### Prerequisites

- **Docker** and **Docker Compose**
- **Python 3.10+** (for local development)
- **AWS** account with S3 access

### 1. Create the S3 bucket (Terraform)

```bash
cd terraform
terraform init
terraform apply
```

Creates the bucket, Glue databases, and optionally an IAM user for Airflow. Note the bucket name and IAM credentials if created.

### 2. Configure environment

```bash
cp env.airflow.example .env.airflow
# Edit .env.airflow with your values
```

**Key variables**:

| Variable | Description |
|----------|-------------|
| `S3_BUCKET_NAME` | S3 bucket name from Terraform |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | AWS credentials (or IAM role) |
| `AWS_DEFAULT_REGION` | Region (e.g. `eu-west-1`) |
| `COINGECKO_API_KEY` | CoinGecko API key (recommended) |
| `AIRFLOW_WWW_USER_PASSWORD` | Airflow password (change this) |

### 3. Set Airflow UID

```bash
# Linux / Raspberry Pi
export AIRFLOW_UID=$(id -u)

# macOS / Windows
export AIRFLOW_UID=50000

# Permanent (Linux)
echo "export AIRFLOW_UID=$(id -u)" >> ~/.bashrc
```

### 4. Initialize and start Airflow

```bash
# One-time initialization
docker compose run --rm airflow-init

# Start services
docker compose up -d

# Access UI: http://localhost:8080
# Credentials: airflow / (password from .env.airflow)
```

---

## Raspberry Pi Deployment

Suitable for running the pipeline continuously on a Raspberry Pi (4 GB RAM minimum recommended).

### Fresh installation

```bash
# 1. Connect to the Pi
ssh pi@<PI_IP>

# 2. Update and install Docker
sudo apt update && sudo apt upgrade -y
sudo apt install git -y
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker pi

# 3. Disconnect and reconnect to apply docker group
exit
ssh pi@<PI_IP>

# 4. Clone repo and configure
git clone <REPO_URL> ~/crypto-pipeline
cd ~/crypto-pipeline
cp env.airflow.example .env.airflow
nano .env.airflow   # Set S3, AWS, API keys, Airflow password

export AIRFLOW_UID=$(id -u)
echo "export AIRFLOW_UID=$(id -u)" >> ~/.bashrc

# 5. Start Airflow
docker compose up -d
```

Access: `http://<PI_IP>:8080`

### Updating code

```bash
cd ~/crypto-pipeline
git pull
docker compose up -d --build
```

---

## Pipelines and DAGs

| DAG | Description |
|-----|-------------|
| `crypto_market_pipeline` | CoinGecko ingestion -> S3 (bronze) |
| dbt transforms | Silver -> Gold via `dbt run` |

DAGs are reloaded automatically by the scheduler (every 30s). dbt models are documented in [`dbt/README.md`](dbt/README.md).

---

## Useful Commands

```bash
# Service logs
docker compose logs -f

# Container status
docker compose ps

# Stop
docker compose down

# Restart
docker compose restart

# Clean volumes (deletes Airflow database)
docker compose down -v
```

**dbt (local)**:

```bash
# With environment variables loaded
uv run python scripts/run_dbt_with_env.py run
uv run python scripts/run_dbt_with_env.py test
```

---

## Troubleshooting

### DAGs not appearing

- Check Python syntax: `python -m py_compile dags/crypto_market_pipeline.py`
- Check logs: `docker compose logs airflow-scheduler`

### Docker Compose version conflict

Use `docker compose` (with space), not `docker-compose`:

```bash
docker compose up -d    # Modern plugin
docker-compose up -d    # Legacy standalone
```

### Docker permission issues

```bash
groups   # Check if "docker" is present
sudo usermod -aG docker $USER
# Disconnect and reconnect
```

### Airflow not accessible (Raspberry Pi)

- Ensure port 8080 is not blocked
- Get Pi IP: `hostname -I`
- Check containers: `docker compose ps`

### Low memory (Raspberry Pi < 4 GB)

- Enable swap or use a Pi with 4 GB+
- Reduce concurrent tasks in Airflow

---

## Production Security

- Change default password in `AIRFLOW_WWW_USER_PASSWORD`
- Prefer IAM roles over keys in `.env` when possible
- Never commit `.env.airflow` (already in `.gitignore`)

---

## Trade-offs

| Choice | Rationale |
|--------|-----------|
| **LocalExecutor** | Single-node deployment; sufficient for homelab; avoids Celery complexity. |
| **Docker Compose** | Simple, reproducible; no Kubernetes overhead; suitable for Pi and local dev. |
| **Terraform for S3** | IaC for consistency; manual setup avoided; Glue catalogs created upfront. |
| **dbt + DuckDB** | SQL-based transforms; DuckDB for local/embedded analytics without Spark cost. |

---

## License

See the project `LICENSE` file.
