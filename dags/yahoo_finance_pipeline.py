"""Airflow DAG for Yahoo Finance stock market data pipeline.

This DAG orchestrates the extraction, transformation, and loading of stock market
data from Yahoo Finance into S3 following the medallion architecture.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.config import Config
from src.monitoring import setup_logging
from src.pipelines.yahoo_finance import YahooFinancePipeline
from dags.utils.alerting import task_failure_callback


def validate_config_task() -> None:
    """Validate configuration before pipeline execution.

    Raises:
        ValueError: If required configuration is missing.
    """
    Config.validate()


def run_yahoo_finance_pipeline_task(**context: dict) -> str:
    """Execute the Yahoo Finance data pipeline.

    This task:
    1. Extracts stock market data from Yahoo Finance
    2. Transforms the data into a structured format
    3. Loads the data to S3 in bronze layer

    Args:
        **context: Airflow task context (contains dag_run, execution_date, etc.)

    Returns:
        str: S3 path where the data was uploaded

    Raises:
        Exception: If pipeline execution fails.
    """
    # Setup logging
    setup_logging()

    # Get parameters from Airflow variables or use defaults
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf:
        symbols = dag_run.conf.get("symbols")
        period = dag_run.conf.get("period", "1mo")
        interval = dag_run.conf.get("interval", "1d")
    else:
        # Top 100+ US companies by market cap + major world indices
        symbols = [
            # Top Tech Companies (20)
            "AAPL",
            "MSFT",
            "GOOGL",
            "GOOG",
            "AMZN",
            "NVDA",
            "META",
            "TSLA",
            "NFLX",
            "AMD",
            "INTC",
            "AVGO",
            "CRM",
            "ORCL",
            "ADBE",
            "CSCO",
            "QCOM",
            "TXN",
            "AMAT",
            "MU",
            # Financial Services (20)
            "JPM",
            "BAC",
            "WFC",
            "GS",
            "MS",
            "C",
            "BLK",
            "SCHW",
            "AXP",
            "COF",
            "USB",
            "PNC",
            "TFC",
            "BK",
            "STT",
            "MTB",
            "CFG",
            "HBAN",
            "KEY",
            "ZION",
            # Healthcare & Pharma (20)
            "UNH",
            "JNJ",
            "PFE",
            "ABBV",
            "TMO",
            "ABT",
            "DHR",
            "BMY",
            "AMGN",
            "GILD",
            "CVS",
            "CI",
            "HUM",
            "ELV",
            "CNC",
            "MRNA",
            "REGN",
            "VRTX",
            "BIIB",
            "ILMN",
            # Consumer & Retail (15)
            "WMT",
            "HD",
            "MCD",
            "NKE",
            "SBUX",
            "TGT",
            "LOW",
            "TJX",
            "DG",
            "COST",
            "EBAY",
            "ETSY",
            "SHOP",
            "BBY",
            "LULU",
            # Automotive
            "F",
            "GM",
            "FORD",
            "RIVN",
            # Energy & Utilities (15)
            "XOM",
            "CVX",
            "COP",
            "SLB",
            "EOG",
            "MPC",
            "VLO",
            "PSX",
            "OXY",
            "HAL",
            "NEE",
            "DUK",
            "SO",
            "D",
            "AEP",
            # Industrial & Aerospace (15)
            "BA",
            "GE",
            "CAT",
            "DE",
            "HON",
            "RTX",
            "LMT",
            "NOC",
            "GD",
            "TDG",
            "EMR",
            "ETN",
            "ITW",
            "PH",
            "ROK",
            # Communication Services (5)
            "VZ",
            "T",
            "CMCSA",
            "DIS",
            "SNAP",
            # Materials & Chemicals (5)
            "LIN",
            "APD",
            "ECL",
            "SHW",
            "PPG",
            # Real Estate (5)
            "AMT",
            "PLD",
            "EQIX",
            "PSA",
            "WELL",
            # Major World Indices (15)
            "^GSPC",  # S&P 500
            "^DJI",  # Dow Jones Industrial Average
            "^IXIC",  # NASDAQ Composite
            "^VIX",  # VIX Volatility Index
            "SPY",  # S&P 500 ETF
            "QQQ",  # NASDAQ 100 ETF
            "DIA",  # Dow Jones ETF
            "IWM",  # Russell 2000 ETF
            "VTI",  # Total Stock Market ETF
            "EFA",  # EAFE (Europe, Asia, Far East)
            "EEM",  # Emerging Markets ETF
            "^FTSE",  # FTSE 100 (UK)
            "^GDAXI",  # DAX (Germany)
            "^FCHI",  # CAC 40 (France)
            "^N225",  # Nikkei 225 (Japan)
            "^HSI",  # Hang Seng (Hong Kong)
            "^SSEC",  # Shanghai Composite (China)
            "^BSESN",  # BSE Sensex (India)
        ]
        period = "2d"  # 2 derniers jours seulement
        interval = "1d"

    # Create and execute pipeline
    pipeline = YahooFinancePipeline(symbols=symbols, period=period, interval=interval)
    s3_path = pipeline.run(layer="bronze")

    return s3_path


# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "on_failure_callback": task_failure_callback,
}

# Create the DAG
dag = DAG(
    "yahoo_finance_pipeline",
    default_args=default_args,
    description="Orchestrates Yahoo Finance stock market data ingestion",
    schedule="0 */6 * * *",  # Every 6 hours (UTC) - récupère les 2 derniers jours
    catchup=False,
    tags=["stocks", "yahoo_finance", "data-ingestion", "market-data"],
)

# Task 1: Validate configuration
validate_config = PythonOperator(
    task_id="validate_config",
    python_callable=validate_config_task,
    dag=dag,
)

# Task 2: Run Yahoo Finance pipeline
run_pipeline = PythonOperator(
    task_id="run_yahoo_finance_pipeline",
    python_callable=run_yahoo_finance_pipeline_task,
    dag=dag,
)

# Define task dependencies
validate_config >> run_pipeline
