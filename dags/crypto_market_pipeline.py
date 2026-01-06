# dags/crypto_market_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['votre-email@example.com'],  # À configurer
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_market_pipeline',
    default_args=default_args,
    description='Pipeline horaire: CoinGecko → PostgreSQL → MinIO',
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,  # Important: ne pas rejouer les runs passés
    tags=['crypto', 'etl'],
)

# Tâche 1: Ingestion
ingest_task = BashOperator(
    task_id='ingest_crypto_market',
    bash_command='cd /opt/airflow && python scripts/ingest_postgres.py',
    dag=dag,
)

# Tâche 2: Export (dépend de ingest)
export_task = BashOperator(
    task_id='export_to_minio',
    bash_command='cd /opt/airflow && python scripts/export_to_s3.py',
    dag=dag,
)

# Tâche 3: Agrégation (dépend de export)
aggregate_task = BashOperator(
    task_id='aggregate_parquet',
    bash_command='cd /opt/airflow && python scripts/aggregate_parquet.py',
    dag=dag,
)

# Définir les dépendances
ingest_task >> export_task >> aggregate_task