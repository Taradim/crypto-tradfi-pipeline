from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
import uuid

import json
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine

LOG_FILE = os.environ.get("INGEST_LOG_FILE", "logs/ingest_postgres.jsonl")

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=100&page=1"
)

def log_event(event: str, **fields: object) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    line = json.dumps(payload, ensure_ascii=False, default=str)
    print(line)

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

@dataclass
class Settings:
    pg_user: str
    pg_password: str
    pg_db: str
    pg_host: str
    pg_port: int 


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        pg_user=os.environ["POSTGRES_USER"],
        pg_password=os.environ["POSTGRES_PASSWORD"],
        pg_db=os.environ["POSTGRES_DB"],
        pg_host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        pg_port=int(os.environ.get("POSTGRES_PORT", "5432")),
    )


def fetch_coins_markets() -> list[dict[str, Any]]:
    response = requests.get(COINGECKO_URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise TypeError(f"Expected list, got {type(data)}")
    return data


def to_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    cols = [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume",
        "last_updated",
    ]
    df = pd.DataFrame(rows)

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing expected columns from API response: {missing}")

    df = df[cols].copy()

    # Parse ISO-8601 timestamps -> timezone-aware UTC
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True, errors="raise")

    # Ensure numeric columns are numeric (NaN allowed)
    for c in ["current_price", "market_cap", "total_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Normalize strings
    df["id"] = df["id"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["name"] = df["name"].astype(str)

    return df


def main() -> None:
    t0 = time.time()
    run_id = str(uuid.uuid4())
    log_event("job_start", run_id=run_id)


    settings = load_settings()
    coins_markets = fetch_coins_markets()
    df = to_dataframe(coins_markets)
    log_event("api_fetch_complete", rows=len(df), run_id=run_id)

    engine = create_engine(
        f"postgresql+psycopg2://{settings.pg_user}:{settings.pg_password}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db}"
    )

    max_ts = pd.read_sql("SELECT MAX(last_updated) AS max_last_updated FROM crypto_market", con=engine)
    cutoff = max_ts.loc[0, "max_last_updated"]
    log_event("db_cutoff_max_last_updated", cutoff=cutoff, run_id=run_id)

    cutoff = pd.to_datetime(cutoff, utc=True) if cutoff is not None else None

    if cutoff is not None:
        df = df[df["last_updated"] > cutoff].copy()

    log_event("rows_to_insert", rows=len(df), run_id=run_id)

    if df.empty:
        log_event("job_end", status="no_op", duration_s=round(time.time() - t0, 3), run_id=run_id)
        return

    df.to_sql(
        "crypto_market", con=engine, if_exists="append", index=False, method="multi"
    )
    log_event("db_insert_complete", rows=len(df), run_id=run_id)

    log_event("job_end", status="ok", duration_s=round(time.time() - t0, 3), run_id=run_id)

if __name__ == "__main__":
    main()
