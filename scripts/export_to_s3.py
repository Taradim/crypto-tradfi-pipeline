from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def main() -> None:
    load_dotenv()

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    # TODO: decide which rows to export (all history vs last N minutes)
    df = pd.read_sql("SELECT * FROM crypto_market", con=engine)

    export_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    os.makedirs("data/exports", exist_ok=True)
    out_path = f"data/exports/crypto_market_{export_ts}.parquet"

    df.to_parquet(out_path, index=False)
    print(f"Wrote parquet: {out_path} rows={len(df)}")


if __name__ == "__main__":
    main()