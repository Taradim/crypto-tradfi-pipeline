from __future__ import annotations

import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def upload_to_minio(
    local_path: str,
    bucket: str,
    s3_key: str,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Upload local file to MinIO (S3-compatible)."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        print(f"Uploaded to MinIO: s3://{bucket}/{s3_key}")
    except ClientError as e:
        raise RuntimeError(f"Failed to upload to MinIO: {e}") from e


def get_last_exported_ts(
    engine, export_key: str = "crypto_market"
) -> pd.Timestamp | None:
    """Read last exported timestamp from metadata table."""

    query = """
        SELECT last_exported_ts
        FROM export_metadata
        WHERE export_key = %s
    """
    result = pd.read_sql(query, con=engine, params=(export_key,))
    if result.empty:
        return None
    return pd.to_datetime(result.loc[0, "last_exported_ts"], utc=True)


def update_export_metadata(
    engine, export_key: str, max_exported_ts: pd.Timestamp, rows_count: int
) -> None:
    """Update metadata table after successful export."""
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO export_metadata (export_key, last_exported_ts, rows_exported)
                VALUES (:export_key, :last_exported_ts, :rows_exported)
                ON CONFLICT (export_key) DO UPDATE
                SET last_exported_ts = EXCLUDED.last_exported_ts,
                    last_exported_at = NOW(),
                    rows_exported = EXCLUDED.rows_exported
            """),
            {
                "export_key": export_key,
                "last_exported_ts": max_exported_ts.isoformat(),
                "rows_exported": rows_count,
            },
        )
        conn.commit()


TARGET_BATCH_SIZE_MB = 100
TARGET_BATCH_SIZE_BYTES = TARGET_BATCH_SIZE_MB * 1024 * 1024


def get_or_create_batch_state(engine, batch_key: str) -> dict:
    """Get current batch state or create new one."""
    query = text("""
        SELECT current_batch_s3_key, current_batch_size_bytes, current_batch_rows
        FROM batch_state
        WHERE batch_key = :batch_key
    """)
    result = pd.read_sql(query, con=engine, params={"batch_key": batch_key})

    if result.empty:
        # Create new batch
        new_s3_key = f"crypto_market_pending/batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO batch_state (batch_key, current_batch_s3_key, current_batch_size_bytes, current_batch_rows)
                    VALUES (:batch_key, :s3_key, 0, 0)
                """),
                {"batch_key": batch_key, "s3_key": new_s3_key},
            )
            conn.commit()
        return {"s3_key": new_s3_key, "size_bytes": 0, "rows": 0}

    return {
        "s3_key": result.loc[0, "current_batch_s3_key"],
        "size_bytes": result.loc[0, "current_batch_size_bytes"],
        "rows": result.loc[0, "current_batch_rows"],
    }


def append_to_batch(
    s3_client,
    bucket: str,
    batch_s3_key: str,
    df_new: pd.DataFrame,
    engine,
    batch_key: str,
) -> tuple[bool, int, int]:
    """
    Append new rows to existing batch in MinIO.
    Returns: (should_flush, new_total_size_bytes, new_total_rows)
    """
    # Download existing batch
    local_temp = "data/temp/batch_current.parquet"
    os.makedirs("data/temp", exist_ok=True)

    try:
        s3_client.download_file(bucket, batch_s3_key, local_temp)
        df_existing = pd.read_parquet(local_temp)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # First append: no existing file
            df_combined = df_new
        else:
            raise

    # Save combined batch
    df_combined.to_parquet(local_temp, index=False)
    new_size = os.path.getsize(local_temp)

    # Upload back to MinIO
    s3_client.upload_file(local_temp, bucket, batch_s3_key)

    # Update state
    with engine.connect() as conn:
        conn.execute(
            text("""
                UPDATE batch_state
                SET current_batch_size_bytes = :size_bytes,
                    current_batch_rows = :rows,
                    last_updated_at = NOW()
                WHERE batch_key = :batch_key
            """),
            {
                "size_bytes": new_size,
                "rows": len(df_combined),
                "batch_key": batch_key,
            },
        )
        conn.commit()

    should_flush = new_size >= TARGET_BATCH_SIZE_BYTES
    return should_flush, new_size, len(df_combined)


def flush_batch_to_final(
    s3_client, bucket: str, batch_s3_key: str, engine, batch_key: str
) -> str:
    """Move batch from pending to final location, reset state."""
    # Download batch
    local_temp = "data/temp/batch_flush.parquet"
    s3_client.download_file(bucket, batch_s3_key, local_temp)

    # Generate final S3 key
    export_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    final_s3_key = f"crypto_market/{export_ts}.parquet"

    # Upload to final location
    s3_client.upload_file(local_temp, bucket, final_s3_key)

    # Delete pending batch
    s3_client.delete_object(Bucket=bucket, Key=batch_s3_key)

    # Reset batch state
    new_batch_key = f"crypto_market_pending/batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    with engine.connect() as conn:
        conn.execute(
            text("""
                UPDATE batch_state
                SET current_batch_s3_key = :new_s3_key,
                    current_batch_size_bytes = 0,
                    current_batch_rows = 0,
                    last_updated_at = NOW()
                WHERE batch_key = :batch_key
            """),
            {"new_s3_key": new_batch_key, "batch_key": batch_key},
        )
        conn.commit()

    return final_s3_key


def main() -> None:
    load_dotenv()

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

    cutoff = get_last_exported_ts(engine)

    if cutoff is None:
        query = "SELECT * FROM crypto_market ORDER BY last_updated"
    else:
        query = f"""
        SELECT * FROM crypto_market
        WHERE last_updated > '{cutoff.isoformat()}'
        ORDER BY last_updated
        """

    df = pd.read_sql(query, con=engine)

    if df.empty:
        print("No new rows to export.")
        return

    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    bucket = os.environ["MINIO_BUCKET_RAW"]
    batch_key = "crypto_market"

    # Get or create batch state
    batch_state = get_or_create_batch_state(engine, batch_key)

    # Append new rows to batch
    should_flush, new_size, new_rows = append_to_batch(
        s3_client, bucket, batch_state["s3_key"], df, engine, batch_key
    )

    if should_flush:
        # Download batch to get max timestamp before flushing
        local_temp = "data/temp/batch_for_max.parquet"
        s3_client.download_file(bucket, batch_state["s3_key"], local_temp)
        df_batch = pd.read_parquet(local_temp)
        max_value = df_batch["last_updated"].max()
        # Ensure we have a scalar Timestamp
        if isinstance(max_value, pd.Series):
            max_value = max_value.iloc[0]
        max_exported = pd.to_datetime(max_value, utc=True)
        if not isinstance(max_exported, pd.Timestamp) or pd.isna(max_exported):
            raise ValueError(
                "Cannot determine max timestamp: batch contains invalid timestamps"
            )

        # Flush batch to final location
        final_s3_key = flush_batch_to_final(
            s3_client, bucket, batch_state["s3_key"], engine, batch_key
        )
        print(
            f"Flushed batch: s3://{bucket}/{final_s3_key} "
            f"size={new_size / 1024 / 1024:.2f}MB rows={new_rows}"
        )

        # Update export metadata (only after successful flush)
        update_export_metadata(engine, "crypto_market", max_exported, new_rows)
    else:
        print(
            f"Accumulated in batch: {new_size / 1024 / 1024:.2f}MB "
            f"({new_rows} rows, target: {TARGET_BATCH_SIZE_MB}MB)"
        )


if __name__ == "__main__":
    main()
