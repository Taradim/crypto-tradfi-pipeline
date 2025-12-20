from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd


def aggregate_small_files(
    s3_client, bucket: str, source_prefix: str, target_prefix: str, target_size_mb: int = 100
) -> None:
    """Combine small Parquet files into larger ones."""
    # List all files in source prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=source_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                })
    
    # Group files into batches (combine until ~target_size)
    target_size_bytes = target_size_mb * 1024 * 1024
    batches = []
    current_batch = []
    current_size = 0
    
    for file in files:
        if current_size + file["size"] > target_size_bytes and current_batch:
            batches.append(current_batch)
            current_batch = [file]
            current_size = file["size"]
        else:
            current_batch.append(file)
            current_size += file["size"]
    
    if current_batch:
        batches.append(current_batch)
    
    # Process each batch
    for i, batch in enumerate(batches):
        dfs = []
        for file in batch:
            local_temp = f"data/temp/agg_{i}_{os.path.basename(file['key'])}"
            s3_client.download_file(bucket, file["key"], local_temp)
            dfs.append(pd.read_parquet(local_temp))
        
        # Combine and save
        df_combined = pd.concat(dfs, ignore_index=True)
        agg_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
        agg_key = f"{target_prefix}/aggregated_{agg_ts}_{i}.parquet"
        local_agg = f"data/temp/agg_{i}_final.parquet"
        df_combined.to_parquet(local_agg, index=False)
        
        # Upload aggregated file
        s3_client.upload_file(local_agg, bucket, agg_key)
        print(f"Aggregated {len(batch)} files → s3://{bucket}/{agg_key}")
        
        # Optional: delete original small files
        # for file in batch:
        #     s3_client.delete_object(Bucket=bucket, Key=file["key"])