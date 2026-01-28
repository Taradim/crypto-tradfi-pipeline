-- Silver model for Yahoo Finance stock market data
-- Reads raw data from S3 bronze layer and standardizes column names and types
-- Bronze data is written by the Yahoo Finance Airflow pipeline (yfinance)

with source as (
    select * from read_parquet('s3://{{ var("s3_bucket_name") }}/bronze/yahoo_finance/**/*.parquet')
    where "Symbol" is not null
      and "Date" is not null
),

renamed as (
    select
        -- Primary keys
        "Symbol" as symbol,
        cast("Date" as date) as trade_date,

        -- OHLCV
        "Open" as open_usd,
        "High" as high_usd,
        "Low" as low_usd,
        "Close" as close_usd,
        "Volume" as volume,

        -- Optional columns from yfinance history()
        "Dividends" as dividends,
        "Stock Splits" as stock_splits,

        current_date as ingestion_date
    from source
),

final as (
    select
        symbol,
        trade_date,
        open_usd,
        high_usd,
        low_usd,
        close_usd,
        volume,
        dividends,
        stock_splits,
        ingestion_date
    from renamed
)

select * from final
