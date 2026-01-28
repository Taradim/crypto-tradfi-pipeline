-- Silver model for CoinGecko cryptocurrency market data
-- This model reads raw data from S3 bronze layer and standardizes column names and types
-- Silver layer: Cleaned and validated data (Bronze data is already written by Python pipelines)

{{ config(
    materialized='external',
    location='silver/coingecko.parquet',
    glue_register=true,
    glue_database='data_pipeline_portfolio'
) }}

with source as (
    select * from read_parquet('s3://{{ var("s3_bucket_name") }}/bronze/coingecko/**/*.parquet')
    where id is not null
),

renamed as (
    select
        -- Primary keys
        id as coin_id,
        symbol as coin_symbol,
        name as coin_name,

        -- Price data
        current_price as price_usd,
        high_24h as high_24h_usd,
        low_24h as low_24h_usd,

        -- Market metrics
        market_cap as market_cap_usd,
        market_cap_rank,
        total_volume as volume_24h_usd,

        -- Supply metrics
        circulating_supply,
        total_supply,
        max_supply,

        -- Price change percentages (column names from CoinGecko API include "_in_currency" suffix)
        price_change_percentage_7d_in_currency as price_change_pct_7d,
        price_change_percentage_14d_in_currency as price_change_pct_14d,
        price_change_percentage_30d_in_currency as price_change_pct_30d,
        price_change_percentage_200d_in_currency as price_change_pct_200d,
        price_change_percentage_1y_in_currency as price_change_pct_1y,

        -- All-time high/low
        ath as all_time_high_usd,
        ath_change_percentage as ath_change_pct,
        ath_date as ath_date_at,
        atl as all_time_low_usd,
        atl_change_percentage as atl_change_pct,
        atl_date as atl_date_at,

        -- Timestamps
        last_updated as last_updated_at,

        -- Metadata
        image as coin_image_url,

        -- Ingestion metadata
        current_date as ingestion_date

    from source
),

final as (
    select
        coin_id,
        coin_symbol,
        coin_name,
        price_usd,
        high_24h_usd,
        low_24h_usd,
        market_cap_usd,
        market_cap_rank,
        volume_24h_usd,
        circulating_supply,
        total_supply,
        max_supply,
        price_change_pct_7d,
        price_change_pct_14d,
        price_change_pct_30d,
        price_change_pct_200d,
        price_change_pct_1y,
        all_time_high_usd,
        ath_change_pct,
        ath_date_at,
        all_time_low_usd,
        atl_change_pct,
        atl_date_at,
        last_updated_at,
        coin_image_url,
        ingestion_date
    from renamed
)

select * from final
