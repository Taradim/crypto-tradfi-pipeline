-- Silver model for DeFiLlama protocol data
-- Reads raw data from S3 bronze layer and standardizes column names and types
-- Bronze data is written by the DeFiLlama Airflow pipeline (protocols endpoint)

{{ config(
    materialized='external',
    location='silver/defillama.parquet',
    glue_register=true,
    glue_database='data_pipeline_portfolio'
) }}

with source as (
    select * from read_parquet('s3://{{ var("s3_bucket_name") }}/bronze/defillama/**/*.parquet')
    where id is not null
),

renamed as (
    select
        -- Primary keys
        id as protocol_id,
        name as protocol_name,
        slug as protocol_slug,
        symbol as protocol_symbol,

        -- Category and chain
        category as protocol_category,
        chain as primary_chain,

        -- TVL metrics (USD)
        tvl as tvl_usd,
        mcap as market_cap_usd,

        -- TVL change percentages
        change_1h as tvl_change_pct_1h,
        change_1d as tvl_change_pct_1d,
        change_7d as tvl_change_pct_7d,

        -- Metadata
        url as protocol_url,
        logo as protocol_logo_url,
        "listedAt" as listed_at_timestamp,
        gecko_id as coingecko_id,

        -- Complex columns kept as string for reference (JSON in bronze)
        chains as chains_json,
        "chainTvls" as chain_tvls_json,

        current_date as ingestion_date
    from source
),

final as (
    select
        protocol_id,
        protocol_name,
        protocol_slug,
        protocol_symbol,
        protocol_category,
        primary_chain,
        tvl_usd,
        market_cap_usd,
        tvl_change_pct_1h,
        tvl_change_pct_1d,
        tvl_change_pct_7d,
        protocol_url,
        protocol_logo_url,
        listed_at_timestamp,
        coingecko_id,
        chains_json,
        chain_tvls_json,
        ingestion_date
    from renamed
)

select * from final
