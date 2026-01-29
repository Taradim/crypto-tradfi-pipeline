-- Gold model: cryptocurrency prices (Plan V1 - Step 3 dbt)
-- Aggregates and enriches CoinGecko Silver data for analytics
-- Gold layer: business-ready analytical data; refs coingecko Silver model

{{ config(location=get_external_location(this)) }}

with silver as (
    select * from {{ ref('coingecko') }}
),

enriched as (
    select
        coin_id,
        coin_symbol,
        coin_name,
        price_usd,
        market_cap_usd,
        market_cap_rank,
        volume_24h_usd,
        
        -- Calculate derived metrics using macro
        {{ calculate_ratio('volume_24h_usd', 'market_cap_usd') }} * 100 as volume_to_market_cap_ratio,
        
        {{ calculate_ratio('market_cap_usd', 'circulating_supply') }} as price_per_coin,
        
        -- Price change categories
        case
            when price_change_pct_7d > 10 then 'strong_uptrend'
            when price_change_pct_7d > 5 then 'uptrend'
            when price_change_pct_7d > -5 then 'sideways'
            when price_change_pct_7d > -10 then 'downtrend'
            else 'strong_downtrend'
        end as trend_7d,
        
        -- Market cap categories
        case
            when market_cap_usd >= 10000000000 then 'large_cap'  -- >= $10B
            when market_cap_usd >= 1000000000 then 'mid_cap'     -- >= $1B
            when market_cap_usd >= 100000000 then 'small_cap'     -- >= $100M
            else 'micro_cap'
        end as market_cap_category,
        
        -- Price metrics
        high_24h_usd,
        low_24h_usd,
        all_time_high_usd,
        all_time_low_usd,
        
        -- Supply metrics
        circulating_supply,
        total_supply,
        {{ calculate_ratio('circulating_supply', 'total_supply') }} * 100 as supply_circulation_pct,
        
        -- Price change percentages
        price_change_pct_7d,
        price_change_pct_14d,
        price_change_pct_30d,
        price_change_pct_200d,
        price_change_pct_1y,
        
        -- Timestamps
        last_updated_at,
        ingestion_date
        
    from silver
)

select * from enriched
