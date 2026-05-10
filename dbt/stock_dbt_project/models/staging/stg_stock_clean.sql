{{ config(materialized='view') }}
-- materialized='view' = model này sẽ được tạo thành VIEW trong database

with source as (
    select * from {{ ref('stg_stock') }}
),

cleaned as (
    select
        -- Primary 
        ticker,
        trading_date,
        -- Price Data
        close_price,
        adjusted_price,
        open_price, 
        high_price,
        low_price,
        -- Volume & Value
        volume,
        match_value,
        price_change,
        -- Indicators
        ema_20,
        ema_50,
        ema_200,
        rsi,
        macd_line,
        macd_signal,
        macd_hist,
        bb_middle,
        bb_upper,
        bb_lower
    from source
    where close_price is not null

)

select
    -- Primary 
    ticker,
    trading_date,
    -- Price Data
    close_price,
    adjusted_price,
    open_price, 
    high_price,
    low_price,
    -- Volume & Value
    volume,
    match_value,
    price_change,
    -- Indicators
    ema_20,
    ema_50,
    ema_200,
    rsi,
    macd_line,
    macd_signal,
    macd_hist,
    bb_middle,
    bb_upper,
    bb_lower
from cleaned
-- 👉 dbt sẽ tạo:
-- create view clean as (
--     select ...
-- )