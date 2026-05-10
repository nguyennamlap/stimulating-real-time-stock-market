{{ config(materialized='table') }}

with base as (

    select * from {{ ref('feature_engineering') }}

),

signal as (

    select
        ticker,
        trading_date,
        close_price,

        rsi,
        macd_line,
        macd_signal,

        -- RSI signal
        case
            when rsi < 30 then 'BUY'
            when rsi > 70 then 'SELL'
            else 'WAIT'
        end as rsi_signal,

        -- MACD signal
        case
            when macd_line > macd_signal then 'BUY'
            else 'SELL'
        end as macd_signal_type,

        -- EMA trend
        case
            when ema_20 > ema_50 and ema_50 > ema_200 then 'UPTREND'
            when ema_20 < ema_50 and ema_50 < ema_200 then 'DOWNTREND'
            else 'SIDEWAY'
        end as trend

    from base

)

select * from signal