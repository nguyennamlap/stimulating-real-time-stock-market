with base as (
    select
        ticker,
        trading_date,
        lag(trading_date) over (partition by ticker order by trading_date) as prev_trading_date
    from {{ ref('stg_stock') }}
),
gaps as (
    select *,
        (trading_date - prev_trading_date) as gap_days
    from base
)

select *
from gaps
where gap_days > 1

-- Chọn những ngày ko giao dịch hoặc nghỉ lễ