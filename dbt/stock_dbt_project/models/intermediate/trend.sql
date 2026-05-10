with base as (
    select * from {{ ref('stg_stock_clean') }}
),

staged as (
    select
        ticker,
        trading_date,
        case 
            when close_price > open_price then 'UP'
            when close_price < open_price then 'DOWN'
            else 'SIDEWAY'
        end as daily_trend -- Một logic phân loại đơn giản
    from base
    where ticker is not null -- Lọc rác
)

select * from staged