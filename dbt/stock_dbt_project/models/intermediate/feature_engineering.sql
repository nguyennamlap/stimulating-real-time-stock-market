with base as (
    select * from {{ ref('stg_stock_clean') }}
),

feature as (
    select
        *,
        
        -- giá đóng cửa ngày hôm trước (biến động theo %)
        (close_price - lag(close_price) over w) / lag(close_price) over w as return_1d,
        
        -- 👉 lead() = lấy giá ngày tiếp theo
        lead(close_price) over w as future_price,
        (lead(close_price) over w - close_price) / close_price as future_return,
        -- Nếu > 0 → giá tăng
        -- Nếu < 0 → giá giảm
        
        -- classification label
        case 
            when (lead(close_price) over w - close_price) / close_price > 0.02 then 1
            else 0
        end as label

    from base

    window w as (partition by ticker order by trading_date)
)
select * from feature

