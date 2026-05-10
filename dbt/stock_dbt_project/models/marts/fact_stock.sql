{{ config(materialized='table') }}

with base_data as (
    -- Dùng bảng staging đã clean làm gốc để đảm bảo không bị sót ngày giao dịch nào
    select * from {{ ref('stg_stock_clean') }} 
),
feature_intermediate as(
    select * from {{ ref('feature_engineering') }}
),
trend_features as (
    select * from {{ ref('trend') }}
),

gap_features as (
    select * from {{ ref('gap') }}
),

final_joined as (
    select
        b.*, -- Lấy toàn bộ thông tin cơ bản (giá, khối lượng, v.v.)
        
        f.return_1d, 
        f.future_return,
        f.label,
        
        -- Chọn lọc các cột feature từ gap
        g.gap_days,
        t.daily_trend 
     
    from base_data b
  
    left join feature_intermediate f
        on b.ticker = f.ticker 
        and b.trading_date = f.trading_date

    left join trend_features t
        on b.ticker = t.ticker 
        and b.trading_date = t.trading_date
        
    left join gap_features g
        on b.ticker = g.ticker 
        and b.trading_date = g.trading_date
        

)

select * from final_joined


