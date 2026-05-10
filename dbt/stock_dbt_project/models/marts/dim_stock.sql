{{ config(materialized='table') }}

with source as (

    select distinct ticker
    from {{ ref('stg_stock_clean') }}

),

final as (

    select
        ticker,

        -- mapping cơ bản
        case 
            when ticker = 'FPT' then 'FPT Corporation'
            when ticker = 'HPG' then 'Hoa Phat Group'
            when ticker = 'MWG' then 'Mobile World Group'
            when ticker = 'VNM' then 'Vinamilk'
            else 'Unknown'
        end as company_name,

        case 
            when ticker = 'FPT' then 'Technology'
            when ticker = 'HPG' then 'Heavy Industry' -- Thép
            when ticker = 'MWG' then 'Consumer Discretionary' -- Bán lẻ kỹ thuật số
            when ticker = 'VNM' then 'Consumer Staples' -- Hàng tiêu dùng thiết yếu
            else 'Other'
        end as sector

    from source

)

select * from final