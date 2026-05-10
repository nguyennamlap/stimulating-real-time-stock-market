{{ config(materialized='table') }}

with dates as (

    select distinct trading_date
    from {{ ref('stg_stock_clean') }}

),

final as (

    select
        trading_date,

        extract(year from trading_date) as year,
        extract(month from trading_date) as month,
        extract(day from trading_date) as day,

        extract(quarter from trading_date) as quarter,

        extract(dow from trading_date) as day_of_week,

        case 
            when extract(dow from trading_date) in (0,6) then 'weekend'
            else 'weekday'
        end as day_type

    from dates

)

select * from final