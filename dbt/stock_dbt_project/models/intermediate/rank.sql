with ranked as (

    select 
        ticker, 
        trading_date,
        row_number() over (
            partition by ticker, trading_date
            order by trading_date desc
        ) as dedup_rank

    from {{ ref('stg_stock') }}

)

select *
from ranked
where dedup_rank = 1
-- Mỗi record chỉ có 1 