
select 
    ticker,
    trading_date,
    count(*) as cnt
from {{ ref('fact_stock') }}
group by ticker, trading_date
having count(*) > 1