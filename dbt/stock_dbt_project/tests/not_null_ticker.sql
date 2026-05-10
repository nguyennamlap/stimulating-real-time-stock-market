
select *
from {{ ref('fact_stock') }}
where ticker is null
   or trading_date is null