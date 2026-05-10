select *
from {{ ref('mart_stock_price') }}
where adjusted_price < 0
   or close_price < 0
   or open_price < 0
   or high_price < 0
   or low_price < 0