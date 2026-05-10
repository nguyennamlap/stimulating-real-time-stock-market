{{
  config(
    materialized='incremental',
    unique_key=['ticker', 'trading_date'],
    on_schema_change='fail'
  )
}} -- ko load lại dữ liệu cũ 

with source as (
    select
        code,
        ngay_ts,
        giadieuchinh,
        giadongcua,
        giamocua,
        giacaonhat,
        giathapnhat,
        khoiluongkhoplenh,
        giatrikhoplenh,
        thaydoi,
        ema_20,
        ema_50,
        ema_200,
        rsi,
        macd_line,
        macd_signal,
        macd_hist,
        bb_middle,
        bb_upper,
        bb_lower
    from {{ source('raw_data', 'stock_table') }}
),

renamed as (
    select
        -- dữ liệu gốc
        code as ticker,
        cast(ngay_ts as date) as trading_date,

        cast(giadieuchinh as numeric) as adjusted_price,
        cast(giadongcua as numeric) as close_price,
        cast(giamocua as numeric) as open_price,
        cast(giacaonhat as numeric) as high_price,
        cast(giathapnhat as numeric) as low_price,

        cast(khoiluongkhoplenh as bigint) as volume,
        cast(giatrikhoplenh as numeric) as match_value,
        cast(thaydoi as numeric) as price_change,

        -- indicator
        cast(ema_20 as numeric) as ema_20,
        cast(ema_50 as numeric) as ema_50,
        cast(ema_200 as numeric) as ema_200,
        cast(rsi as numeric) as rsi,
        cast(macd_line as numeric) as macd_line,
        cast(macd_signal as numeric) as macd_signal,
        cast(macd_hist as numeric) as macd_hist,
        cast(bb_middle as numeric) as bb_middle,
        cast(bb_upper as numeric) as bb_upper,
        cast(bb_lower as numeric) as bb_lower

    from source
    where code is not null
)

select * from renamed

{% if is_incremental() %}
where trading_date > (select max(trading_date) from {{ this }}) -- this là model hiện tại đang chạy
{% endif %} -- điều kiện ko load lại dữ liệu cũ 