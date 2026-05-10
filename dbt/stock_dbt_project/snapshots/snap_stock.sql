{% snapshot snap_stock %}

{{
    config(
        target_schema='snapshots',
        unique_key='code',
        strategy='check',
        check_cols=['company_name', 'sector']
    )
}}

select *
from {{ ref('stock_info') }}

{% endsnapshot %}

-- 🔥 Kết quả:
-- code	company_name	sector	dbt_valid_from	dbt_valid_to

-- 👉 sẽ track được:
-- lịch sử thay đổi dữ liệu