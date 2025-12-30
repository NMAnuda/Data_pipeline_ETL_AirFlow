{{ config(materialized='view') }}

select
    o.order_date,
    o.amount,
    row_number() over (order by o.amount desc) as revenue_rank
from {{ ref('stg_fact_orders') }} o