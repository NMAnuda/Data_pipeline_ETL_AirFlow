{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('ecommerce', 'fact_orders') }}
),

cleaned as (
    select
        order_id,
        order_date::date as order_date,
        customer_id,
        product_id,
        quantity,
        amount,
        profit
    from source
    where order_id is not null
      and order_date is not null
      and amount > 0
      and profit >= -amount * 2
),

deduped as (
    select distinct on (order_id) *
    from cleaned
    order by order_id, order_date
)


select *
from deduped
