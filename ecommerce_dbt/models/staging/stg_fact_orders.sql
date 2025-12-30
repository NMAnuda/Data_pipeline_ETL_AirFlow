{{ config(materialized='table') }}

{# Check if the source relation exists; if not, return an empty set so dbt run doesn't fail #}
{% set src_relation = source('ecommerce', 'fact_orders') %}
{% set exists = adapter.get_relation(
    database=src_relation.database,
    schema=src_relation.schema,
    identifier=src_relation.identifier
) %}

{% if not exists %}
select
  null::varchar as order_id,
  null::date as order_date,
  null::int as customer_id,
  null::int as product_id,
  null::int as quantity,
  null::numeric as amount,
  null::numeric as profit
where false
{% else %}

with source as (
    select *
    from {{ src_relation }}
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

{% endif %}