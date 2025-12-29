select 
    p."Category",         -- use the correct column name here
    p."Sub-Category",
    sum(o.amount) as total_revenue
from {{ source('ecommerce', 'fact_orders') }} o
join {{ source('ecommerce', 'dim_products') }} p
    on o.product_id = p.product_id
group by 1,2
order by total_revenue desc
