from extract import extract
from transform import transform
from load import load

orders, details, targets = extract()

dim_customers, dim_products, dim_date, fact_orders, fact_targets = transform(
    orders, details, targets
)

load(
    dim_customers,
    dim_products,
    dim_date,
    fact_orders,
    fact_targets
)
