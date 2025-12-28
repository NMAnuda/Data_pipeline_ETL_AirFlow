import pandas as pd
from load import get_engine
from incremental import (
    get_last_load_date,
    apply_incremental_filter,
    update_last_load_date
)

def extract():
    engine = get_engine()
    last_date = get_last_load_date(engine)
    print(f"Incremental: Loading data after {last_date}")

    storage_options = {
        "key": "minioadmin",
        "secret": "minioadmin",
        "client_kwargs": {
            "endpoint_url": "http://minio:9000"
        }
    }

    orders = pd.read_csv(
        "s3://ecommerce-raw-data/List_of_Orders.csv",
        storage_options=storage_options
    )

    details = pd.read_csv(
        "s3://ecommerce-raw-data/Order Details.csv",
        storage_options=storage_options
    )

    targets = pd.read_csv(
        "s3://ecommerce-raw-data/Sales target.csv",
        storage_options=storage_options
    )

    # Convert date BEFORE filtering
    orders['Order Date'] = pd.to_datetime(
        orders['Order Date'],
        dayfirst=True
    )

    # Incremental filter
    orders = apply_incremental_filter(
        orders,
        last_date,
        'Order Date'
    )
    print(f"New orders: {len(orders)} records")

    # Filter details for new orders only
    new_order_ids = orders['Order ID'].unique()
    details = details[details['Order ID'].isin(new_order_ids)].copy()
    print(f"New details: {len(details)} records")

    # Update last load date
    if not orders.empty:
        latest_date = orders['Order Date'].max()
        update_last_load_date(engine, latest_date)
        print(f"Updated last_load_date to {latest_date}")

    return orders, details, targets
