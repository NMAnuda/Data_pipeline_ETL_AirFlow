import pandas as pd
from load import get_engine
from incremental import get_last_load_date, apply_incremental_filter, update_last_load_date

def extract():
    engine = get_engine()
    last_date = get_last_load_date(engine)
    print(f"Incremental: Loading data after {last_date}")

    orders = pd.read_csv('data/raw/List_of_Orders.csv')
    details = pd.read_csv('data/raw/Order Details.csv')
    targets = pd.read_csv('data/raw/Sales target.csv')

    # Filter ONLY orders by date (details has no 'Order Date')
    orders = apply_incremental_filter(orders, last_date, 'Order Date')
    print(f"New orders: {len(orders)} records")

    # Filter details to only new orders (join on Order ID)
    new_order_ids = orders['Order ID'].unique()
    details = details[details['Order ID'].isin(new_order_ids)].copy()
    print(f"New details: {len(details)} records for new orders")

    # Update tracker with latest date
    if len(orders) > 0:
        latest_date = orders['Order Date'].max()
        update_last_load_date(engine, latest_date)
        print(f"Updated last_load_date to {latest_date}")

    return orders, details, targets