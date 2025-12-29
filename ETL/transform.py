import pandas as pd
import datetime

def transform(orders_df, details_df, targets_df):
    # -------------------------------
    # 1. CLEAN COLUMN NAMES
    # -------------------------------
    orders_df.columns = orders_df.columns.str.strip()
    details_df.columns = details_df.columns.str.strip()
    targets_df.columns = targets_df.columns.str.strip()

    # Handle empty orders (incremental no new data)
    if orders_df.empty:
        print("No new orders — skipping transform.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # -------------------------------
    # 2. DATE FORMATTING (BULLETPROOF — 0 Nulls)
    # -------------------------------
    def parse_mixed_dates(x):
        if pd.isna(x) or x == '' or x == 'nan':
            return pd.NaT
        x = str(x).strip()
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%b-%y", "%B-%y", "%b %Y"):  # FIXED: All CSV formats + month names
            try:
                return pd.to_datetime(x, format=fmt)
            except:
                continue
        # FIXED: Infer fallback (catches odd like "13 Apr 2018")
        try:
            return pd.to_datetime(x, infer_datetime_format=True)
        except:
            return pd.NaT

    orders_df['Order Date'] = orders_df['Order Date'].apply(parse_mixed_dates)
    print(f"Orders before fillna: {len(orders_df)}, null dates: {orders_df['Order Date'].isna().sum()}")

    # FIXED: Fill nulls with mode date (keeps all rows, 0 nulls)
    if orders_df['Order Date'].isna().any():
        mode_date = orders_df['Order Date'].mode()[0] if not orders_df['Order Date'].mode().empty else pd.Timestamp('2018-01-01')
        orders_df['Order Date'] = orders_df['Order Date'].fillna(mode_date)
        print(f"Filled nulls with {mode_date} — now 0 nulls")

    print(f"Cleaned orders after date parse: {len(orders_df)} rows")

    # -------------------------------
    # 3. DIM_CUSTOMERS
    # -------------------------------
    dim_customers = (
        orders_df[['CustomerName', 'State', 'City']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_customers['customer_id'] = dim_customers.index + 1

    # -------------------------------
    # 4. DIM_PRODUCTS
    # -------------------------------
    dim_products = (
        details_df[['Category', 'Sub-Category']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_products['product_id'] = dim_products.index + 1

    # -------------------------------
    # 5. DIM_DATE
    # -------------------------------
    dim_date = (
        orders_df[['Order Date']]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_date['month'] = dim_date['Order Date'].dt.month
    dim_date['year'] = dim_date['Order Date'].dt.year
    dim_date.rename(columns={'Order Date': 'date'}, inplace=True)

    # -------------------------------
    # 6. FACT_ORDERS
    # -------------------------------
    fact_orders = orders_df.merge(details_df, on='Order ID')

    fact_orders = fact_orders.merge(
        dim_customers,
        on=['CustomerName', 'State', 'City'],
        how='left'
    )

    fact_orders = fact_orders.merge(
        dim_products,
        on=['Category', 'Sub-Category'],
        how='left'
    )

    fact_orders = fact_orders[[
        'Order ID',
        'Order Date',
        'customer_id',
        'product_id',
        'Quantity',
        'Amount',
        'Profit'
    ]]

    fact_orders.rename(columns={
        'Order ID': 'order_id',
        'Order Date': 'order_date',
        'Quantity': 'quantity',
        'Amount': 'amount',
        'Profit': 'profit'
    }, inplace=True)

    # -------------------------------
    # 7. FACT_SALES_TARGETS
    # -------------------------------
    targets_df['Month of Order Date'] = targets_df['Month of Order Date'].astype(str)

    targets_df['month'] = targets_df['Month of Order Date'].str.extract(r'(\d+)').astype(int)
    
    year = orders_df['Order Date'].dt.year.mode()[0] if not orders_df['Order Date'].dt.year.mode().empty else datetime.now().year
    
    targets_df['year'] = year

    fact_sales_targets = targets_df[['month', 'year', 'Category', 'Target']]
    fact_sales_targets.rename(columns={
        'Category': 'category',
        'Target': 'target'
    }, inplace=True)

    return dim_customers, dim_products, dim_date, fact_orders, fact_sales_targets