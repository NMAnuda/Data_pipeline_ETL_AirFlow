import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_engine():
    load_dotenv()
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT') or '5432'
    name = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    print(f"DEBUG: Host={host}, Port={port}, Name={name}, User={user}")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    )

def load(dim_customers, dim_products, dim_date, fact_orders, fact_targets):
    engine = get_engine()
    with engine.begin() as conn:
        # FIXED: No TRUNCATE or UPSERT — Simple append (keeps previous data)
        print("Appending to existing tables (no delete/upsert)")

        # Append dims (unique, no dups)
        dim_customers.to_sql('dim_customers', con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
        dim_products.to_sql('dim_products', con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
        dim_date.to_sql('dim_date', con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
        fact_targets.to_sql('fact_sales_targets', con=engine, if_exists='append', index=False, chunksize=1000, method='multi')

        # Append fact_orders (simple — no upsert for now)
        fact_orders.to_sql('fact_orders', con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
        
        print(f"Appended: customers={len(dim_customers)}, products={len(dim_products)}, orders={len(fact_orders)}")
    
    print(" Data loaded successfully into PostgreSQL (appended)")
