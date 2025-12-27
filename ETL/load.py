import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_engine():
    load_dotenv()  # Reload env
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT') or '5432'  # Fallback
    name = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    print(f"DEBUG: Host={host}, Port={port}, Name={name}, User={user}")  # Temp debug
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    )

def load(dim_customers, dim_products, dim_date, fact_orders, fact_targets):
    engine = get_engine()
    with engine.begin() as conn:  # FIXED: Use engine.begin() for auto-commit transaction
        # Truncate for idempotency (or use UPSERT for incremental)
        conn.execute(text("TRUNCATE TABLE dim_customers, dim_products, dim_date, fact_orders, fact_sales_targets;"))
        
        # Load dims (replace if exists)
        dim_customers.to_sql('dim_customers', con=engine, if_exists='append', index=False)
        dim_products.to_sql('dim_products', con=engine, if_exists='append', index=False)
        dim_date.to_sql('dim_date', con=engine, if_exists='append', index=False)
        fact_orders.to_sql('fact_orders', con=engine, if_exists='append', index=False)
        fact_targets.to_sql('fact_sales_targets', con=engine, if_exists='append', index=False)
    
    print("✅ Data loaded successfully into PostgreSQL")