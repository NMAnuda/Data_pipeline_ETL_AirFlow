from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def load_table(df, table_name, engine):
    df.to_sql(
        table_name,
        engine,
        if_exists='append',
        index=False
    )


def load(
    dim_customers,
    dim_products,
    dim_date,
    fact_orders,
    fact_sales_targets
):
    engine = get_engine()

    # Load DIMENSIONS first
    load_table(dim_customers, 'dim_customers', engine)
    load_table(dim_products, 'dim_products', engine)
    load_table(dim_date, 'dim_date', engine)

    # Load FACTS
    load_table(fact_orders, 'fact_orders', engine)
    load_table(fact_sales_targets, 'fact_sales_targets', engine)

    print("✅ Data loaded successfully into PostgreSQL")
