-- FIXED: Create postgres user/role
CREATE USER postgres WITH SUPERUSER PASSWORD '17970';

-- FIXED: Create ecommerce_dw DB
CREATE DATABASE IF NOT EXISTS ecommerce_dw OWNER postgres;

-- Connect to ecommerce_dw
\c ecommerce_dw

-- FIXED: Create public schema
CREATE SCHEMA IF NOT EXISTS public;

-- FIXED: Create tables for ETL
CREATE TABLE IF NOT EXISTS public.dim_customers (
  customer_id SERIAL PRIMARY KEY,
  "CustomerName" VARCHAR(255),
  state VARCHAR(100),
  city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS public.dim_products (
  product_id SERIAL PRIMARY KEY,
  category VARCHAR(100),
  "Sub-Category" VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS public.dim_date (
  id SERIAL PRIMARY KEY,
  date DATE UNIQUE,
  year INTEGER,
  month INTEGER
);

CREATE TABLE IF NOT EXISTS public.fact_orders (
  order_id VARCHAR PRIMARY KEY,
  order_date DATE,
  customer_id INTEGER REFERENCES public.dim_customers(customer_id),
  product_id INTEGER REFERENCES public.dim_products(product_id),
  quantity INTEGER,
  amount DECIMAL(10,2),
  profit DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS public.fact_sales_targets (
  id SERIAL PRIMARY KEY,
  year INTEGER,
  month INTEGER,
  category VARCHAR(100),
  target DECIMAL(10,2)
);

-- FIXED: Incremental tracker
CREATE TABLE IF NOT EXISTS public.load_tracker (
  id SERIAL PRIMARY KEY,
  last_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- FIXED: Grant permissions
GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO postgres;