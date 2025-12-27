-- 1. Monthly Revenue View (Materialized — Fast Aggregates)
CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_revenue_view AS
SELECT 
    d.year, 
    d.month, 
    ROUND(COALESCE(SUM(o.amount), 0), 2) AS total_revenue,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COUNT(*) AS total_orders
FROM dim_date d
LEFT JOIN fact_orders o ON d.date = o.order_date
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;

-- 2. Top Customers View (By Profit — Materialized for Speed)
CREATE MATERIALIZED VIEW IF NOT EXISTS top_customers_view AS
SELECT 
    c.customer_name, 
    c.state, 
    c.city,
    ROUND(SUM(o.profit), 2) AS total_profit,
    COUNT(*) AS order_count
FROM dim_customers c
JOIN fact_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.state, c.city
ORDER BY total_profit DESC
LIMIT 10;

-- Refresh Commands (Call from DAG)
COMMENT ON MATERIALIZED VIEW monthly_revenue_view IS 'Refreshed after ETL load';
COMMENT ON MATERIALIZED VIEW top_customers_view IS 'Refreshed after ETL load';