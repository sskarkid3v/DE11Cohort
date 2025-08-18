CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    updated_at TIMESTAMP NOT NULL
);