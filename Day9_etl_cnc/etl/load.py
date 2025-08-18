import psycopg2
from psycopg2.extras import execute_batch

upsert_sql = """
INSERT INTO fact_orders (order_id, order_date, customer_id, amount, updated_at)
VALUES (%(order_id)s, %(order_date)s, %(customer_id)s, %(amount)s, %(updated_at)s)
ON CONFLICT (order_id) DO UPDATE SET
    order_date = EXCLUDED.order_date,
    customer_id = EXCLUDED.customer_id,
    amount = EXCLUDED.amount,
    updated_at = EXCLUDED.updated_at
WHERE fact_orders.updated_at < EXCLUDED.updated_at;
"""

def upsert_rows(conn_str, rows):
    if not rows:
        return 0
    conn = psycopg2.connect(conn_str)
    try:
        with conn, conn.cursor() as cur:
            execute_batch(cur, upsert_sql, rows, page_size=500)
        return len(rows)
    finally:
        conn.close()
            