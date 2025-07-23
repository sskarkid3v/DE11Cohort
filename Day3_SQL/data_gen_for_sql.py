import pandas as pd
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Generate customers
customers = []
for i in range(1, 501):
    customers.append({
        'customer_id': i,
        'name': f'Customer {i}',
        'email': f'customer{i}@example.com',
        'created_at': datetime(2022, 1, 1) + timedelta(days=random.randint(0, 900))
    })

# Generate products
categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Beauty']
products = []
for i in range(1, 101):
    products.append({
        'product_id': i,
        'name': f'Product {i}',
        'category': random.choice(categories),
        'price': round(random.uniform(5, 500), 2)
    })

# Generate orders, order_items, payments
orders = []
order_items = []
payments = []
order_id = 1
order_item_id = 1
payment_id = 1

for cid in range(1, 501):
    num_orders = random.randint(5, 20)
    for _ in range(num_orders):
        o_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 900))
        orders.append({
            'order_id': order_id,
            'customer_id': cid,
            'order_date': o_date,
            'status': random.choice(['placed', 'paid', 'shipped', 'cancelled'])
        })

        num_items = random.randint(1, 5)
        total = 0
        for _ in range(num_items):
            pid = random.randint(1, 100)
            qty = random.randint(1, 3)
            price = next(p['price'] for p in products if p['product_id'] == pid)
            total += price * qty
            order_items.append({
                'order_item_id': order_item_id,
                'order_id': order_id,
                'product_id': pid,
                'quantity': qty
            })
            order_item_id += 1

        payments.append({
            'payment_id': payment_id,
            'order_id': order_id,
            'amount': round(total, 2),
            'payment_date': o_date + timedelta(days=random.randint(1, 7)),
            'method': random.choice(['credit_card', 'paypal', 'bank_transfer'])
        })
        payment_id += 1
        order_id += 1

# Convert to DataFrames
df_customers = pd.DataFrame(customers)
df_products = pd.DataFrame(products)
df_orders = pd.DataFrame(orders)
df_order_items = pd.DataFrame(order_items)
df_payments = pd.DataFrame(payments)

# Update this with your actual PostgreSQL connection
engine = create_engine("postgresql+psycopg2://demo_user:demo_pass@localhost:5432/salesdb")

# Load to PostgreSQL
df_customers.to_sql("customers", engine, if_exists="replace", index=False)
df_products.to_sql("products", engine, if_exists="replace", index=False)
df_orders.to_sql("orders", engine, if_exists="replace", index=False)
df_order_items.to_sql("order_items", engine, if_exists="replace", index=False)
df_payments.to_sql("payments", engine, if_exists="replace", index=False)

print("✅ Sample data successfully loaded into PostgreSQL.")
