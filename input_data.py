import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

# Number of records
NUM_CUSTOMERS = 100
NUM_ORDERS = 300
NUM_PRODUCTS = 50
NUM_ORDER_ITEMS = 500

# Generate Customers Data
def generate_customers(num_customers):
    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": i,
            "name": f"Customer_{i}",
            "email": f"customer_{i}@example.com",
            "created_at": datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        })
    return pd.DataFrame(customers)

# Generate Products Data
def generate_products(num_products):
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Accessories"]
    products = []
    for i in range(1, num_products + 1):
        products.append({
            "product_id": i,
            "name": f"Product_{i}",
            "category": random.choice(categories),
            "price": round(random.uniform(10, 500), 2)
        })
    return pd.DataFrame(products)

# Generate Orders Data
def generate_orders(num_orders, num_customers):
    orders = []
    for i in range(1, num_orders + 1):
        orders.append({
            "order_id": i,
            "customer_id": random.randint(1, num_customers),
            "order_date": datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365)),
            "total_amount": round(random.uniform(20, 1000), 2)
        })
    return pd.DataFrame(orders)

# Generate Order Items Data
def generate_order_items(num_order_items, num_orders, num_products):
    order_items = []
    for i in range(1, num_order_items + 1):
        order_items.append({
            "order_item_id": i,
            "order_id": random.randint(1, num_orders),
            "product_id": random.randint(1, num_products),
            "quantity": random.randint(1, 5),
            "price_per_unit": round(random.uniform(10, 500), 2)
        })
    return pd.DataFrame(order_items)

# Generate datasets
customers_df = generate_customers(NUM_CUSTOMERS)
products_df = generate_products(NUM_PRODUCTS)
orders_df = generate_orders(NUM_ORDERS, NUM_CUSTOMERS)
order_items_df = generate_order_items(NUM_ORDER_ITEMS, NUM_ORDERS, NUM_PRODUCTS)

# Save as Parquet files
def save_parquet(df, filename):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)

save_parquet(customers_df, "customers.parquet")
save_parquet(products_df, "products.parquet")
save_parquet(orders_df, "orders.parquet")
save_parquet(order_items_df, "order_items.parquet")

print("Parquet files generated successfully! 🎉")
