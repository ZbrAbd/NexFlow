import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Create a folder called data/raw to save our CSV files
os.makedirs("data/raw", exist_ok=True)
# exist_ok=True means: don't crash if folder already exists

segments = ["Enterprise", "Mid-Market", "SMB", "Consumer"]
# These are common B2B customer types — good to know for interviews

customers = []  # empty list, we'll fill it

for i in range(1, 5001):  # 5000 customers
    customers.append({
        "customer_id":       i,
        "customer_name":     fake.name(),
        "email":             fake.email() if random.random() > 0.05 else None,
        # ↑ 95% chance of real email, 5% chance of NULL (intentional bad data)
        "segment":           random.choice(segments),
        "city":              fake.city(),
        "state":             fake.state_abbr(),
        "country":           "US",
        "registration_date": fake.date_between(start_date="-5y", end_date="-6m"),
    })

dim_customers = pd.DataFrame(customers)
dim_customers.to_csv("data/raw/dim_customers.csv", index=False)
# index=False means: don't save the row numbers (0,1,2...) as a column


categories = {
    "Electronics": ["Laptops", "Monitors", "Accessories"],
    "Office":      ["Furniture", "Supplies", "Printers"],
    "Warehouse":   ["Shelving", "Forklifts", "Safety Gear"],
}

products = []
for i in range(1, 501):  # 500 products
    category   = random.choice(list(categories.keys()))
    sub_cat    = random.choice(categories[category])
    cost       = round(random.uniform(5, 800), 2)       # random price between $5-$800
    list_price = round(cost * random.uniform(1.2, 2.5), 2)  # sell price = cost × markup

    products.append({
        "product_id":   i,
        "product_name": f"{fake.word().capitalize()} {sub_cat} {i}",
        "category":     category,
        "sub_category": sub_cat,
        "cost_price":   cost if random.random() > 0.02 else None,  # 2% nulls
        "list_price":   list_price,
    })

dim_products = pd.DataFrame(products)
dim_products.to_csv("data/raw/dim_products.csv", index=False)

statuses = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
weights  = [0.65, 0.15, 0.10, 0.06, 0.04]
# 65% of orders are Delivered, only 4% Returned — realistic distribution

orders = []
for i in range(1, 55001):  # slightly over 50k (we'll add dupes too)
    order_date    = fake.date_between(start_date="-2y", end_date="today")
    ship_date     = order_date + timedelta(days=random.randint(1, 5))
    delivery_date = ship_date  + timedelta(days=random.randint(1, 7))

    quantity     = random.randint(1, 50)
    unit_price   = round(random.uniform(10, 1000), 2)
    discount_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
    total        = round(unit_price * quantity * (1 - discount_pct), 2)

    # ── Intentional data quality issues ──────────────────
    if random.random() < 0.03: quantity   = -quantity      # negative qty
    if random.random() < 0.02: total      = -abs(total)    # negative total
    if random.random() < 0.04: ship_date  = None           # missing ship date
    # ─────────────────────────────────────────────────────

    orders.append({
        "order_line_id": i,
        "customer_id":   random.randint(1, 5000) if random.random() > 0.01 else None,
        "product_id":    random.randint(1, 500),
        "warehouse_id":  random.randint(1, 10),
        "order_date":    order_date,
        "ship_date":     ship_date,
        "delivery_date": delivery_date,
        "quantity":      quantity,
        "unit_price":    unit_price,
        "discount_pct":  discount_pct,
        "total_amount":  total,
        "order_status":  random.choices(statuses, weights=weights)[0],
        "shipping_cost": round(random.uniform(5, 150), 2),
    })

fact_orders = pd.DataFrame(orders)

# Add ~500 duplicate rows (same data = sneaky duplicates to catch in ETL)
dupes = fact_orders.sample(500, random_state=1).copy()
fact_orders = pd.concat([fact_orders, dupes], ignore_index=True)
# pd.concat = stack two tables on top of each other

fact_orders.to_csv("data/raw/fact_orders.csv", index=False)

# Warehouses — hardcoded, only 10
warehouse_data = [
    (1, "Chicago Central",  "Chicago",     "IL", "Midwest",   50000),
    (2, "LA West",          "Los Angeles", "CA", "West",      45000),
    (3, "Dallas South",     "Dallas",      "TX", "South",     40000),
    (4, "New York East",    "New York",    "NY", "Northeast", 35000),
    (5, "Seattle Pacific",  "Seattle",     "WA", "West",      30000),
    (6, "Miami Gulf",       "Miami",       "FL", "South",     28000),
    (7, "Denver Mountain",  "Denver",      "CO", "Midwest",   25000),
    (8, "Atlanta Hub",      "Atlanta",     "GA", "South",     32000),
    (9, "Boston Harbor",    "Boston",      "MA", "Northeast", 22000),
    (10,"Phoenix Desert",   "Phoenix",     "AZ", "West",      27000),
]
dim_warehouses = pd.DataFrame(warehouse_data,
    columns=["warehouse_id","warehouse_name","city","state","region","capacity"])
dim_warehouses.to_csv("data/raw/dim_warehouses.csv", index=False)

# Inventory — one row per product per warehouse = 500 × 10 = 5,000 rows
inventory = []
inv_id = 1
for p in range(1, 501):
    for w in range(1, 11):
        qty = random.randint(0, 500)
        inventory.append({
            "inventory_id":      inv_id,
            "product_id":        p,
            "warehouse_id":      w,
            "snapshot_date":     datetime.today().date(),
            "quantity_on_hand":  qty if random.random() > 0.02 else None,
            "quantity_reserved": random.randint(0, min(qty, 100)) if qty > 0 else 0,
            "reorder_point":     random.randint(20, 100),
        })
        inv_id += 1

pd.DataFrame(inventory).to_csv("data/raw/fact_inventory.csv", index=False)

print("Done! Files saved to data/raw/")