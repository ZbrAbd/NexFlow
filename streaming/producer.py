# streaming/producer.py
# The Producer generates fake orders and sends them to Kafka one by one
# Think of it as a cash register sending every sale to a central system in real time

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ── Connect to Kafka ─────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # where Kafka is running
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
    # ↑ converts Python dict → JSON string → bytes
    # Kafka only understands bytes, not Python dicts
)

# ── Settings ─────────────────────────────────────────────────────────
TOPIC = 'orders'         # Kafka topic name — like a folder for our messages
TOTAL_ORDERS = 1000      # how many orders to send (keep small for testing)

statuses = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
weights  = [0.65, 0.15, 0.10, 0.06, 0.04]

# ── Generate and Send Orders ─────────────────────────────────────────
print(f"Sending {TOTAL_ORDERS} orders to Kafka topic '{TOPIC}'...")

for i in range(1, TOTAL_ORDERS + 1):
    order_date    = datetime.today()
    ship_date     = order_date + timedelta(days=random.randint(1, 5))
    delivery_date = ship_date  + timedelta(days=random.randint(1, 7))

    quantity     = random.randint(1, 50)
    unit_price   = round(random.uniform(10, 1000), 2)
    discount_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
    total        = round(unit_price * quantity * (1 - discount_pct), 2)

    order = {
        "order_line_id": i,
        "customer_id":   random.randint(1, 5000),
        "product_id":    random.randint(1, 500),
        "warehouse_id":  random.randint(1, 10),
        "order_date":    order_date.strftime("%Y-%m-%d"),
        "ship_date":     ship_date.strftime("%Y-%m-%d"),
        "delivery_date": delivery_date.strftime("%Y-%m-%d"),
        "quantity":      quantity,
        "unit_price":    unit_price,
        "discount_pct":  discount_pct,
        "total_amount":  total,
        "order_status":  random.choices(statuses, weights=weights)[0],
        "shipping_cost": round(random.uniform(5, 150), 2),
    }

    # Send to Kafka
    producer.send(TOPIC, value=order)

    # Print progress every 100 orders
    if i % 100 == 0:
        print(f"  Sent {i} orders...")

    time.sleep(0.01)  # small delay so we can see streaming in action

producer.flush()  # make sure all messages are sent before exiting
print(f"Done! {TOTAL_ORDERS} orders sent to Kafka.")