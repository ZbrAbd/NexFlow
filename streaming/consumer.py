# streaming/consumer.py
# The Consumer reads orders from Kafka and prints them
# Think of it as a warehouse system receiving and processing each order

import json
from kafka import KafkaConsumer

# ── Connect to Kafka ─────────────────────────────────────────────────
consumer = KafkaConsumer(
    'orders',                            # topic to listen to
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',        # read from beginning if no offset saved
    # ↑ 'earliest' = read all messages from start
    # ↑ 'latest'   = only read new messages from now
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    # ↑ converts bytes → JSON string → Python dict (opposite of producer)
)

print("Consumer started. Waiting for orders...")
print("-" * 50)

count = 0
for message in consumer:
    order = message.value  # the actual order dict
    count += 1

    print(f"Order #{count} received:")
    print(f"  Customer: {order['customer_id']} | Product: {order['product_id']}")
    print(f"  Quantity: {order['quantity']} | Total: ${order['total_amount']}")
    print(f"  Status: {order['order_status']}")
    print("-" * 50)

    if count >= 20:  # stop after 20 orders so terminal doesn't flood
        print("Received 20 orders. Stopping consumer.")
        break