#!/usr/bin/env python3

import json
import random
from datetime import datetime, timedelta

def generate_orders(count=10000):
    statuses = ["pending", "processing", "paid", "shipped", "delivered", "cancelled"]
    customers = [f"CUST-{i:03d}" for i in range(1, 101)]

    base_date = datetime.now() - timedelta(days=7)

    for i in range(1, count + 1):
        order_id = f"ORD-2025-{i:06d}"
        customer_id = random.choice(customers)
        status = random.choice(statuses)
        amount_cents = random.randint(1000, 100000)  # $10 to $1000

        # Add some time progression
        base_date += timedelta(seconds=random.randint(1, 60))

        order = {
            "id": order_id,
            "customer_id": customer_id,
            "status": status,
            "amount_cents": amount_cents,
            "created_at": base_date.isoformat(),
            "items": random.randint(1, 10)
        }

        print(json.dumps(order))

if __name__ == "__main__":
    generate_orders()