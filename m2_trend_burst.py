"""
m2_trend_burst.py - the 'sudden trend' for the live demo: a burst of clicks and add-to-carts for one product.

    python m2_trend_burst.py --product "Ice Cream"
    python m2_trend_burst.py --product-id 491314 --events 400 --seconds 20
"""
import argparse
import json
import random
import sys
import time
import uuid

import config
from common import make_event
from simulator import World

ap = argparse.ArgumentParser()
ap.add_argument("--product", default="Ice Cream", help="product name (the first matching product is used)")
ap.add_argument("--product-id", type=int, default=None)
ap.add_argument("--events", type=int, default=300)
ap.add_argument("--seconds", type=float, default=20)
ap.add_argument("--dry-run", action="store_true")
args = ap.parse_args()

w = World()
if args.product_id:
    p = w.products.get(args.product_id) or sys.exit("Unknown product id")
else:
    matches = sorted(w.by_name.get(args.product, []), key=lambda x: x["product_id"])
    p = matches[0] if matches else sys.exit(f"No product named '{args.product}'")
print(f"Burst for: {p['name']} (product_id {p['product_id']}, {p['category']}), "
      f"{args.events} events over {args.seconds:.0f}s")

producer = None
if not args.dry_run:
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=[config.KAFKA_BROKER],
                             key_serializer=lambda k: str(k).encode(),
                             value_serializer=lambda v: json.dumps(v).encode())

rng = random.Random()
gap = args.seconds / args.events
sent = 0
for i in range(args.events):
    user = rng.choice(w.user_ids)
    sid = "s_burst_" + uuid.uuid4().hex[:6]
    now = time.time()
    if rng.random() < 0.6:
        ev = make_event("product_click", user, sid, now,
                        {"product_id": p["product_id"], "category": p["category"], "source": "home"})
    else:
        ev = make_event("add_to_cart", user, sid, now,
                        {"product_id": p["product_id"], "category": p["category"], "qty": 1, "price": p["price"]})
    if producer:
        producer.send(config.TOPIC_EVENTS, key=user, value=ev)
    sent += 1
    if not args.dry_run:
        time.sleep(gap)
if producer:
    producer.flush()
print(f"Burst finished: {sent} events sent.")
