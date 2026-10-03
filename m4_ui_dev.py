"""
m4_ui_dev.py - the customer screen WITHOUT Kafka and WITHOUT MongoDB, for working on the page's look.

Same address, same API and same live recommendations as m4_app.py (the real recommender runs in the same
process), but nothing is sent to Kafka and the dashboard does not move.  Use it while redesigning web/index.html.

    python m4_ui_dev.py          then open  http://127.0.0.1:8000/?group=treatment
"""
import pandas as pd
import uvicorn

import config
from common import ab_group
from m4_app import create_app
from recommender import TRIGGERS, Recommender, load_base_from_csv, pair_counts_from_simulation
from simulator import Simulator


class LocalBus:
    """Stands in for Kafka: every event goes straight to the recommender, recommendations come straight back."""
    def __init__(self, rec):
        self.rec, self.cb = rec, None

    def send_event(self, ev):
        self.rec.process(ev)
        if ev["event_type"] in TRIGGERS and self.cb:
            self.cb(self.rec.recommend(ev))

    def start_recs_listener(self, on_rec):
        self.cb = on_rec

    def close(self):
        pass


def catalogue_from_csv():
    d = config.DATA_DIR
    products = pd.read_csv(config.PRODUCTS_CSV)
    items = pd.read_csv(d / "blinkit_order_items.csv")
    fb = pd.read_csv(d / "blinkit_customer_feedback.csv").merge(items[["order_id", "product_id"]], on="order_id")
    g = fb.groupby("product_id").rating.agg(["count", "sum"])
    out = []
    for r in products.itertuples():
        c = int(g["count"].get(r.product_id, 0))
        off = config.offer_for(r.category)
        out.append({"product_id": int(r.product_id), "name": r.product_name, "brand": r.brand, "category": r.category,
                    "price": float(r.price), "mrp": float(r.mrp), "offer_pct": off["discount_pct"] if off else 0, "offer_name": off["name"] if off else "",
                    "rating": round(float(g["sum"].get(r.product_id, 0)) / c, 1) if c >= config.RATING_MIN_COUNT else None, "rating_count": c})
    out.sort(key=lambda p: (p["category"], p["name"], p["product_id"]))
    return out


def demo_users():
    d = config.DATA_DIR
    cust = pd.read_csv(d / "blinkit_customers.csv")
    ordered = set(pd.read_csv(d / "blinkit_orders.csv").customer_id)
    pick = {}
    for uid in sorted(int(x) for x in cust.customer_id if x not in ordered):
        pick.setdefault(ab_group(uid), uid)
        if len(pick) == 2:
            break
    return pick


def build():
    sim = Simulator(seed=config.RANDOM_SEED)
    rec = Recommender(*load_base_from_csv(sim), pair_counts_from_simulation(Simulator(seed=config.RANDOM_SEED)))
    return create_app(LocalBus(rec), catalogue_from_csv(), demo_users())


if __name__ == "__main__":
    import sys
    share = "--share" in sys.argv
    print(f"UI dev server (no Kafka, no MongoDB): http://{config.lan_ip() if share else config.WEB_HOST}:{config.WEB_PORT}/?group=treatment")
    uvicorn.run(build(), host="0.0.0.0" if share else config.WEB_HOST, port=config.WEB_PORT, log_level="warning")
