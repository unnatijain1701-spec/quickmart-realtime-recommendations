"""
m1_load_data.py  -  Milestone 1: prepare the supplied data and load it into MongoDB.

Creates these collections (safe to re-run, it replaces them):
  products       one row per product (catalogue)
  users          one row per customer (no names, emails or phones, we do not need them)
  order_history  one row per past order, joined with its item
  rating_stats   per-product rating sum, count and average (from the feedback file)

Run:  python m1_load_data.py
"""
from datetime import datetime
import pandas as pd
import config


def _dt(value):
    return pd.to_datetime(value).to_pydatetime()


def build_products(df):
    return [{
        "_id": int(r.product_id), "product_id": int(r.product_id),
        "name": r.product_name, "category": r.category, "brand": r.brand,
        "price": float(r.price), "mrp": float(r.mrp),
        "margin_pct": float(r.margin_percentage), "shelf_life_days": int(r.shelf_life_days),
        "min_stock": int(r.min_stock_level), "max_stock": int(r.max_stock_level),
    } for r in df.itertuples()]


def build_users(customers, orders):
    n_orders = orders.groupby("customer_id").size()
    return [{
        "_id": int(r.customer_id), "user_id": int(r.customer_id),
        "area": r.area, "segment": r.customer_segment,
        "registered_on": _dt(r.registration_date),
        "order_count": int(n_orders.get(r.customer_id, 0)),
    } for r in customers.itertuples()]


def build_order_history(orders, items, products=None):
    m = orders.merge(items, on="order_id", how="inner")
    if products is not None:                       # use the catalogue price, so history agrees with the prices shown
        m["unit_price"] = m.product_id.map(dict(zip(products.product_id, products.price)))
    return [{
        "order_id": int(r.order_id), "user_id": int(r.customer_id), "product_id": int(r.product_id),
        "quantity": int(r.quantity), "unit_price": float(r.unit_price),
        "order_time": _dt(r.order_date), "delivery_status": r.delivery_status,
        "payment_method": r.payment_method,
    } for r in m.itertuples()]


def build_rating_stats(feedback, items, products):
    m = feedback.merge(items[["order_id", "product_id"]], on="order_id", how="inner")
    g = m.groupby("product_id").rating.agg(["count", "sum"])
    docs = []
    for pid in products.product_id:
        c = int(g["count"].get(pid, 0)); sm = int(g["sum"].get(pid, 0))
        docs.append({
            "_id": int(pid), "product_id": int(pid),
            "rating_count": c, "rating_sum": sm,
            "avg_rating": round(sm / c, 3) if c else None,
            "eligible": c >= config.RATING_MIN_COUNT,
        })
    return docs


def check_affinity(products):
    names = set(products.product_name); cats = set(products.category)
    for a, b in config.AFFINITY_PRODUCT_PAIRS:
        for n in (a, b):
            if n not in names:
                print(f"  WARNING: affinity product '{n}' is not in the catalogue")
    for a, b in config.AFFINITY_CATEGORY_PAIRS:
        for c in (a, b):
            if c not in cats:
                print(f"  WARNING: affinity category '{c}' is not in the catalogue")
    for o in config.OFFERS:
        if o["category"] not in cats:
            print(f"  WARNING: offer category '{o['category']}' is not in the catalogue")


def main(db=None):
    if db is None:
        from db import get_db
        db = get_db()
    d = config.DATA_DIR
    products = pd.read_csv(config.PRODUCTS_CSV)
    customers = pd.read_csv(d / "blinkit_customers.csv")
    orders = pd.read_csv(d / "blinkit_orders.csv")
    items = pd.read_csv(d / "blinkit_order_items.csv")
    feedback = pd.read_csv(d / "blinkit_customer_feedback.csv")

    check_affinity(products)
    plan = {
        "products": build_products(products),
        "users": build_users(customers, orders),
        "order_history": build_order_history(orders, items, products),
        "rating_stats": build_rating_stats(feedback, items, products),
    }
    for name, docs in plan.items():
        db[name].drop()
        db[name].insert_many(docs)
        print(f"  loaded {len(docs):>5} documents into '{name}'")
    db["order_history"].create_index("user_id")
    db["order_history"].create_index("product_id")
    db["products"].create_index("category")

    # ---- sanity check: these numbers should match what the plan says ----
    rs = list(db["rating_stats"].find())
    eligible = [r for r in rs if r["eligible"]]
    low = [r for r in eligible if r["avg_rating"] < config.RATING_LOW_CUTOFF]
    print("\nSANITY CHECK")
    print(f"  products                       : {db['products'].count_documents({})}   (expect 268)")
    print(f"  users                          : {db['users'].count_documents({})}   (expect 2500)")
    print(f"  users with order history       : {db['users'].count_documents({'order_count': {'$gt': 0}})}   (expect 2172)")
    print(f"  order_history rows             : {db['order_history'].count_documents({})}   (expect 5000)")
    print(f"  products with >= {config.RATING_MIN_COUNT} ratings      : {len(eligible)}   (expect 224)")
    print(f"  of those, average < {config.RATING_LOW_CUTOFF}          : {len(low)}   (expect 20)")
    print("\nM1 complete." )


if __name__ == "__main__":
    main()
