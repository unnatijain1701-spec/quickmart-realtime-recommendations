"""
make_realistic_prices.py - writes data/products_priced.csv: the supplied products with realistic prices.

Each product gets a price inside the band for its name (config.PRICE_BANDS), the same every time you run it, and an MRP
(old price) with a 0 to 30% discount.  Run once, then reload MongoDB:   python m1_load_data.py
"""
import random

import pandas as pd

import config

src = pd.read_csv(config.DATA_DIR / "blinkit_products.csv")
missing = sorted(set(src.product_name) - set(config.PRICE_BANDS))
if missing:
    raise SystemExit(f"No price band for: {missing}")


def price_for(pid, name):
    lo, hi = config.PRICE_BANDS[name]
    p = lo * (hi / lo) ** random.Random(f"price-{pid}").random()      # log-uniform: many cheap packs, a few big ones
    return float(round(p)) if p < 100 else float(5 * round(p / 5))


def mrp_for(pid, price):
    d = random.Random(f"mrp-{pid}").choice([0, 0, 5, 10, 10, 15, 20, 25, 30]) / 100
    return float(round(price / (1 - d)))


src["price"] = [price_for(r.product_id, r.product_name) for r in src.itertuples()]
src["mrp"] = [mrp_for(r.product_id, r.price) for r in src.itertuples()]
out = config.DATA_DIR / "products_priced.csv"
src.to_csv(out, index=False)
print(f"wrote {out}  ({len(src)} products)")
s = src.groupby("product_name").price.agg(["min", "median", "max"]).round(0).astype(int)
print("\nprice per product name (Rs): min / median / max")
print(s.to_string())
print(f"\nall products: median Rs {src.price.median():.0f}, 25th percentile Rs {src.price.quantile(.25):.0f}, 75th percentile Rs {src.price.quantile(.75):.0f}")
