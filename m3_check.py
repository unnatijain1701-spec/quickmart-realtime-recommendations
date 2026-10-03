"""m3_check.py - quick health check of the recommendations and metrics stored in MongoDB."""
import pandas as pd
from db import get_db

db = get_db()
n = db["recommendations"].count_documents({})
if n == 0:
    raise SystemExit("No recommendations yet. Start m3_processor.py, then the producer, and wait a minute.")
docs = list(db["recommendations"].find({}, {"group": 1, "view": 1, "trigger": 1, "latency_ms": 1, "recommendations": 1}))
df = pd.DataFrame(docs)
print(f"recommendations stored : {n}")
print("\nby group / view:")
print(df.groupby(["group", "view"]).size().to_string())
print("\nby trigger:")
print(df.trigger.value_counts().to_string())
print(f"\nlatency (ms)           : median {df.latency_ms.median():.0f} | 95th pct {df.latency_ms.quantile(0.95):.0f} | max {df.latency_ms.max():.0f}   (target: mostly under 1000)")
rules = pd.Series([x["rule"] for d in docs for x in d["recommendations"]]).value_counts()
print("\nrecommendations shown, by rule:")
print(rules.to_string())
adj = sum(x["rating_adjusted"] for d in docs for x in d["recommendations"])
print(f"\nrating rule replaced a low-rated item {adj} times")
print("\nSAMPLE RECOMMENDATIONS (most recent):")
for d in db["recommendations"].find().sort("created_at", -1).limit(5):
    cart = ", ".join(f"{c['name']} x{c['qty']}" for c in d["cart"]) or "empty"
    print(f"  user {d['user_id']} [{d['group']}/{d['view']}] after {d['trigger']} | cart: {cart} (Rs {d['cart_value']})")
    for i, x in enumerate(d["recommendations"], 1):
        r = f"rating {x['rating']}" if x["rating"] else "no rating yet"
        print(f"     {i}. {x['name']} {x['brand']} Rs {x['price']:.0f} | {x['reason']} | {r}")
print("\nLATEST MINUTE METRICS:")
for m in db["metrics_minute"].find().sort("bucket_start", -1).limit(3):
    print(f"  {m['bucket_start']:%H:%M} | events {m['events']} | late {100*m['late_share']:.1f}% | recs {m['recs']} | "
          f"rec latency avg {m['rec_latency_avg_ms']:.0f} ms, p95 {m['rec_latency_p95_ms']:.0f} ms")
al = db["alerts"].count_documents({})
print(f"\nalerts raised: {al}")
