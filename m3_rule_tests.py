"""
m3_rule_tests.py - checks that each recommendation rule behaves correctly, using hand-made carts.
No Kafka or MongoDB needed.   Run:  python m3_rule_tests.py
"""
import time
import config
from common import ab_group, make_event
from recommender import Recommender, load_base_from_csv, pair_counts_from_simulation
from simulator import Simulator

sim = Simulator(seed=7)
products, user_cats, ratings = load_base_from_csv(sim)
pairs = pair_counts_from_simulation(sim)
R = lambda **kw: Recommender(products, user_cats, ratings, pairs, **kw)

def user_in(group, start=1000):
    u = start
    while ab_group(u) != group:
        u += 1
    return u

def trigger(uid, et="add_to_cart"):
    return make_event(et, uid, "s_test", time.time(), {})

def sku(name):
    return sorted(p["product_id"] for p in products if p["name"] == name)[0]

results = []
def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + (f"   [{detail}]" if detail else ""))

# 1. cart complement: Bread in the cart -> a partner product with reason "Frequently bought with Bread"
rec = R(); u = user_in("treatment")
rec.live.carts[u] = {sku("Bread"): 1}
out = rec.recommend(trigger(u))
cc = [r for r in out["recommendations"] if r["rule"] == "cart_complement"]
check("Rule 1 cart complement: Bread in cart -> partner item",
      bool(cc) and cc[0]["name"] in {"Butter", "Eggs", "Cheese", "Milk", "Cereal"} and "Bread" in cc[0]["reason"],
      f"{cc[0]['name']} | {cc[0]['reason']}" if cc else "no recommendation")

# 2. trending: a burst of adds for Ice Cream puts it on top for a fresh user
rec = R(trend_slide=0); u = user_in("treatment", 5000)
target = sku("Ice Cream"); now = time.time()
for i in range(200):
    rec.process(make_event("add_to_cart", 9000 + i, "s_b", now, {"product_id": target, "category": "Instant & Frozen Food", "qty": 1, "price": 1}))
out = rec.recommend(trigger(u, "app_open"))
tr = [r for r in out["recommendations"] if r["rule"] == "trending"]
check("Rule 2 trending: burst of adds makes the item trend",
      bool(tr) and tr[0]["product_id"] == target and "Trending" in tr[0]["reason"],
      tr[0]["reason"] if tr else "no trending item")

# 3. free delivery: a cart Rs ~200 short of the threshold gets an add-on that closes the gap
rec = R(); u = user_in("treatment", 9000)
no_offer = [p for p in products if config.offer_for(p["category"]) is None]
base = min(no_offer, key=lambda p: abs(p["price"] - (config.FREE_DELIVERY_THRESHOLD - 0.6 * config.FREE_DELIVERY_MAX_GAP)))
rec.live.carts[u] = {base["product_id"]: 1}
gap = config.FREE_DELIVERY_THRESHOLD - base["price"]
out = rec.recommend(trigger(u))
of = [r for r in out["recommendations"] if r["rule"] == "offer" and "free delivery" in r["reason"]]
eff = lambda x: x["price"] * (1 - x["offer_pct"] / 100)
check("Rule 3 free delivery: suggested item closes the gap",
      bool(of) and eff(of[0]) >= gap and abs(out["cart_value"] - base["price"]) < 0.01,
      f"cart Rs {base['price']:.0f}, gap Rs {gap:.0f}, suggested Rs {eff(of[0]):.0f}" if of else "none")
# 3b. a cart item in a promotion category counts at its discounted price
promo = next(p for p in products if config.offer_for(p["category"]))
rec.live.carts[u] = {promo["product_id"]: 1}
out2 = rec.recommend(trigger(u))
exp = promo["price"] * (1 - config.offer_for(promo["category"])["discount_pct"] / 100)
check("Cart value uses the promotion price", abs(out2["cart_value"] - exp) < 0.01, f"Rs {out2['cart_value']:.2f} vs list Rs {promo['price']:.2f}")

# 4. rating rule: a low-rated candidate is replaced; a well-rated or thinly-rated one is kept
rec = R(); view = rec.live
low = next(p for p, (c, s) in ratings.items() if c >= config.RATING_MIN_COUNT and s / c < config.RATING_LOW_CUTOFF)
res = rec._apply_rating_rule(view, low, set())
alt_ok = res is not None and res[1] and res[0] != low and rec._eligible_avg(view, res[0]) >= config.RATING_LOW_CUTOFF
check("Rule 4 rating: low-rated candidate is replaced by a better-rated one", alt_ok,
      f"avg {ratings[low][1]/ratings[low][0]:.2f} replaced by avg {rec._eligible_avg(view, res[0]):.2f}" if alt_ok else str(res))
thin = next(p for p, (c, s) in ratings.items() if c < config.RATING_MIN_COUNT)
res = rec._apply_rating_rule(view, thin, set())
check("Rule 4 rating: product with too few ratings is left alone", res == (thin, False))

# 5. live vs stale: control users do not see the cart change; treatment users do
rec = R(); tu, cu = user_in("treatment", 20000), user_in("control", 20000)
for u in (tu, cu):
    rec.process(make_event("add_to_cart", u, "s_t", time.time(), {"product_id": sku("Bread"), "category": "Dairy & Breakfast", "qty": 1, "price": 1}))
rt, rc = rec.recommend(trigger(tu)), rec.recommend(trigger(cu))
check("Live vs stale: treatment sees its new cart, control sees the stale (empty) cart",
      rt["view"] == "live" and len(rt["cart"]) == 1 and rc["view"] == "stale" and len(rc["cart"]) == 0)
rec.refresh_stale()
rc2 = rec.recommend(trigger(cu))
check("Live vs stale: after the snapshot refresh the control group catches up", len(rc2["cart"]) == 1)

# 6. variety and limits
check("At most 3 recommendations, none already in the cart",
      all(len(o["recommendations"]) <= config.TOP_N_RECS for o in (rt, rc2)) and
      all(r["product_id"] not in {c["product_id"] for c in o["cart"]} for o in (rt, rc2) for r in o["recommendations"]))

# 7. fix checks on a realistic stream: no repeated product name, no category offer for an empty cart, at most one offer
sim2 = Simulator(seed=11)
rec = R(trend_slide=0); recs = []
def feed(ev):
    rec.process(ev)
    if ev["event_type"] in ("app_open", "add_to_cart", "remove_from_cart"):
        recs.append(rec.recommend(ev))
sim2.run(feed, rate=3000, speed=100, duration=6)
dup = sum(1 for r in recs if len({x["name"] for x in r["recommendations"]} | {c["name"] for c in r["cart"]})
          < len(r["recommendations"]) + len({c["name"] for c in r["cart"]}))
off_empty = sum(1 for r in recs if not r["cart"] for x in r["recommendations"] if x["rule"] == "offer")
multi_off = sum(1 for r in recs if sum(x["rule"] == "offer" for x in r["recommendations"]) > 1)
check(f"No repeated product name or cart item name ({len(recs)} recommendations checked)", dup == 0, f"{dup} repeats")
check("No category offer for an empty cart", off_empty == 0, f"{off_empty} found")
check("At most one offer per recommendation", multi_off == 0, f"{multi_off} found")

# 8. a new visit starts with an empty cart; the trending burst then shows up for that customer
import config as _c
rating_ok = lambda p: ratings.get(p["product_id"], (0, 0))[0] >= _c.RATING_MIN_COUNT and ratings[p["product_id"]][1] / ratings[p["product_id"]][0] >= _c.RATING_LOW_CUTOFF
ice = sorted((p for p in products if p["name"] == "Ice Cream" and rating_ok(p)), key=lambda p: p["product_id"])[0]
rec = R(trend_slide=0); u = user_in("treatment", 40000); now = time.time()
for i in range(200):
    rec.process(make_event("add_to_cart", 70000 + i, "s_b", now, {"product_id": ice["product_id"], "category": ice["category"], "qty": 1, "price": 1}))
rec.process(make_event("add_to_cart", u, "s_old", now - 600, {"product_id": ice["product_id"], "category": ice["category"], "qty": 1, "price": ice["price"]}))
before = rec.recommend(trigger(u, "app_open"))
check("An item left in an old cart blocks its own recommendation (the bug)", all(x["product_id"] != ice["product_id"] for x in before["recommendations"]))
rec.process(make_event("app_open", u, "s_new", now, {"device": "web", "city": ""}))
after = rec.recommend(trigger(u, "app_open"))
check("A new visit starts with an empty cart and the trending item appears", after["cart"] == [] and any(x["product_id"] == ice["product_id"] and x["rule"] == "trending" for x in after["recommendations"]),
      ", ".join(x["name"] for x in after["recommendations"]))
rec.process(make_event("add_to_cart", u, "s_new", now + 5, {"product_id": ice["product_id"], "category": ice["category"], "qty": 1, "price": ice["price"]}))
rec.process(make_event("app_open", u, "s_late", now + 1, {"device": "web", "city": ""}))      # a LATE older app_open must not wipe a cart in progress
check("A late-arriving older app_open does not wipe a cart in progress", len(rec.live.carts[u]) == 1)

# 9. a stale recommendation says how old its snapshot is; a live one says 0
clk = {"t": 1_800_000_000.0}
rec = R(now_fn=lambda: clk["t"], stale_refresh=300); cu2 = user_in("control", 60000); tu2 = user_in("treatment", 60000)
a0 = rec.recommend(trigger(cu2, "app_open"))["snapshot_age_sec"]
clk["t"] += 120; a1 = rec.recommend(trigger(cu2, "app_open"))["snapshot_age_sec"]; live_age = rec.recommend(trigger(tu2, "app_open"))["snapshot_age_sec"]
clk["t"] += 200; a2 = rec.recommend(trigger(cu2, "app_open"))["snapshot_age_sec"]
check("Stale recommendations report the snapshot age (0, 120, then refreshed after 300 s) and live ones 0", (a0, a1, a2, live_age) == (0, 120, 0, 0), str((a0, a1, a2, live_age)))

print(f"\n{sum(results)} of {len(results)} checks passed")
