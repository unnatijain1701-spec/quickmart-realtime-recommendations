"""
interactive.py - customers that REACT to recommendations.

One behaviour model, used by both the offline experiment (m6_experiment.py) and the live producer
(m6_live_producer.py).  A customer follows an organic session script (the same script in every
experiment arm); after each cart change or app open they glance at the recommendations they are
shown and may click and add an item.

THE ASSUMPTION (stated in the project):
    chance to click a shown item = BASE_CTR x (1 + LIFT x relevance)
    chance to add it after clicking = BASE_ADD x (1 + 0.5 x LIFT x relevance)
    relevance: 1.0 if the item is a partner of something in the customer's cart (0.5 if only its
               category is), +0.5 if it is in a category they usually buy, +0.25 if it is on promotion.
LIFT is the scenario knob: 0 (null), 1 (low), 2 (base), 3 (high).
A live recommender wins only if it shows MORE RELEVANT items, because it can see the cart.
"""
import random

import config

TRIGGERS = ("app_open", "add_to_cart", "remove_from_cart")


class ResponseModel:
    def __init__(self, world, lift, base_ctr=None, base_add=None):
        self.w, self.lift = world, lift
        self.base_ctr = config.BASE_CTR if base_ctr is None else base_ctr
        self.base_add = config.BASE_CLICK_TO_CART if base_add is None else base_add

    def relevance(self, item, cart_pids, user_cats):
        w = self.w
        names = {w.products[p]["name"] for p in cart_pids if p in w.products}
        cats = {w.products[p]["category"] for p in cart_pids if p in w.products}
        r = 0.0
        if any(item["name"] in w.name_partners.get(n, ()) for n in names):
            r = 1.0
        elif any(item["category"] in w.cat_partners.get(c, ()) for c in cats):
            r = 0.5
        if item["category"] in user_cats:
            r += 0.5
        if item.get("offer_pct"):
            r += 0.25
        return r

    def p_click(self, r):
        return min(0.9, self.base_ctr * (1 + self.lift * r))

    def p_add(self, r):
        return min(0.9, self.base_add * (1 + 0.5 * self.lift * r))


def session_gen(script, sidx, ctx):
    """
    script: organic events [(ts, event_type, user, session_id, payload)] sorted by time.
    ctx provides: world, model, seed, recs_enabled, emit(ts, et, user, sid, payload) -> event,
                  request_rec(event), get_rec(event) -> rec or None, finish(summary).
    A generator: it yields the time it wants to be woken at.
    """
    w, model, seed = ctx.world, ctx.model, ctx.seed
    user, sid = script[0][2], script[0][3]
    user_cats = w.user_cats.get(user, set())
    organic_cart = any(e[1] == "add_to_cart" for e in script)
    cart = {}
    st = {"ordered": False, "revenue": 0.0, "units": 0, "shown": 0, "clicks": 0, "adds": 0}
    last_ts = script[0][0]
    trig = 0

    def place_order(ts):
        lines = [{"product_id": p, "qty": q, "price": w.products[p]["price"]} for p, q in cart.items()]
        subtotal = sum(l["qty"] * l["price"] for l in lines)
        discount, names = 0.0, set()
        for l in lines:
            off = config.offer_for(w.products[l["product_id"]]["category"])
            if off:
                discount += l["qty"] * l["price"] * off["discount_pct"] / 100
                names.add(off["name"])
        ctx.emit(ts, "order_placed", user, sid, {
            "order_id": "o_" + sid[-8:], "items": lines, "subtotal": round(subtotal, 2),
            "discount_total": round(discount, 2), "order_value": round(subtotal - discount, 2),
            "offer_applied": sorted(names) or None})
        st.update(ordered=True, revenue=subtotal - discount, units=sum(l["qty"] for l in lines))
        cart.clear()

    for ts, et, _u, _s, payload in script:
        ts = max(ts, last_ts)
        yield ts
        if et == "order_placed":
            if cart:                                   # the order reflects the ACTUAL cart (organic + recommended items)
                place_order(ts)
            last_ts = ts
            continue
        ev = ctx.emit(ts, et, user, sid, payload)
        last_ts = ts
        if et == "add_to_cart":
            cart[payload["product_id"]] = cart.get(payload["product_id"], 0) + payload.get("qty", 1)
        elif et == "remove_from_cart":
            cart.pop(payload["product_id"], None)
        if et in TRIGGERS and ctx.recs_enabled:
            trig += 1
            ctx.request_rec(ev)
            look = ts + random.Random(f"{seed}-{sidx}-{trig}-look").uniform(0.25, 0.45)
            yield look
            rec = ctx.get_rec(ev)
            last_ts = max(last_ts, look)
            if rec and rec["recommendations"]:
                items = rec["recommendations"]
                for pos, it in enumerate(items, 1):
                    ctx.emit(look, "rec_shown", user, sid, {"rec_id": rec["rec_id"], "product_id": it["product_id"],
                                                            "rule": it["rule"], "reason": it["reason"], "position": pos,
                                                            "group": rec["group"]})
                    st["shown"] += 1
                t = look
                for it in items:
                    pid = it["product_id"]
                    if pid in cart:                    # they already have it
                        continue
                    rr = random.Random(f"{seed}-{sidx}-{trig}-{pid}")
                    r = model.relevance(it, cart, user_cats)
                    if rr.random() < model.p_click(r):
                        t += 0.1
                        info = {"rec_id": rec["rec_id"], "product_id": pid, "rule": it["rule"], "group": rec["group"]}
                        ctx.emit(t, "rec_clicked", user, sid, info)
                        st["clicks"] += 1
                        if rr.random() < model.p_add(r):
                            t += 0.1
                            ctx.emit(t, "rec_added_to_cart", user, sid, info)
                            ctx.emit(t + 0.01, "add_to_cart", user, sid, {"product_id": pid, "category": it["category"], "qty": 1,
                                                                          "price": it["price"], "source": "recommendation"})
                            cart[pid] = cart.get(pid, 0) + 1
                            st["adds"] += 1
                last_ts = max(last_ts, t)

    # a customer with no organic cart who was persuaded to add something may still check out
    if not st["ordered"] and not organic_cart and cart:
        if random.Random(f"{seed}-{sidx}-order").random() < config.CART_TO_ORDER_P:
            ts = last_ts + random.Random(f"{seed}-{sidx}-gap").uniform(3, 10)
            yield ts
            place_order(ts)
    ctx.finish(dict(sidx=sidx, **st))
