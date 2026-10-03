"""
simulator.py - generates customer behaviour events for the quick-commerce app.

Uses the supplied data (products, customers with order history, ratings) as the base and
simulates the rest: app opens, impressions, clicks, cart actions, orders, ratings, and
late-arriving events (phone offline).  All behaviour numbers live in config.py.
"""
import heapq
import random
import time
import uuid
from collections import defaultdict

import pandas as pd
import config
from common import make_event, iso


class World:
    """Static data the simulator draws from (read from the CSV files)."""

    def __init__(self):
        d = config.DATA_DIR
        products = pd.read_csv(config.PRODUCTS_CSV)
        orders = pd.read_csv(d / "blinkit_orders.csv")
        items = pd.read_csv(d / "blinkit_order_items.csv")
        feedback = pd.read_csv(d / "blinkit_customer_feedback.csv")
        customers = pd.read_csv(d / "blinkit_customers.csv")

        self.products = {int(r.product_id): {
            "product_id": int(r.product_id), "name": r.product_name,
            "category": r.category, "price": float(r.price)} for r in products.itertuples()}
        self.by_name = defaultdict(list)
        self.by_category = defaultdict(list)
        for p in self.products.values():
            self.by_name[p["name"]].append(p)
            self.by_category[p["category"]].append(p)
        self.all_products = list(self.products.values())

        # users with order history, and the categories they bought from before
        hist = orders.merge(items, on="order_id")
        hist["category"] = hist.product_id.map(lambda x: self.products[int(x)]["category"])
        self.user_cats = {int(u): sorted(set(g.category)) for u, g in hist.groupby("customer_id")}
        self.user_ids = sorted(self.user_cats)
        self.user_area = {int(r.customer_id): r.area for r in customers.itertuples()}

        # historical ratings per product (so live rating events look like the real ones)
        fb = feedback.merge(items[["order_id", "product_id"]], on="order_id")
        self.ratings = {int(p): g.rating.astype(int).tolist() for p, g in fb.groupby("product_id")}
        self.all_ratings = fb.rating.astype(int).tolist()

        # affinity maps (both directions)
        self.name_partners = defaultdict(set)
        for a, b in config.AFFINITY_PRODUCT_PAIRS:
            self.name_partners[a].add(b); self.name_partners[b].add(a)
        self.cat_partners = defaultdict(set)
        for a, b in config.AFFINITY_CATEGORY_PAIRS:
            self.cat_partners[a].add(b); self.cat_partners[b].add(a)


class Simulator:
    def __init__(self, seed=None):
        self.rng = random.Random(config.RANDOM_SEED if seed is None else seed)
        self.world = World()
        self.mean_events = None
        self.mean_length = 30.0

    # ------------------------------------------------------------ helpers
    def _pick_product(self, user):
        w, rng = self.world, self.rng
        cats = w.user_cats.get(user)
        if cats and rng.random() < config.PREFERRED_CATEGORY_P:
            return rng.choice(w.by_category[rng.choice(cats)])
        return rng.choice(w.all_products)

    def _pick_partner(self, product):
        w, rng = self.world, self.rng
        names = sorted(w.name_partners.get(product["name"], []))
        if names:
            return rng.choice(w.by_name[rng.choice(names)])
        cats = sorted(w.cat_partners.get(product["category"], []))
        if cats:
            return rng.choice(w.by_category[rng.choice(cats)])
        return None

    def _rating_for(self, pid):
        r = self.world.ratings.get(pid) or self.world.all_ratings
        return self.rng.choice(r)

    # ------------------------------------------------------------ one session
    def build_session(self, t0):
        """Returns a list of (event_ts, event_type, user_id, session_id, payload) for one customer session."""
        rng, w = self.rng, self.world
        user = rng.choice(w.user_ids)
        sid = "s_" + uuid.uuid4().hex[:8]
        out, cart = [], {}
        t = t0

        def add(ts, et, payload):
            out.append((ts, et, user, sid, payload))

        add(t, "app_open", {"device": rng.choice(["android", "ios"]), "city": w.user_area.get(user, "")})

        def cart_add(p, ts):
            qty = rng.choice([1, 1, 1, 2])
            cart[p["product_id"]] = cart.get(p["product_id"], 0) + qty
            add(ts, "add_to_cart", {"product_id": p["product_id"], "category": p["category"],
                                    "qty": qty, "price": p["price"]})

        pos = 0
        for _ in range(rng.randint(*config.SESSION_VIEWS_RANGE)):
            t += rng.uniform(*config.THINK_TIME_RANGE_SEC)
            p = self._pick_product(user)
            pos += 1
            add(t, "product_impression", {"product_id": p["product_id"], "category": p["category"],
                                          "position": pos, "dwell_ms": rng.randint(400, 6000)})
            if rng.random() < config.ORGANIC_CLICK_P:
                t += rng.uniform(0.5, 2.0)
                add(t, "product_click", {"product_id": p["product_id"], "category": p["category"],
                                         "source": rng.choice(["search", "home", "category"])})
                if rng.random() < config.ORGANIC_CART_P:
                    t += rng.uniform(0.5, 2.0)
                    cart_add(p, t)
                    if rng.random() < config.AFFINITY_FOLLOW_P:
                        partner = self._pick_partner(p)
                        if partner:
                            t += rng.uniform(1.0, 3.0)
                            pos += 1
                            add(t, "product_impression", {"product_id": partner["product_id"], "category": partner["category"],
                                                          "position": pos, "dwell_ms": rng.randint(800, 5000)})
                            t += rng.uniform(0.5, 2.0)
                            add(t, "product_click", {"product_id": partner["product_id"], "category": partner["category"],
                                                     "source": "search"})
                            t += rng.uniform(0.5, 2.0)
                            cart_add(partner, t)

        if len(cart) > 1 and rng.random() < config.REMOVE_FROM_CART_P:
            pid = rng.choice(sorted(cart))
            t += rng.uniform(1.0, 4.0)
            p = w.products[pid]
            add(t, "remove_from_cart", {"product_id": pid, "category": p["category"],
                                        "qty": cart.pop(pid), "price": p["price"]})

        if cart and rng.random() < config.CART_TO_ORDER_P:
            t += rng.uniform(3.0, 10.0)
            lines = [{"product_id": pid, "qty": q, "price": w.products[pid]["price"]} for pid, q in cart.items()]
            oid = "o_" + uuid.uuid4().hex[:8]
            add(t, "order_placed", {"order_id": oid, "items": lines,
                                    "order_value": round(sum(l["qty"] * l["price"] for l in lines), 2),
                                    "offer_applied": None})
            if rng.random() < config.ORDER_RATING_P:
                pid = rng.choice(sorted(cart))
                rt = t + rng.uniform(*config.RATING_DELAY_RANGE_SEC)
                add(rt, "rating_submitted", {"order_id": oid, "product_id": pid,
                                             "rating": self._rating_for(pid), "rating_type": "product"})
        return out

    def build_session_seeded(self, t0, seed):
        """Same seed -> same session script (customer, products, organic clicks and carts)."""
        saved = self.rng
        self.rng = random.Random(seed)
        try:
            return self.build_session(t0)
        finally:
            self.rng = saved

    def estimate_mean_events(self, n=300):
        if self.mean_events is None:
            saved = self.rng.getstate()
            sessions = [self.build_session(0) for _ in range(n)]
            self.mean_events = sum(len(x) for x in sessions) / n
            self.mean_length = sum(max(e[0] for e in x if e[1] != "rating_submitted") for x in sessions) / n
            self.rng.setstate(saved)
        return self.mean_events

    # ------------------------------------------------------------ the live loop
    def run(self, publish, rate=20.0, duration=0, speed=1.0, stop=lambda: False, on_tick=None):
        """
        Emits events in real time.  rate = target events per (wall) second.
        publish(event_dict) is called for each event.  duration = wall seconds (0 = until stop()).
        Some events are held back (late) and published after their event_time + a delay.
        """
        rng = self.rng
        mean_gap = self.estimate_mean_events() * speed / rate     # sim seconds between session starts
        wall0 = time.time(); sim0 = wall0

        def sim_now():
            return sim0 + (time.time() - wall0) * speed

        def wait_until(ts):
            delay = (ts - sim_now()) / speed
            if delay > 0.002:
                time.sleep(delay)

        end = sim0 + duration * speed if duration else float("inf")
        heap, seq = [], 0

        def schedule(session):
            nonlocal seq
            for ts, et, user, sid, payload in session:
                late = rng.random() < config.LATE_FRACTION
                emit = ts + rng.uniform(config.LATE_DELAY_MIN_SEC, config.LATE_DELAY_MAX_SEC) if late else ts
                seq += 1
                heapq.heappush(heap, (emit, seq, make_event(et, user, sid, ts, payload, late)))

        # warm-up: sessions already in progress at start, so the rate is steady from the first second
        for _ in range(int(self.mean_length / mean_gap)):
            start = sim0 - rng.uniform(0, self.mean_length)
            schedule([e for e in self.build_session(start) if e[0] >= sim0])
        next_session = sim0
        last_tick = time.time()
        while not stop():
            top = heap[0][0] if heap else float("inf")
            if next_session <= top:
                if next_session >= end:
                    break
                wait_until(next_session)
                schedule(self.build_session(next_session))
                next_session += rng.expovariate(1.0 / mean_gap)
            else:
                if top >= end and next_session >= end:
                    break
                wait_until(top)
                publish(heapq.heappop(heap)[2])
            if on_tick and time.time() - last_tick >= 5:
                last_tick = time.time(); on_tick()
        return len(heap)   # events still held back when we stopped
