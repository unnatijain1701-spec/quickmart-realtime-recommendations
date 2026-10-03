"""
recommender.py - the recommendation brain (M3).

Pure Python: no Kafka and no MongoDB inside, so it can be tested offline and reused in the
experiment (M6).  It keeps state (carts, trending window, ratings, pair counts) from the
event stream and answers: "what should we recommend to this user right now?"

Two views of the state exist:
  live  - updated by every event               -> used for the TREATMENT group
  stale - a frozen copy refreshed hourly       -> used for the CONTROL group
"""
import copy
import time
import uuid
from collections import Counter, defaultdict, deque

import config
from common import ab_group, iso, parse_iso

TRIGGERS = ("app_open", "add_to_cart", "remove_from_cart")
RULE_ORDER = ("cart_complement", "offer", "trending")


class View:
    def __init__(self):
        self.carts = {}                    # user_id -> {product_id: qty}
        self.ratings = {}                  # product_id -> [count, sum]
        self.pairs = defaultdict(Counter)  # product name -> Counter of partner names
        self.ranking = []                  # trending: [(product_id, score)], best first
        self.ranking_at = 0.0


class Recommender:
    def __init__(self, products, user_cats, rating_stats, pair_counts,
                 trend_window=None, trend_slide=None, stale_refresh=None, now_fn=time.time):
        self.now_fn = now_fn
        self.trend_window = trend_window or config.TRENDING_WINDOW_SEC
        self.trend_slide = config.TRENDING_SLIDE_SEC if trend_slide is None else trend_slide
        self.stale_refresh = stale_refresh or config.STALE_REFRESH_SEC

        self.products = {p["product_id"]: p for p in products}
        self.all_products = list(self.products.values())
        self.by_name, self.by_category = defaultdict(list), defaultdict(list)
        for p in self.all_products:
            self.by_name[p["name"]].append(p["product_id"])
            self.by_category[p["category"]].append(p["product_id"])
        self.user_cats = {u: set(c) for u, c in user_cats.items()}

        self.live = View()
        self.live.ratings = {pid: list(v) for pid, v in rating_stats.items()}
        for a, partners in pair_counts.items():
            self.live.pairs[a] = Counter(partners)
        self.trend = defaultdict(deque)    # product_id -> deque[(event_ts, weight)]
        self.order_ts = {}                 # user_id -> time of last order (ignore older cart events)
        self.cart_ts = {}                  # user_id -> time of the last cart change
        self.stale = copy.deepcopy(self.live)
        self.stale_at = self.now_fn()

    # ------------------------------------------------------------ state updates
    def process(self, ev):
        """Update the live state from one event."""
        et, uid, pl = ev["event_type"], ev["user_id"], ev["payload"]
        ts = parse_iso(ev["event_time"]).timestamp()
        v = self.live
        if et == "app_open":
            if ts >= self.cart_ts.get(uid, 0):      # a new visit starts with an empty cart (abandoned carts must not linger)
                v.carts[uid] = {}
                self.cart_ts[uid] = ts
        elif et == "add_to_cart":
            self._trend_add(pl["product_id"], ts, config.TREND_WEIGHT_ADD)
            if ts >= self.order_ts.get(uid, 0):
                cart = v.carts.setdefault(uid, {})
                cart[pl["product_id"]] = cart.get(pl["product_id"], 0) + pl.get("qty", 1)
                self.cart_ts[uid] = max(ts, self.cart_ts.get(uid, 0))
        elif et == "remove_from_cart":
            if ts >= self.order_ts.get(uid, 0):
                v.carts.get(uid, {}).pop(pl["product_id"], None)
                self.cart_ts[uid] = max(ts, self.cart_ts.get(uid, 0))
        elif et == "product_click":
            self._trend_add(pl["product_id"], ts, config.TREND_WEIGHT_CLICK)
        elif et == "order_placed":
            items = [i for i in pl["items"] if i["product_id"] in self.products]
            self._count_pairs(v.pairs, {self.products[i["product_id"]]["name"] for i in items})
            self.user_cats.setdefault(uid, set()).update(self.products[i["product_id"]]["category"] for i in items)
            v.carts[uid] = {}
            self.order_ts[uid] = max(ts, self.order_ts.get(uid, 0))
        elif et == "rating_submitted":
            r = v.ratings.setdefault(pl["product_id"], [0, 0])
            r[0] += 1
            r[1] += pl["rating"]

    def _trend_add(self, pid, ts, w):
        self.trend[pid].append((ts, w))

    @staticmethod
    def _count_pairs(pairs, names):
        names = sorted(names)
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                pairs[a][b] += 1
                pairs[b][a] += 1

    def refresh_stale(self, now=None):
        now = now or self.now_fn()
        self._ranking(self.live, now, force=True)
        self.stale = copy.deepcopy(self.live)
        self.stale_at = now

    # ------------------------------------------------------------ helpers
    def _ranking(self, view, now, force=False):
        if view is self.stale:
            return view.ranking
        if not force and now - view.ranking_at < self.trend_slide:
            return view.ranking
        cutoff = now - self.trend_window
        scores = {}
        for pid, dq in self.trend.items():
            while dq and dq[0][0] < cutoff:
                dq.popleft()
            s = sum(w for ts, w in dq if ts >= cutoff)
            if s > 0:
                scores[pid] = s
        view.ranking = sorted(scores.items(), key=lambda x: -x[1])[:config.TREND_TOP_K]
        view.ranking_at = now
        return view.ranking

    @staticmethod
    def _eff(p):
        """price after the category promotion (applied at checkout)."""
        o = config.offer_for(p["category"])
        return p["price"] * (1 - o["discount_pct"] / 100) if o else p["price"]

    def _rating(self, view, pid):
        c, s = view.ratings.get(pid, (0, 0))
        return (s / c if c else None), c

    def _eligible_avg(self, view, pid):
        avg, c = self._rating(view, pid)
        return avg if (avg is not None and c >= config.RATING_MIN_COUNT) else None

    def _best_in(self, pids, view, exclude):
        """best-rated product among pids (neutral 3.0 when it has too few ratings)."""
        best, key = None, None
        for pid in pids:
            if pid in exclude:
                continue
            avg = self._eligible_avg(view, pid)
            k = (avg if avg is not None else 3.0, -self.products[pid]["price"])
            if key is None or k > key:
                best, key = pid, k
        return best

    def _best_alternative(self, view, category, exclude):
        """best-rated product in the category whose (reliable) average is at least the cutoff."""
        best, key = None, None
        for pid in self.by_category[category]:
            avg = self._eligible_avg(view, pid)
            if pid in exclude or avg is None or avg < config.RATING_LOW_CUTOFF:
                continue
            k = (avg, view.ratings[pid][0])
            if key is None or k > key:
                best, key = pid, k
        return best

    # ------------------------------------------------------------ the rules
    def _rule_cart_complement(self, view, cart):
        out = []
        for pid in reversed(list(cart)):                 # most recently added first
            name = self.products[pid]["name"]
            partners = view.pairs.get(name)
            if not partners:
                continue
            top = partners.most_common(4)
            mx = top[0][1]
            for pname, cnt in top:
                sku = self._best_in(self.by_name[pname], view, set(cart))
                if sku:
                    out.append((sku, "cart_complement", f"Frequently bought with {name}", cnt / mx))
        return sorted(out, key=lambda c: -c[3])

    def _rule_trending(self, view, cart, cats, now):
        ranking = self._ranking(view, now)
        if not ranking:
            return []
        w = self.trend_window
        label = f"{w/60:g} minutes" if w % 60 == 0 else f"{w:g} seconds"
        mx = ranking[0][1]
        out = []
        for pid, sc in ranking:
            if pid in cart or pid not in self.products:
                continue
            if cats and self.products[pid]["category"] not in cats:
                continue
            out.append((pid, "trending", f"Trending in the last {label}", sc / mx))
        return out

    def _rule_offer(self, view, cart, cart_value, cats):
        out = []
        gap = config.FREE_DELIVERY_THRESHOLD - cart_value
        if cart and 0 < gap <= config.FREE_DELIVERY_MAX_GAP:
            pool = [p for p in self.all_products
                    if self._eff(p) >= gap and p["product_id"] not in cart and (not cats or p["category"] in cats)]
            near = [p for p in pool if self._eff(p) <= gap + config.FREE_DELIVERY_MAX_OVERSHOOT]
            pool = sorted(near or pool, key=lambda p: self._eff(p))
            for p in pool[:3]:
                out.append((p["product_id"], "offer", f"Add Rs {gap:.0f} more for free delivery", 0.9))
        cart_cats = {self.products[p]["category"] for p in cart if p in self.products}
        for o in config.OFFERS:
            if o["category"] in cart_cats:                       # category offers only when the cart is not empty
                sku = self._best_in(self.by_category[o["category"]], view, set(cart))
                if sku:
                    out.append((sku, "offer", f"{o['name']}: {o['discount_pct']}% off {o['category']}", 0.7))
        return out

    def _apply_rating_rule(self, view, pid, used):
        """Rule 4. Returns (product_id, adjusted) or None when nothing suitable exists."""
        avg = self._eligible_avg(view, pid)
        if avg is not None and avg < config.RATING_LOW_CUTOFF:
            alt = self._best_alternative(view, self.products[pid]["category"], used)
            return (alt, True) if alt else None
        return pid, False

    # ------------------------------------------------------------ the recommendation
    def recommend(self, ev, view=None):
        """view: None = by experiment group; 'live' or 'stale' forces one (used by the offline experiment)."""
        uid = ev["user_id"]
        now = self.now_fn()
        if now - self.stale_at >= self.stale_refresh:
            self.refresh_stale(now)
        if view is None:
            group = ab_group(uid)
            v = self.live if group == "treatment" else self.stale
        else:
            v = self.live if view == "live" else self.stale
            group = "treatment" if view == "live" else "control"
        view = v
        cart = dict(view.carts.get(uid, {}))
        cart_value = sum(q * self._eff(self.products[p]) for p, q in cart.items() if p in self.products)
        cats = set(self.user_cats.get(uid, set())) | {self.products[p]["category"] for p in cart if p in self.products}

        queues = {
            "cart_complement": self._rule_cart_complement(view, cart),
            "offer": self._rule_offer(view, cart, cart_value, cats),
            "trending": self._rule_trending(view, cart, cats, now),
        }
        chosen, used = [], set(cart)
        used_names = {self.products[p]["name"] for p in cart if p in self.products}
        n_offers = [0]

        def take(pid, rl, reason, score):
            """apply the rating rule and the no-repeat rules; add the item if it passes."""
            blocked = used | {x for n in used_names for x in self.by_name[n]}   # same product name in another brand = a repeat
            res = self._apply_rating_rule(view, pid, blocked)
            if res is None:
                return False
            fpid, adjusted = res
            if fpid in blocked:
                return False
            used.add(fpid)
            used_names.add(self.products[fpid]["name"])
            n_offers[0] += 1 if rl == "offer" else 0
            p = self.products[fpid]
            avg, c = self._rating(view, fpid)
            off = config.offer_for(p["category"])
            chosen.append({
                "product_id": fpid, "name": p["name"], "brand": p.get("brand", ""),
                "category": p["category"], "price": p["price"],
                "rating": round(avg, 1) if (avg is not None and c >= config.RATING_MIN_COUNT) else None,
                "rating_count": c, "offer_pct": off["discount_pct"] if off else 0,
                "offer_name": off["name"] if off else "", "rule": rl,
                "reason": reason + (" (better-rated alternative)" if adjusted else ""),
                "rating_adjusted": adjusted, "score": round(score, 3)})
            return True

        while len(chosen) < config.TOP_N_RECS and any(queues.values()):
            progressed = False
            for rule in RULE_ORDER:
                q = queues[rule]
                while q:
                    pid, rl, reason, score = q.pop(0)
                    if rl == "offer" and n_offers[0] >= 1:          # at most one offer per recommendation
                        q.clear()
                        break
                    if take(pid, rl, reason, score):
                        progressed = True
                        break
                if len(chosen) >= config.TOP_N_RECS:
                    break
            if not progressed:
                break

        # nothing specific left to say: fall back to what is popular right now (any category), like a batch list would
        if len(chosen) < config.TOP_N_RECS:
            ranking = self._ranking(view, now)
            mx = ranking[0][1] if ranking else 1
            for pid, sc in ranking:
                if len(chosen) >= config.TOP_N_RECS:
                    break
                if pid in self.products:
                    take(pid, "trending", "Popular right now", 0.5 * sc / mx)

        return {
            "rec_id": "r_" + uuid.uuid4().hex[:10], "user_id": uid, "group": group,
            "view": "live" if view is self.live else "stale",
            "snapshot_age_sec": 0 if view is self.live else round(now - self.stale_at),
            "trigger": ev["event_type"], "trigger_event_id": ev["event_id"],
            "trigger_event_time": ev["event_time"], "created_at": iso(now),
            "cart": [{"product_id": p, "name": self.products[p]["name"], "qty": q,
                      "price": self.products[p]["price"]} for p, q in cart.items() if p in self.products],
            "cart_value": round(cart_value, 2),
            "recommendations": chosen,
        }


# ====================================================================== loading helpers
def pair_counts_from_simulation(sim, n=None):
    """Initialise 'frequently bought together' from simulated baskets (assumed affinity, stated in the project)."""
    n = n or config.PAIR_COUNT_SESSIONS
    pairs = defaultdict(Counter)
    prod = sim.world.products
    for _ in range(n):
        names = {prod[e[4]["product_id"]]["name"] for e in sim.build_session(0) if e[1] == "add_to_cart"}
        Recommender._count_pairs(pairs, names)
    return {a: dict(c) for a, c in pairs.items()}


def load_base_from_db(db):
    """Products, user categories and rating stats from the MongoDB collections loaded in M1."""
    products = [{"product_id": d["product_id"], "name": d["name"], "category": d["category"],
                 "price": d["price"], "brand": d.get("brand", "")} for d in db["products"].find()]
    cat = {p["product_id"]: p["category"] for p in products}
    user_cats = defaultdict(set)
    for d in db["order_history"].find({}, {"user_id": 1, "product_id": 1}):
        user_cats[d["user_id"]].add(cat[d["product_id"]])
    ratings = {d["product_id"]: (d["rating_count"], d["rating_sum"]) for d in db["rating_stats"].find()}
    return products, user_cats, ratings


def load_base_from_csv(sim):
    """Same data straight from the CSV files (used by offline tests)."""
    import pandas as pd
    w = sim.world
    products = [dict(p, brand="") for p in w.all_products]
    ratings = {pid: (len(r), sum(r)) for pid, r in w.ratings.items()}
    return products, w.user_cats, ratings
