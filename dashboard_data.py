"""
dashboard_data.py - every query the dashboard needs, in one place (no charts here).
Queries run in parallel so one refresh takes about as long as the slowest query.
"""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import config

FUNNEL_STAGES = [("app_open", "App opened"), ("product_impression", "Viewed a product"),
                 ("product_click", "Clicked a product"), ("add_to_cart", "Added to cart"),
                 ("order_placed", "Placed an order")]
MINUTE_FMT = "%Y-%m-%dT%H:%M"


class DataLayer:
    def __init__(self, db, workers=6):
        self.db = db
        self.pool = ThreadPoolExecutor(max_workers=workers)
        self.category = {d["product_id"]: d["category"] for d in db["products"].find({}, {"product_id": 1, "category": 1})}
        sums = defaultdict(lambda: [0, 0])
        for d in db["rating_stats"].find():
            c = self.category.get(d["product_id"])
            sums[c][0] += d["rating_sum"]
            sums[c][1] += d["rating_count"]
        self.baseline = {c: s / n for c, (s, n) in sums.items() if n}      # historical average rating per category
        info = {p["product_id"]: p for p in db["products"].find({}, {"product_id": 1, "name": 1, "brand": 1, "category": 1})}
        stats = list(db["rating_stats"].find())
        reliable = [s for s in stats if s["rating_count"] >= config.RATING_MIN_COUNT]
        low = sorted((s for s in reliable if s["rating_sum"] / s["rating_count"] < config.RATING_LOW_CUTOFF), key=lambda s: s["rating_sum"] / s["rating_count"])
        self.quality = {"products": len(stats), "reliable": len(reliable), "below": len(low),
                        "low_list": [{"name": info[s["product_id"]]["name"], "brand": info[s["product_id"]].get("brand", ""),
                                      "category": info[s["product_id"]]["category"], "avg": s["rating_sum"] / s["rating_count"],
                                      "n": s["rating_count"]} for s in low[:10] if s["product_id"] in info]}

    def ensure_indexes(self):
        ev = self.db["events"]
        ev.create_index("processed_time")
        ev.create_index("event_time")
        self.db["metrics_minute"].create_index("bucket_start")

    # ------------------------------------------------------------ individual queries
    def events_per_sec(self, now, seconds=10):
        n = self.db["events"].count_documents({"processed_time": {"$gte": now - timedelta(seconds=seconds)}})
        return n / seconds

    def funnel(self, cutoff):
        types = [t for t, _ in FUNNEL_STAGES]
        rows = self.db["events"].aggregate([
            {"$match": {"event_time": {"$gte": cutoff}, "event_type": {"$in": types}}},
            {"$group": {"_id": "$session_id", "types": {"$addToSet": "$event_type"}}},
            {"$match": {"types": "app_open"}},                    # sessions that started inside the window
            {"$unwind": "$types"},
            {"$group": {"_id": "$types", "n": {"$sum": 1}}}])
        got = {r["_id"]: r["n"] for r in rows}
        return {t: got.get(t, 0) for t in types}

    def metrics(self, window_min):
        docs = list(self.db["metrics_minute"].find().sort("bucket_start", -1).limit(window_min + 1))
        return list(reversed(docs))

    def late_events(self, cutoff):
        base = {"device_offline": True, "processed_time": {"$gte": cutoff}}
        out = {}
        for key, field in (("by_event", "$event_time"), ("by_proc", "$processed_time")):
            rows = self.db["events"].aggregate([
                {"$match": base},
                {"$group": {"_id": {"$dateToString": {"format": MINUTE_FMT, "date": field}}, "n": {"$sum": 1}}}])
            out[key] = {r["_id"]: r["n"] for r in rows}
        return out

    def rec_stats(self, cutoff):
        rows = self.db["events"].aggregate([
            {"$match": {"event_type": {"$in": ["rec_shown", "rec_clicked", "rec_added_to_cart"]}, "event_time": {"$gte": cutoff}}},
            {"$group": {"_id": {"t": "$event_type", "rule": "$payload.rule", "g": "$group"}, "n": {"$sum": 1}}}])
        return [{"t": r["_id"]["t"], "rule": r["_id"].get("rule"), "g": r["_id"].get("g"), "n": r["n"]} for r in rows]

    def business(self, cutoff):
        ev = self.db["events"]
        sess = {r["_id"]: {"sessions": r["sessions"], "converted": r["converted"]} for r in ev.aggregate([
            {"$match": {"event_type": {"$in": ["app_open", "order_placed"]}, "event_time": {"$gte": cutoff}}},
            {"$group": {"_id": "$session_id", "g": {"$first": "$group"}, "types": {"$addToSet": "$event_type"}}},
            {"$match": {"types": "app_open"}},
            {"$group": {"_id": "$g", "sessions": {"$sum": 1},
                        "converted": {"$sum": {"$cond": [{"$in": ["order_placed", "$types"]}, 1, 0]}}}}])}
        orders = {r["_id"]: {"n": r["n"], "value": r["value"], "sumsq": r["sumsq"]} for r in ev.aggregate([
            {"$match": {"event_type": "order_placed", "event_time": {"$gte": cutoff}}},
            {"$group": {"_id": "$group", "n": {"$sum": 1}, "value": {"$sum": "$payload.order_value"},
                        "sumsq": {"$sum": {"$multiply": ["$payload.order_value", "$payload.order_value"]}}}}])}
        return {"sessions": sess, "orders": orders}

    def ratings(self, cutoff):
        agg = defaultdict(lambda: [0, 0])
        for d in self.db["events"].find({"event_type": "rating_submitted", "event_time": {"$gte": cutoff}},
                                        {"payload.product_id": 1, "payload.rating": 1}):
            cat = self.category.get(d["payload"]["product_id"])
            agg[cat][0] += d["payload"]["rating"]
            agg[cat][1] += 1
        return {c: (s / n, n) for c, (s, n) in agg.items() if n}

    def rating_replacements(self, cutoff):
        rows = self.db["recommendations"].aggregate([
            {"$match": {"created_at": {"$gte": cutoff}}},
            {"$unwind": "$recommendations"},
            {"$group": {"_id": "$recommendations.rating_adjusted", "n": {"$sum": 1}}}])
        got = {bool(r["_id"]): r["n"] for r in rows}
        return {"items": sum(got.values()), "replaced": got.get(True, 0)}

    def experiment(self):
        return self.db["experiment_results"].find_one(sort=[("created_at", -1)])

    def alerts(self):
        return list(self.db["alerts"].find().sort("created_at", -1).limit(5))

    # ------------------------------------------------------------ everything for one refresh
    def gather(self, window_min):
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=window_min)
        jobs = {
            "eps": (self.events_per_sec, now), "funnel": (self.funnel, cutoff), "metrics": (self.metrics, window_min),
            "late": (self.late_events, cutoff), "rec": (self.rec_stats, cutoff), "business": (self.business, cutoff),
            "ratings": (self.ratings, cutoff), "alerts": (self.alerts,), "experiment": (self.experiment,), "replaced": (self.rating_replacements, cutoff),
        }
        futures = {k: self.pool.submit(fn, *args) for k, (fn, *args) in jobs.items()}
        out, errors = {}, {}
        for k, fu in futures.items():
            try:
                out[k] = fu.result(timeout=20)
            except Exception as e:
                out[k], errors[k] = None, f"{k}: {str(e)[:90]}"
        out.update(now=now, window_min=window_min, errors=errors, baseline=self.baseline, quality=self.quality)
        return out
