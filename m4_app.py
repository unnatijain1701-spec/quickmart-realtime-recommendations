"""
m4_app.py - Milestone 4: the customer screen (FastAPI + WebSocket).

* serves the customer web page
* turns the customer's clicks into events and sends them to Kafka (customer_events)
* reads recommendations from Kafka (topic 'recommendations') and pushes them to the browser over a WebSocket
* when a recommendation is displayed it records a 'rec_shown' event, so the dashboard can compute click-through

Run:   python m4_app.py        then open  http://127.0.0.1:8000
"""
import asyncio
import json
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Query, WebSocket
from fastapi.responses import FileResponse
from starlette.websockets import WebSocketDisconnect

import config
from common import ab_group, make_event

WEB_DIR = Path(__file__).parent / "web"


# ------------------------------------------------------------------ message buses
class KafkaBus:
    def __init__(self):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=[config.KAFKA_BROKER],
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"), linger_ms=0)
        self._stop = False

    def send_event(self, ev):
        self.producer.send(config.TOPIC_EVENTS, key=ev["user_id"], value=ev)

    def start_recs_listener(self, on_rec):
        from kafka import KafkaConsumer

        def run():
            consumer = KafkaConsumer(
                config.TOPIC_RECS, bootstrap_servers=[config.KAFKA_BROKER],
                group_id="qc-web-" + uuid.uuid4().hex[:6], auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")))
            while not self._stop:
                for msgs in consumer.poll(timeout_ms=200).values():
                    for m in msgs:
                        on_rec(m.value)
            consumer.close()
        threading.Thread(target=run, daemon=True).start()

    def close(self):
        self._stop = True
        self.producer.flush()


class FakeBus:
    """Used by the tests: no Kafka."""
    def __init__(self):
        self.events, self.cb = [], None

    def send_event(self, ev):
        self.events.append(ev)

    def start_recs_listener(self, on_rec):
        self.cb = on_rec

    def inject_rec(self, rec):
        self.cb(rec)

    def close(self):
        pass


# ------------------------------------------------------------------ the app
def create_app(bus, products, demo_users, areas=None):
    """products: list of dicts (product_id, name, brand, category, price, rating, rating_count).
       demo_users: {'treatment': id, 'control': id}."""
    areas = areas or {}
    by_id = {p["product_id"]: p for p in products}
    demo_ids = set(demo_users.values())
    connections = {}                     # user_id -> set of asyncio queues
    state = {"loop": None}

    def deliver(rec):                    # runs on the event loop
        qs = connections.get(rec["user_id"])
        if not qs:
            return
        for q in list(qs):
            q.put_nowait(rec)
        now = time.time()
        for pos, x in enumerate(rec["recommendations"], 1):      # the customer can now see these
            bus.send_event(make_event("rec_shown", rec["user_id"], "s_web", now, {
                "rec_id": rec["rec_id"], "product_id": x["product_id"], "rule": x["rule"],
                "reason": x["reason"], "position": pos, "group": rec["group"]}))

    @asynccontextmanager
    async def lifespan(app):
        loop = asyncio.get_running_loop()
        bus.start_recs_listener(lambda rec: loop.call_soon_threadsafe(deliver, rec))
        yield
        bus.close()

    app = FastAPI(title="Quick-commerce customer screen", lifespan=lifespan)

    @app.get("/")
    def index():
        return FileResponse(WEB_DIR / "index.html")

    @app.get("/api/config")
    def get_config():
        return {"demo_users": demo_users,
                "groups": {str(u): ab_group(u) for u in demo_ids},
                "free_delivery_threshold": config.FREE_DELIVERY_THRESHOLD,
                "categories": sorted({p["category"] for p in products})}

    @app.get("/api/products")
    def list_products(category: str = "", q: str = "", limit: int = Query(24, le=300)):
        out = [p for p in products if (not category or p["category"] == category)
               and (not q or q.lower() in p["name"].lower() or q.lower() in p["brand"].lower())]
        return out[:limit]

    @app.post("/api/action")
    async def action(payload: dict = Body(...)):
        try:
            uid = int(payload["user_id"])
            kind = payload["type"]
        except (KeyError, ValueError):
            raise HTTPException(400, "user_id and type are required")
        if uid not in demo_ids:
            raise HTTPException(400, "unknown demo user")
        sid = str(payload.get("session_id", "s_web"))[:40]
        now = time.time()

        def product():
            p = by_id.get(int(payload.get("product_id", -1)))
            if not p:
                raise HTTPException(400, "unknown product")
            return p

        def rec_info(p):
            return {"rec_id": payload.get("rec_id"), "product_id": p["product_id"],
                    "rule": payload.get("rule"), "group": ab_group(uid)}

        def line(p, qty):
            return {"product_id": p["product_id"], "category": p["category"], "qty": qty, "price": p["price"]}

        evs = []
        if kind == "app_open":
            evs.append(make_event("app_open", uid, sid, now, {"device": "web", "city": areas.get(uid, "")}))
        elif kind in ("add_to_cart", "remove_from_cart"):
            p = product()
            evs.append(make_event(kind, uid, sid, now, line(p, max(1, int(payload.get("qty", 1))))))
        elif kind == "product_click":
            p = product()
            evs.append(make_event("product_click", uid, sid, now,
                                  {"product_id": p["product_id"], "category": p["category"], "source": "browse"}))
        elif kind == "rec_click":
            p = product()
            evs.append(make_event("product_click", uid, sid, now,
                                  {"product_id": p["product_id"], "category": p["category"], "source": "recommendation"}))
            evs.append(make_event("rec_clicked", uid, sid, now + 0.001, rec_info(p)))
        elif kind == "rec_add":
            p = product()
            evs.append(make_event("rec_clicked", uid, sid, now, rec_info(p)))
            evs.append(make_event("rec_added_to_cart", uid, sid, now + 0.001, rec_info(p)))
            evs.append(make_event("add_to_cart", uid, sid, now + 0.002, dict(line(p, 1), source="recommendation")))
        elif kind == "order":
            lines = []
            for it in payload.get("items", []):
                p = by_id.get(int(it.get("product_id", -1)))
                if p:
                    lines.append({"product_id": p["product_id"], "qty": max(1, int(it.get("qty", 1))), "price": p["price"]})
            if not lines:
                raise HTTPException(400, "empty order")
            subtotal = sum(l["qty"] * l["price"] for l in lines)
            discount, names = 0.0, set()
            for l in lines:
                p = by_id[l["product_id"]]
                if p.get("offer_pct"):
                    discount += l["qty"] * l["price"] * p["offer_pct"] / 100
                    names.add(p["offer_name"])
            evs.append(make_event("order_placed", uid, sid, now, {
                "order_id": "o_web_" + uuid.uuid4().hex[:8], "items": lines,
                "subtotal": round(subtotal, 2), "discount_total": round(discount, 2),
                "order_value": round(subtotal - discount, 2), "offer_applied": sorted(names) or None}))
        else:
            raise HTTPException(400, "unknown action type")
        for e in evs:
            bus.send_event(e)
        return {"event_ids": [e["event_id"] for e in evs]}

    @app.websocket("/ws/{user_id}")
    async def ws(websocket: WebSocket, user_id: int):
        await websocket.accept()
        q = asyncio.Queue()
        connections.setdefault(user_id, set()).add(q)

        async def reader():
            try:
                while True:
                    await websocket.receive_text()
            except WebSocketDisconnect:
                pass

        async def writer():
            while True:
                rec = await q.get()
                await websocket.send_json({"type": "recommendation", **rec})

        tasks = [asyncio.create_task(reader()), asyncio.create_task(writer())]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            connections.get(user_id, set()).discard(q)

    return app


# ------------------------------------------------------------------ real start-up
def load_catalogue(db):
    stats = {d["product_id"]: d for d in db["rating_stats"].find()}
    products = []
    for d in db["products"].find():
        st = stats.get(d["product_id"], {})
        c = st.get("rating_count", 0)
        off = config.offer_for(d["category"])
        products.append({"product_id": d["product_id"], "name": d["name"], "brand": d.get("brand", ""),
                         "category": d["category"], "price": d["price"], "mrp": d.get("mrp", d["price"]),
                         "offer_pct": off["discount_pct"] if off else 0, "offer_name": off["name"] if off else "",
                         "rating": round(st["rating_sum"] / c, 1) if c >= config.RATING_MIN_COUNT else None,
                         "rating_count": c})
    products.sort(key=lambda p: (p["category"], p["name"], p["product_id"]))
    return products


def pick_demo_users(db):
    """Demo customers come from the ones who never appear in the simulated stream (no order history),
       so the screen only ever shows your own actions."""
    pool = [d for d in db["users"].find({"order_count": 0})] or list(db["users"].find())
    pool.sort(key=lambda d: d["user_id"])
    pick = {}
    for d in pool:
        pick.setdefault(ab_group(d["user_id"]), d)
        if len(pick) == 2:
            break
    return {g: d["user_id"] for g, d in pick.items()}, {d["user_id"]: d.get("area", "") for d in pool}


def main():
    import argparse
    import uvicorn
    from db import get_db
    ap = argparse.ArgumentParser()
    ap.add_argument("--share", action="store_true", help="let other laptops on the same network open the shop page")
    args = ap.parse_args()
    db = get_db()
    products = load_catalogue(db)
    demo_users, areas = pick_demo_users(db)
    try:
        bus = KafkaBus()
    except Exception as e:
        raise SystemExit(f"Could not connect to Kafka at {config.KAFKA_BROKER}: {e}")
    app = create_app(bus, products, demo_users, areas)
    host = "0.0.0.0" if args.share else config.WEB_HOST
    shown = config.lan_ip() if args.share else config.WEB_HOST
    print(f"Customer screen on this laptop: http://127.0.0.1:{config.WEB_PORT}")
    print(f"  live recommendations  (treatment): http://{shown}:{config.WEB_PORT}/?group=treatment")
    print(f"  stale recommendations (control)  : http://{shown}:{config.WEB_PORT}/?group=control")
    if args.share:
        print("  SHARING is on: type the addresses above on the other laptop (same Wi-Fi or hotspot).")
    uvicorn.run(app, host=host, port=config.WEB_PORT, log_level="warning")


if __name__ == "__main__":
    main()
