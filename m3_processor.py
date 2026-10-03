"""
m3_processor.py - Milestone 3: the stream processor.

Reads customer_events from Kafka, keeps live state (carts, trending window, ratings),
applies the four recommendation rules, publishes each recommendation to the 'recommendations'
topic and stores it in MongoDB.  Every 5 seconds it also writes the current minute's metrics
(events, late share, latency) to MongoDB 'metrics_minute', and raises alerts on the 'alerts' topic.

Start order:  Kafka, then this processor, then the producer.
Demo settings:  python m3_processor.py --trend-window 90 --trend-slide 2
"""
import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone

import config
from common import iso
from recommender import TRIGGERS, Recommender, load_base_from_db, pair_counts_from_simulation


def p95(values):
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(0.95 * len(s)))]


class Processor:
    def __init__(self, rec, produce, store, now_fn=time.time):
        """produce(topic, key, value) sends to Kafka.  store = dict of MongoDB collections."""
        self.rec, self.produce, self.store, self.now_fn = rec, produce, store, now_fn
        self.rec_buffer = []
        self._reset(self._bucket_start(now_fn()))
        self.last_flush = now_fn()
        self.alerted = set()
        self.total_events = self.total_recs = 0

    @staticmethod
    def _bucket_start(now):
        return int(now // config.TUMBLING_SEC) * config.TUMBLING_SEC

    def _reset(self, start):
        self.bucket = start
        self.events = self.late = self.recs = 0
        self.delays, self.rec_lat = [], []
        self.by_rule, self.by_group = {}, {}

    # ------------------------------------------------------------ one event
    def handle(self, ev, kafka_ts_ms):
        now = self.now_fn()
        if self._bucket_start(now) != self.bucket:
            self.flush_metrics(final=True)
            self._reset(self._bucket_start(now))
            self.alerted.clear()
        self.events += 1
        self.total_events += 1
        self.late += 1 if ev["device_offline"] else 0
        self.delays.append(now * 1000 - kafka_ts_ms)
        self.rec.process(ev)
        if ev["event_type"] in TRIGGERS:
            r = self.rec.recommend(ev)
            r["latency_ms"] = round(self.now_fn() * 1000 - kafka_ts_ms, 1)
            self.produce(config.TOPIC_RECS, r["user_id"], r)
            self.rec_buffer.append(r)
            self.recs += 1
            self.total_recs += 1
            self.rec_lat.append(r["latency_ms"])
            self.by_group[r["group"]] = self.by_group.get(r["group"], 0) + 1
            for x in r["recommendations"]:
                self.by_rule[x["rule"]] = self.by_rule.get(x["rule"], 0) + 1

    # ------------------------------------------------------------ MongoDB writes
    def flush_recs(self):
        if self.rec_buffer and "recommendations" in self.store:
            docs = []
            for r in self.rec_buffer:
                d = dict(r)
                d["_id"] = d["rec_id"]
                d["created_at"] = datetime.fromtimestamp(self.now_fn(), tz=timezone.utc)
                docs.append(d)
            try:
                self.store["recommendations"].insert_many(docs, ordered=False)
            except Exception as e:
                print("  warning while storing recommendations:", str(e)[:100])
        self.rec_buffer = []

    def flush_metrics(self, final=False):
        self.flush_recs()
        n = max(self.events, 1)
        doc = {
            "bucket_start": datetime.fromtimestamp(self.bucket, tz=timezone.utc),
            "events": self.events, "late_events": self.late, "late_share": round(self.late / n, 4),
            "recs": self.recs, "recs_by_rule": self.by_rule, "recs_by_group": self.by_group,
            "proc_delay_avg_ms": round(sum(self.delays) / n, 1), "proc_delay_p95_ms": round(p95(self.delays), 1),
            "rec_latency_avg_ms": round(sum(self.rec_lat) / max(len(self.rec_lat), 1), 1),
            "rec_latency_p95_ms": round(p95(self.rec_lat), 1),
            "updated_at": datetime.fromtimestamp(self.now_fn(), tz=timezone.utc), "final": final,
        }
        if "metrics_minute" in self.store:
            try:
                self.store["metrics_minute"].replace_one({"_id": iso(self.bucket)}, dict(doc, _id=iso(self.bucket)), upsert=True)
            except Exception as e:
                print("  warning while storing metrics:", str(e)[:100])
        self._check_alerts(doc)
        self.last_flush = self.now_fn()

    def _check_alerts(self, doc):
        if self.events < 20:
            return
        for kind, bad, msg in (
            ("processing_delay", doc["proc_delay_p95_ms"] > config.ALERT_MAX_DELAY_SEC * 1000,
             f"95th-percentile processing delay {doc['proc_delay_p95_ms']:.0f} ms is above {config.ALERT_MAX_DELAY_SEC * 1000} ms"),
            ("late_share", doc["late_share"] > config.ALERT_MAX_LATE_SHARE,
             f"late-event share {100 * doc['late_share']:.1f}% is above {100 * config.ALERT_MAX_LATE_SHARE:.0f}%"),
        ):
            if bad and kind not in self.alerted:
                self.alerted.add(kind)
                alert = {"kind": kind, "message": msg, "bucket_start": iso(self.bucket), "created_at": iso(self.now_fn())}
                self.produce(config.TOPIC_ALERTS, kind, alert)
                if "alerts" in self.store:
                    self.store["alerts"].insert_one(dict(alert))
                print("  ALERT:", msg)

    def tick(self):
        if self.now_fn() - self.last_flush >= config.METRICS_FLUSH_SEC:
            self.flush_metrics()
            return True
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trend-window", type=float, default=None, help="seconds (default 300; use 90 for the demo)")
    ap.add_argument("--trend-slide", type=float, default=None, help="seconds between trending refreshes (default 10; use 2 for the demo)")
    ap.add_argument("--stale-refresh", type=float, default=None, help="seconds between control-group snapshots (default 3600)")
    args = ap.parse_args()

    from kafka import KafkaConsumer, KafkaProducer
    from db import get_db
    from simulator import Simulator

    db = get_db()
    print("Loading base data from MongoDB ...")
    products, user_cats, ratings = load_base_from_db(db)
    print(f"Building 'frequently bought together' counts from {config.PAIR_COUNT_SESSIONS} simulated baskets ...")
    pairs = pair_counts_from_simulation(Simulator(seed=config.RANDOM_SEED))
    rec = Recommender(products, user_cats, ratings, pairs, trend_window=args.trend_window,
                      trend_slide=args.trend_slide, stale_refresh=args.stale_refresh)
    db["recommendations"].create_index("user_id")
    db["recommendations"].create_index("created_at")

    try:
        producer = KafkaProducer(bootstrap_servers=[config.KAFKA_BROKER],
                                 key_serializer=lambda k: str(k).encode(),
                                 value_serializer=lambda v: json.dumps(v).encode(), linger_ms=5)
        consumer = KafkaConsumer(config.TOPIC_EVENTS, bootstrap_servers=[config.KAFKA_BROKER], group_id="qc-processor",
                                 auto_offset_reset="latest", enable_auto_commit=True,
                                 value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    except Exception as e:
        sys.exit(f"Could not connect to Kafka at {config.KAFKA_BROKER}: {e}")

    proc = Processor(rec, lambda t, k, v: producer.send(t, key=k, value=v),
                     {"recommendations": db["recommendations"], "metrics_minute": db["metrics_minute"], "alerts": db["alerts"]})
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *a: stop.update(flag=True))
    print(f"Processor running: trending window {rec.trend_window:g}s (refresh {rec.trend_slide:g}s). Ctrl+C to stop.")
    started, last = time.time(), time.time()
    while not stop["flag"]:
        batch = consumer.poll(timeout_ms=300, max_records=500)
        for msgs in batch.values():
            for m in msgs:
                proc.handle(m.value, m.timestamp)
        proc.flush_recs()
        proc.tick()
        if time.time() - last >= 5:
            last = time.time()
            print(f"  events {proc.total_events:>7} | recommendations {proc.total_recs:>6} | "
                  f"{proc.total_events / (last - started):5.1f} events/sec")
    proc.flush_metrics(final=True)
    producer.flush()
    consumer.close()
    print(f"Stopped. Processed {proc.total_events} events, produced {proc.total_recs} recommendations.")


if __name__ == "__main__":
    main()
