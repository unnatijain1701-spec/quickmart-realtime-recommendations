"""
m2_store_consumer.py - reads customer_events from Kafka and stores every event in MongoDB ('events').
Adds processed_time (when we received it) and lateness_sec (processed_time minus event_time).

START THIS BEFORE the producer (it only reads events that arrive after it starts).
"""
import json
import signal
import sys
import time
from datetime import datetime, timezone

import config
from common import ab_group, parse_iso


def to_doc(ev, partition=None, offset=None):
    """Turn a Kafka event into the MongoDB document."""
    processed = datetime.now(timezone.utc)
    event_time = parse_iso(ev["event_time"])
    doc = dict(ev)
    doc["_id"] = ev["event_id"]
    doc["event_time"] = event_time
    doc["processed_time"] = processed
    doc["lateness_sec"] = round((processed - event_time).total_seconds(), 3)
    doc["group"] = ab_group(ev["user_id"])
    doc["partition"], doc["offset"] = partition, offset
    return doc


def flush(coll, docs):
    if not docs:
        return 0
    try:
        coll.insert_many(docs, ordered=False)
    except Exception as e:                      # duplicate ids etc. should not stop the pipeline
        print("  warning while storing batch:", str(e)[:120])
    return len(docs)


def main():
    from kafka import KafkaConsumer
    from db import get_db
    coll = get_db()["events"]
    coll.create_index("event_type")
    coll.create_index("event_time")
    try:
        consumer = KafkaConsumer(
            config.TOPIC_EVENTS, bootstrap_servers=[config.KAFKA_BROKER], group_id="qc-store",
            auto_offset_reset="latest", enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    except Exception as e:
        sys.exit(f"Could not connect to Kafka at {config.KAFKA_BROKER}: {e}")

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *a: stop.update(flag=True))
    print(f"Store consumer started: Kafka '{config.TOPIC_EVENTS}' -> MongoDB '{config.DB_NAME}.events'. Ctrl+C to stop.")
    stored, started, last = 0, time.time(), time.time()
    while not stop["flag"]:
        batch = consumer.poll(timeout_ms=500, max_records=500)
        docs = [to_doc(m.value, m.partition, m.offset) for msgs in batch.values() for m in msgs]
        stored += flush(coll, docs)
        if time.time() - last >= 5:
            last = time.time()
            print(f"  stored {stored:>7} events | {stored/(last-started):6.1f}/sec")
    consumer.close()
    print(f"Stopped. Stored {stored} events.")


if __name__ == "__main__":
    main()
