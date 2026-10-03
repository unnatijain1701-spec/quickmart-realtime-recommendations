"""
m2_producer.py - Milestone 2: stream simulated customer events into Kafka (topic customer_events).

Examples:
    python m2_producer.py                       # 20 events/sec until you press Ctrl+C
    python m2_producer.py --rate 50             # 50 events/sec
    python m2_producer.py --dry-run --speed 100 --duration 2   # no Kafka: quick self-test
"""
import argparse
import json
import signal
import sys
import time
from collections import Counter

import config
from simulator import Simulator

ap = argparse.ArgumentParser()
ap.add_argument("--rate", type=float, default=20.0, help="target events per second")
ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until Ctrl+C)")
ap.add_argument("--speed", type=float, default=1.0, help="time speed-up (1 = real time)")
ap.add_argument("--seed", type=int, default=None)
ap.add_argument("--dry-run", action="store_true", help="do not use Kafka, just print and count")
args = ap.parse_args()

stopping = {"flag": False}
signal.signal(signal.SIGINT, lambda *a: stopping.update(flag=True))

counts, late = Counter(), [0]
started = time.time()

if args.dry_run:
    shown = [0]
    def publish(ev):
        if shown[0] < 12:
            print(json.dumps(ev)); shown[0] += 1
    producer = None
else:
    from kafka import KafkaProducer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[config.KAFKA_BROKER],
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5, acks=1, request_timeout_ms=10000)
    except Exception as e:
        sys.exit(f"Could not connect to Kafka at {config.KAFKA_BROKER}: {e}\n"
                 "Is Docker Desktop running and Kafka started (docker compose up -d)?")
    def publish(ev):
        producer.send(config.TOPIC_EVENTS, key=ev["user_id"], value=ev)

def counting_publish(ev):
    counts[ev["event_type"]] += 1
    late[0] += ev["device_offline"]
    publish(ev)

def tick():
    n = sum(counts.values()); el = time.time() - started
    print(f"  sent {n:>7} events | {n/el:6.1f}/sec | late {100*late[0]/max(n,1):4.1f}%")

print(f"Producer started: target {args.rate}/sec, Kafka {'OFF (dry run)' if args.dry_run else config.KAFKA_BROKER}. Ctrl+C to stop.")
sim = Simulator(seed=args.seed)
held = sim.run(counting_publish, rate=args.rate, duration=args.duration, speed=args.speed,
               stop=lambda: stopping["flag"], on_tick=tick)
if producer:
    producer.flush()
n = sum(counts.values())
print("\nDONE")
print(f"  total events sent : {n}")
print(f"  late (offline)    : {late[0]}  ({100*late[0]/max(n,1):.1f}%)")
print(f"  held back at stop : {held}")
for k, v in counts.most_common():
    print(f"  {k:<20} {v}")
