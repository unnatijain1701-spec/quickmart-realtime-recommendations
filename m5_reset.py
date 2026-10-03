"""
m5_reset.py - clears the stored RUN data for a clean demo or screen recording.
Deletes: events, recommendations, metrics_minute, alerts.   Keeps: products, users, order_history, rating_stats.
Stop the producer, processor, consumer and web app first, otherwise new data keeps arriving.

    python m5_reset.py
"""
import sys

RUN_COLLECTIONS = ["events", "recommendations", "metrics_minute", "alerts"]


def reset(db):
    return {name: db[name].delete_many({}).deleted_count for name in RUN_COLLECTIONS}


if __name__ == "__main__":
    from db import get_db
    db = get_db()
    print("This will DELETE the stored run data:")
    for name in RUN_COLLECTIONS:
        print(f"  {name:<16} {db[name].count_documents({}):>8} documents")
    if input("Type RESET to continue: ").strip() != "RESET":
        sys.exit("Cancelled. Nothing was deleted.")
    for name, n in reset(db).items():
        print(f"  deleted {n} from {name}")
    print("Done. Start the consumer, processor and producer again for a fresh run.")
