"""m2_check.py - quick health check of the events stored in MongoDB."""
import pandas as pd
from db import get_db

db = get_db()
rows = list(db["events"].find({}, {"event_type": 1, "device_offline": 1, "lateness_sec": 1, "user_id": 1, "partition": 1}))
if not rows:
    raise SystemExit("No events found yet. Run the consumer and the producer first.")
df = pd.DataFrame(rows)
n = len(df)
print(f"events stored          : {n}")
print(f"distinct customers     : {df.user_id.nunique()}")
print("\nevents by type:")
print(df.event_type.value_counts().to_string())
late = df[df.device_offline]
ontime = df[~df.device_offline]
print(f"\nlate (offline) events  : {len(late)}  ({100*len(late)/n:.1f}% of all)   <- target 5 to 10% once running for a few minutes")
print(f"on-time lag, median    : {ontime.lateness_sec.median():.2f} s   (should be well under 1 s)")
if len(late):
    print(f"late lag, median / max : {late.lateness_sec.median():.0f} s / {late.lateness_sec.max():.0f} s   (should be about 30 to 300 s)")
if "partition" in df and df.partition.notna().any():
    print("\nevents per Kafka partition:")
    print(df.partition.value_counts().sort_index().to_string())
