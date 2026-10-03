"""
m6_load_results.py - puts the newest results/experiment_*.json into MongoDB so the dashboard panel shows it.
Use it instead of re-running the 10-minute experiment when the file already exists.

    python m6_load_results.py
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

files = sorted(Path("results").glob("experiment_*.json"))
if not files:
    sys.exit("No results/experiment_*.json file found. Run python m6_experiment.py first.")
doc = json.loads(files[-1].read_text())
doc["created_at"] = datetime.now(timezone.utc)
from db import get_db
get_db()["experiment_results"].insert_one(doc)
p = doc["params"]
print(f"Loaded {files[-1].name} ({p['replications']} replications x {p['measured_sessions_per_replication']} measured sessions) into MongoDB.")
print("Restart the dashboard (python m5_dashboard.py) to see it.")
