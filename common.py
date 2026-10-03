"""common.py - small helpers shared by all scripts."""
import hashlib
import uuid
from datetime import datetime, timezone


def iso(ts):
    """epoch seconds -> ISO-8601 UTC string with milliseconds."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds")


def parse_iso(s):
    return datetime.fromisoformat(s)


def ab_group(user_id):
    """Fixed 50/50 split: every user is always in the same group."""
    h = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16)
    return "treatment" if h % 2 == 0 else "control"


def make_event(event_type, user_id, session_id, event_ts, payload, late=False):
    return {
        "event_id": "e_" + uuid.uuid4().hex[:12],
        "event_type": event_type,
        "user_id": int(user_id),
        "session_id": session_id,
        "event_time": iso(event_ts),
        "device_offline": bool(late),
        "payload": payload,
    }
