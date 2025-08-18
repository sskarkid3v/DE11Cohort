import json
from datetime import datetime

def load_state(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"last_loaded_at": "1970-01-01T00:00:00Z"}

def save_state(path, state):
    with open(path, "w") as f:
        json.dump(state, f)

def max_ts(rows, default_ts):
    if not rows:
        return default_ts
    mx = max(r["updated_at"] for r in rows)
    return mx