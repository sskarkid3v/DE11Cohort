import json, os
from datetime import datetime, timedelta
from dateutil.parser import isoparse
from .utils import retry
from .config import DATA_DIR, LOOKBACK_DAYS

def _read_json_file(path):
    with open(path, "r") as f:
        return json.load(f)
   
@retry(attempts=3, base_sleep=1)    
def fetch_incremental(now_utc, last_loaded_at_iso):
    lookback_start = (now_utc - timedelta(days=LOOKBACK_DAYS)).date()
    rows = []
    for fname in sorted(os.listdir(DATA_DIR)):
        if not fname.endswith(".json"):
            continue
        fdate = datetime.fromisoformat(fname.replace(".json", "")).date()
        if fdate >= lookback_start:
            path = os.path.join(DATA_DIR, fname)
            rows.extend(_read_json_file(path))
    
    return [
        r for r in rows
        if isoparse(r["updated_at"]) > isoparse(last_loaded_at_iso)
    ]

@retry(attempts=3, base_sleep=1)    
def fetch_for_date(date_str):
    path = os.path.join(DATA_DIR, f"{date_str}.json")
    if not os.path.exists(path):
        return []
    return _read_json_file(path)