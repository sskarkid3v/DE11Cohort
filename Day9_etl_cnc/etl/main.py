import argparse
from datetime import datetime, timedelta
from dateutil.parser import isoparse

from .config import DB_URL, STATE_FILE, LOOKBACK_DAYS
from .utils import get_logger
from .state import load_state, save_state, max_ts
from .source import fetch_incremental, fetch_for_date
from .transform import clean_rows
from .load import upsert_rows

log = get_logger()

def run_incremental():
    log.info("mode=incremental start")
    state = load_state(STATE_FILE)
    last_loaded_at = state["last_loaded_at"]
    
    now_utc = datetime.utcnow()
    raw = fetch_incremental(now_utc, last_loaded_at)
    rows = clean_rows(raw)
    
    upserts = upsert_rows(DB_URL, rows)
    log.info(f"mode=incremental upserts={upserts}")
    
    new_high = max_ts(rows, last_loaded_at)
    state["last_loaded_at"] = new_high
    save_state(STATE_FILE, state)
    
    log.info("mode=incremental success")
    
def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)  
        
def run_backfill(start, end):
    log.info("mode=backfill start={start} end ={end}")
    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()
    total = 0
    
    for d in daterange(start_d, end_d):
        raw = fetch_for_date(d.isoformat())
        rows = clean_rows(raw)
        upserts = upsert_rows(DB_URL, rows)
        total += upserts
        log.info(f"mode=backfill date={d.isoformat()} upserts={upserts}")
    
    log.info(f"mode=backfill success total_upserts={total}")
    
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["incremental", "backfill"], required=True)
    p.add_argument("--start")
    p.add_argument("--end")
    args = p.parse_args()
    
    if args.mode == "incremental":
        run_incremental()
    else:
        if not (args.start and args.end):
            raise SystemExit("For backfill mode, --start and --end arguments with values are required")
        run_backfill(args.start, args.end)

