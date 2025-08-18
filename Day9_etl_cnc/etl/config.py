import os

DB_URL = os.getenv("DB_URL", "postgresql://postgres:postgres@db:5432/warehouse")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", 2))
DATA_DIR = os.getenv("DATA_DIR", "data/source")
STATE_FILE = os.getenv("STATE_FILE", "etl_state.json")