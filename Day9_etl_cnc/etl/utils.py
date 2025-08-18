import logging, time
from functools import wraps

def get_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("etl")
    
def retry(attempts=3, base_sleep=1):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            sleep = base_sleep
            for i in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if i == attempts - 1:
                        raise
                    time.sleep(sleep)
                    sleep *= 2  # Exponential backoff
        return wrapper 
    return deco