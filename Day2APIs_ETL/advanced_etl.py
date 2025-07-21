import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from apscheduler.schedulers.blocking import BlockingScheduler

#setup logging


#rate limit setting
MAX_REQUESTS_PER_SECOND = 1
RATE_LIMIT_DELAY = 1/MAX_REQUESTS_PER_SECOND

#postgreSQL database connection

#retry decorator for handling API request failures
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def fetch_data_page(page):
    url = f"https:/catfact.ninja/facts?page={page}"
    response= requests.get(url, timeout=10)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()

def etl_datafacts():
    all_facts = []
    page = 1
    while True:
        data = fetch_data_page(page)
        facts = data.get('data', [])
        for fact in facts:
            #extract key values from the dictionary
        if not data.get("next_page_url"):
            break
        page += 1
        
    #create a DataFrame from the list of facts
    
    #load data into the PostgreSQL database

scheduler = BlockingScheduler()
scheduler.add_job(etl_datafacts, 'interval', hours=1)

if __name__ == "__main__":
    scheduler.start()
