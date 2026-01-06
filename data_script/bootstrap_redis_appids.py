import os
import json
import time
import datetime as dt
import redis
import sys
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# The source file of all known app IDs
APP_LIST_FILE = 'applist.cache.json'

# Redis keys to manage
REDIS_KEY_TO_SCRAPE = 'steam:apps:to_scrape'
REDIS_KEY_SCRAPED = 'steam:apps:scraped'
REDIS_KEY_DISCARDED = 'steam:apps:discarded'

def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def create_redis_client():
    log(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
        client.ping()
        log("SUCCESS: Redis client connected.")
        return client
    except Exception as e:
        log(f"CRITICAL: Could not connect to Redis: {e}")
        return None

if __name__ == "__main__":
    if not os.path.exists(APP_LIST_FILE):
        log(f"CRITICAL: App list cache file '{APP_LIST_FILE}' not found. Cannot bootstrap. Aborting.")
        sys.exit(1)
        
    redis_client = create_redis_client()
    if not redis_client:
        sys.exit(1)
        
    log("--- Redis State Initializer from App Cache ---")

    # --- Step 1: Reset ALL related state keys in Redis ---
    log("Resetting all producer state keys for a clean start...")
    deleted_count = redis_client.delete(
        REDIS_KEY_TO_SCRAPE, 
        REDIS_KEY_SCRAPED, 
        REDIS_KEY_DISCARDED
    )
    log(f"  - Deleted {deleted_count} old keys. State is now empty.")
    
    # --- Step 2: Load all App IDs from the cache file ---
    with open(APP_LIST_FILE, 'r', encoding='utf-8') as f:
        all_appids = json.load(f)
    log(f"Loaded {len(all_appids)} total App IDs from '{APP_LIST_FILE}'.")
    
    # --- Step 3: Populate the 'to_scrape' set with all App IDs ---
    log(f"Populating the '{REDIS_KEY_SCRAPED}' set. This will be the master work queue.")
    
    pipe = redis_client.pipeline()
    batch_size = 50000
    for i in range(0, len(all_appids), batch_size):
        batch = all_appids[i:i + batch_size]
        pipe.sadd(REDIS_KEY_SCRAPED, *batch)
    
    log("Executing bulk insert into Redis...")
    pipe.execute()
    
    # --- Step 4: Verify the final state ---
    scraped_count = redis_client.scard(REDIS_KEY_SCRAPED)
    to_scrape_count = redis_client.scard(REDIS_KEY_TO_SCRAPE)
    discarded_count = redis_client.scard(REDIS_KEY_DISCARDED)
    
    log("\n--- Verification ---")
    log(f"'{REDIS_KEY_TO_SCRAPE}' now contains: {to_scrape_count} IDs.")
    log(f"'{REDIS_KEY_SCRAPED}' now contains:   {scraped_count} IDs.")
    log(f"'{REDIS_KEY_DISCARDED}' now contains: {discarded_count} IDs.")
    log("--- BOOTSTRAP COMPLETE ---")
    log("The producer is now ready to start processing the entire catalog from scratch.")