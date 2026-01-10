# File: producer.py
import os
import time
import json
import requests
import datetime as dt
import redis
import re
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

# Load environment variables for local development
load_dotenv()

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# --- BEHAVIOR CONFIGURATION ---
POLL_INTERVAL_SECONDS = 3600 # 1 hour
API_SLEEP_SECONDS = 1.5
BATCH_SIZE = 50
CHECK_FOR_NEW_APPS_CYCLE = 6 # Every 6 hours

# --- KAFKA/REDIS CONFIGURATION ---
KAFKA_TOPIC = 'steam-games-raw' # Correct topic for raw game data
REDIS_KEY_TO_SCRAPE = 'steam:apps:to_scrape'
REDIS_KEY_SCRAPED = 'steam:apps:scraped'
REDIS_KEY_DISCARDED = 'steam:apps:discarded'

# --- API ENDPOINTS ---
STEAM_APP_LIST_URL = 'https://api.steampowered.com/IStoreService/GetAppList/v1/'
STEAM_APP_DETAILS_URL = 'https://store.steampowered.com/api/appdetails'
STEAMSPY_API_URL = 'https://steamspy.com/api.php'
STEAM_API_KEY = os.getenv('STEAM_API_KEY')

# --- HELPER FUNCTIONS ---

def log(message):
    """Prints a formatted log message and forces an immediate flush."""
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)

def create_kafka_producer():
    """Creates a KafkaProducer with robust retry logic."""
    log(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    for i in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version_auto_timeout_ms=15000
            )
            log("SUCCESS: Kafka producer connected.")
            return producer
        except NoBrokersAvailable:
            log(f"WARNING: Kafka not ready. Retrying in {5 * (i+1)}s...")
            time.sleep(5 * (i+1))
    log("CRITICAL: Could not connect to Kafka."); return None

def create_redis_client():
    """Creates a Redis client with robust retry logic."""
    log(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    for i in range(5):
        try:
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
            client.ping()
            log("SUCCESS: Redis client connected.")
            return client
        except Exception as e:
            log(f"WARNING: Redis not ready: {e}. Retrying in {5 * (i+1)}s...")
            time.sleep(5 * (i+1))
    log("CRITICAL: Could not connect to Redis."); return None

def do_request(url, params=None, retries=3, initial_sleep=2):
    """Makes a web request with exponential backoff."""
    sleep_time = initial_sleep
    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200: return response.json()
            elif response.status_code == 429: log(f"  - WARNING: Rate limited (429). Retrying in {sleep_time}s...")
        except requests.exceptions.RequestException: pass
        time.sleep(sleep_time); sleep_time *= 2
    return None

def check_for_new_apps(redis_client):
    """
    Fetches the full app list using the new IStoreService API (paginated),
    compares it against Redis, and adds new discoveries to the queue.
    """
    log("Checking for newly released Steam apps (using IStoreService)...")
    
    if not STEAM_API_KEY:
        log("  - ERROR: STEAM_API_KEY is required for the new AppList API.")
        return

    current_master_ids = set()
    last_appid = 0
    more_results = True
    
    try:
        while more_results:
            params = {
                'key': STEAM_API_KEY,
                'max_results': 50000, # Request max allowed size
                'last_appid': last_appid,
                "include_games": "true", 
                "include_dlc": "true",     # Set to 'true' if you want DLCs
                "include_software": "false",
                "include_hardware": "false"
            }
            
            response = requests.get(STEAM_APP_LIST_URL, params=params, timeout=30)
            
            if response.status_code != 200:
                log(f"  - ERROR: Failed to fetch app list page. Status: {response.status_code}")
                break
                
            data = response.json().get('response', {})
            apps_batch = data.get('apps', [])
            
            if not apps_batch:
                more_results = False
                break
                
            for app in apps_batch:
                current_master_ids.add(str(app['appid']))
            
            # Update cursor for next page
            last_appid = data.get('last_appid')
            
            # If no last_appid is returned, we are done
            if not last_appid:
                more_results = False
            
            # Respect rate limits between pages
            time.sleep(1)

        log(f"  - Steam master list fetch complete. Found {len(current_master_ids)} apps.")
        
        # --- Comparison Logic (Same as before) ---
        if current_master_ids:
            known_ids = redis_client.sunion(REDIS_KEY_TO_SCRAPE, REDIS_KEY_SCRAPED, REDIS_KEY_DISCARDED)
            new_app_ids = current_master_ids - known_ids
            
            if new_app_ids:
                log(f"  - Found {len(new_app_ids)} new apps! Adding them to the scrape queue.")
                redis_client.sadd(REDIS_KEY_TO_SCRAPE, *new_app_ids)
            else:
                log("  - No new apps found.")
    
    except Exception as e:
        log(f"  - ERROR: An unexpected error occurred while checking for new apps: {e}")

# --- MAIN APPLICATION LOGIC ---

def main():
    """The main entry point for the long-running producer service."""
    producer = create_kafka_producer()
    redis_client = create_redis_client()
    if not producer or not redis_client:
        sys.exit(1)

    log("--- Game Catalog Producer Started (Write-Once Mode) ---")
    cycle_count = 0
    
    while True:
        try:
            cycle_count += 1
            log(f"\n--- Starting Cycle #{cycle_count} ---")
            
            # Periodically check for brand new apps to add to the work queue
            if cycle_count % CHECK_FOR_NEW_APPS_CYCLE == 1:
                check_for_new_apps(redis_client)

            log(f"Fetching a batch of up to {BATCH_SIZE} apps to scrape from Redis...")
            appids_to_process = redis_client.spop(REDIS_KEY_TO_SCRAPE, BATCH_SIZE)
            
            if not appids_to_process:
                log("Work queue is empty. Nothing to process in this cycle.")
                log(f"Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            
            log(f"Processing a batch of {len(appids_to_process)} apps.")
            successful_scrapes = []
            discarded_in_batch = []

            for enum_id,appid in enumerate(appids_to_process):
                steam_raw = do_request(STEAM_APP_DETAILS_URL, {'appids': appid, 'cc': 'us', 'l': 'en'})
                
                if not steam_raw or not steam_raw.get(appid, {}).get('success'):
                    discarded_in_batch.append(appid)
                    continue
                
                app_data = steam_raw[appid]['data']
                
                steamspy_raw = do_request(STEAMSPY_API_URL, {'request': 'appdetails', 'appid': appid})
                if steamspy_raw:
                    for key, value in steamspy_raw.items():
                        app_data[f'steamspy_{key}'] = value
                
                # The message is an object with the appid as the top-level key,
                # exactly matching the format of the .jsonl backfill file.
                message = {appid: app_data}
                
                producer.send(KAFKA_TOPIC, key=appid.encode('utf-8'), value=message)
                successful_scrapes.append(appid)
                time.sleep(API_SLEEP_SECONDS)
                if enum_id%10==0:
                    log(f"Finished {enum_id} app")

            # Update Redis state after the batch is processed
            if successful_scrapes:
                log(f"Successfully scraped and published {len(successful_scrapes)} games.")
                redis_client.sadd(REDIS_KEY_SCRAPED, *successful_scrapes)
            if discarded_in_batch:
                log(f"Discarded {len(discarded_in_batch)} apps to the permanent discard list.")
                redis_client.sadd(REDIS_KEY_DISCARDED, *discarded_in_batch)
            
            producer.flush()
            log(f"Cycle complete. Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(POLL_INTERVAL_SECONDS)

        except (KeyboardInterrupt, SystemExit):
            log("Shutdown signal received. Exiting.")
            break
        except Exception as e:
            log(f"CRITICAL: An unexpected error occurred in the main loop: {e}")
            log("Restarting loop after a 60-second delay...")
            time.sleep(60)
            
    if producer:
        log("Closing Kafka producer...")
        producer.close()

if __name__ == "__main__":
    main()