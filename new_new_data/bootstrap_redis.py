import os
import json
import time
import datetime as dt
import redis
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# Paths to your local backfill data
GAMES_FILE_PATH = 'steam_apps_dataset_raw.jsonl'
REVIEWS_DIR_PATH = 'top_10_reviews_raw'

# Redis Keys (Must match what is in your Producer scripts)
REDIS_KEY_SCRAPED = 'steam:apps:scraped'

def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def create_redis_client():
    log(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        client.ping()
        log("SUCCESS: Redis client connected.")
        return client
    except Exception as e:
        log(f"CRITICAL: Could not connect to Redis: {e}")
        return None

# --- PART 1: GAME CATALOG BOOTSTRAP ---
def bootstrap_game_catalog(redis_client):
    """
    Reads the raw games JSONL file and marks all those App IDs as 'scraped' in Redis.
    This prevents the GameCatalogProducer from re-scraping them on startup.
    """
    log("\n--- 1. Bootstrapping Game Catalog State ---")
    if not os.path.exists(GAMES_FILE_PATH):
        log(f"WARNING: File '{GAMES_FILE_PATH}' not found. Skipping.")
        return

    # Use a pipeline for bulk insertion (much faster)
    pipe = redis_client.pipeline()
    count = 0
    
    with open(GAMES_FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                # Format is {"730": {...}}
                data = json.loads(line)
                appid = list(data.keys())[0]
                
                # Add to the 'scraped' set
                pipe.sadd(REDIS_KEY_SCRAPED, appid)
                count += 1
                
                if count % 10000 == 0:
                    pipe.execute() # Execute batch
                    pipe = redis_client.pipeline() # Start new batch
                    log(f"  - Processed {count} apps...")
            except (json.JSONDecodeError, IndexError):
                continue

    pipe.execute() # Final flush
    log(f"SUCCESS: Marked {count} apps as 'scraped' in Redis.")

# --- PART 2: REVIEW STATE BOOTSTRAP ---
def bootstrap_review_state(redis_client):
    """
    Reads the review JSONL files and saves the IDs of the most recent reviews.
    This prevents the LiveReviewProducer from re-sending the last batch of reviews.
    """
    log("\n--- 2. Bootstrapping Live Review State ---")
    if not os.path.exists(REVIEWS_DIR_PATH):
        log(f"WARNING: Directory '{REVIEWS_DIR_PATH}' not found. Skipping.")
        return

    files = [f for f in os.listdir(REVIEWS_DIR_PATH) if f.endswith(".jsonl")]
    
    for filename in files:
        appid = filename.split('.')[0]
        filepath = os.path.join(REVIEWS_DIR_PATH, filename)
        
        try:
            # We only need the most recent reviews to set the deduplication cache.
            # Reading the whole file is fine for this scale, or use `tail` logic for huge files.
            all_reviews = []
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        all_reviews.append(json.loads(line))
                    except: continue

            if not all_reviews:
                continue

            # Sort by creation time (Newest first)
            all_reviews.sort(key=lambda r: r.get('timestamp_created', 0), reverse=True)
            
            # Take the top 150 IDs (slightly more than the poll batch size of 100)
            recent_ids = {str(r['recommendationid']) for r in all_reviews[:150]}
            
            if recent_ids:
                redis_key = f"steam:reviews:{appid}:recent_ids"
                
                # Reset the key to ensure it's fresh
                redis_client.delete(redis_key)
                redis_client.sadd(redis_key, *recent_ids)
                # Set expiry to 2 hours (live producer will refresh this constantly)
                redis_client.expire(redis_key, 7200) 
                
                log(f"  - App ID {appid}: Cached {len(recent_ids)} recent review IDs.")

        except Exception as e:
            log(f"  - ERROR processing {filename}: {e}")

if __name__ == "__main__":
    redis_client = create_redis_client()
    if not redis_client:
        sys.exit(1)
        
    bootstrap_game_catalog(redis_client)
    bootstrap_review_state(redis_client)
    
    redis_client.close()
    log("\n--- REDIS BOOTSTRAP COMPLETE ---")