# File: bootstrap_redis.py
import os
import json
import time
import datetime as dt
import redis
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# Local files containing the backfill data
GAMES_FILE_PATH = 'steam_apps_dataset_raw.jsonl' # For prices
REVIEWS_DIR_PATH = 'top_10_reviews_raw'      # For review IDs

def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def create_redis_client():
    """Creates and returns a Redis client, handling authentication."""
    log(f"Attempting to connect to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True # Important: decodes keys/values from bytes to strings
        )
        client.ping() # Verify the connection
        log("SUCCESS: Redis client connected and authenticated.")
        return client
    except redis.exceptions.AuthenticationError:
        log("CRITICAL: Redis authentication failed. Please check your REDIS_PASSWORD in the .env file.")
        return None
    except Exception as e:
        log(f"CRITICAL: Could not connect to Redis: {e}")
        return None

def bootstrap_review_state(redis_client):
    """
    Populates Redis with the last 100 seen review IDs for each game.
    This gives the LiveReviewProducer a baseline to check against.
    """
    log("\n--- Bootstrapping State for Live Review Producer ---")
    if not os.path.exists(REVIEWS_DIR_PATH):
        log(f"WARNING: Directory '{REVIEWS_DIR_PATH}' not found. Skipping review state bootstrap.")
        return

    for filename in os.listdir(REVIEWS_DIR_PATH):
        if filename.endswith(".jsonl"):
            appid = filename.split('.')[0]
            filepath = os.path.join(REVIEWS_DIR_PATH, filename)
            log(f"  - Processing reviews for App ID: {appid}")

            try:
                # Efficiently read the last ~150 lines to be safe
                with open(filepath, 'r', encoding='utf-8') as f:
                    # In a real-world scenario with huge files, a more memory-efficient
                    # method might be needed, but this is fine for project-scale files.
                    all_reviews = [json.loads(line) for line in f]
                
                # Get the most recent 100 reviews based on creation time
                all_reviews.sort(key=lambda r: r.get('timestamp_created', 0), reverse=True)
                print(len(all_reviews))
                most_recent_ids = {str(r['recommendationid']) for r in all_reviews[:100]}
                
                if most_recent_ids:
                    redis_key = f"steam:reviews:{appid}:recent_ids"
                    
                    # Clean up old state and set the new one
                    redis_client.delete(redis_key)
                    redis_client.sadd(redis_key, *most_recent_ids)
                    redis_client.expire(redis_key, 3600) # Expire in 1 hour if not updated
                    log(f"    - Set {len(most_recent_ids)} recent review IDs in Redis for key '{redis_key}'.")

            except Exception as e:
                log(f"    - ERROR: Failed to process file {filename}: {e}")




if __name__ == "__main__":
    redis_client = create_redis_client()
    if not redis_client:
        log("Exiting due to Redis connection failure.")
        sys.exit(1)
        
    # Run the bootstrap functions
    bootstrap_review_state(redis_client)
    
    log("\n--- REDIS BOOTSTRAP COMPLETE ---")
    log("Initial state for producers has been populated.")