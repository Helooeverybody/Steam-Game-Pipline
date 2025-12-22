import json
import os
import time
import argparse
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- CONFIGURATION ---
# Your k3s external address
KAFKA_BOOTSTRAP_SERVERS = '100.120.97.104:32565'

# Paths to your local backfill data
GAMES_FILE_PATH = 'steam_apps_dataset_raw.jsonl'
REVIEWS_DIR_PATH = 'top_10_reviews_raw'
PLAYER_COUNTS_FILE_PATH = 'historical_player_counts_monthly.jsonl'

# Kafka Topics
GAMES_TOPIC = 'steam-games-raw'
REVIEWS_TOPIC = 'steam-reviews-raw'
PLAYER_COUNTS_TOPIC = 'steam-player-counts-raw'

def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def create_producer():
    """Creates a KafkaProducer with robust connection settings."""
    log(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # High timeout is useful for initial connections or slow networks
            api_version_auto_timeout_ms=15000,
            request_timeout_ms=30000
        )
        log("Successfully connected to Kafka.")
        return producer
    except Exception as e:
        log(f"CRITICAL: Could not connect to Kafka: {e}")
        return None

def publish_file_sorted(producer, topic, filepath, key_name, sort_key=None):
    """
    Reads a file, optionally SORTS it by a timestamp key (Oldest -> Newest),
    and publishes messages to Kafka.
    """
    log(f"--- Processing '{filepath}' for topic '{topic}' ---")
    
    if not os.path.exists(filepath):
        log(f"WARNING: File '{filepath}' not found. Skipping.")
        return

    data_buffer = []

    # 1. Read all data into memory
    # (Note: For massive files >10GB, you might need chunking, but for this project RAM is usually fine)
    log("  - Reading file into memory...")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue # Skip empty lines
                try:
                    data_buffer.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        log(f"  - ERROR reading file: {e}")
        return

    if not data_buffer:
        log("  - File was empty. Skipping.")
        return

    # 2. SORT by timestamp (if sort_key is provided)
    # This ensures we send Oldest -> Newest so Spark Watermarks don't drop data.
    if sort_key:
        log(f"  - Sorting {len(data_buffer)} records by '{sort_key}' (Oldest -> Newest)...")
        try:
            # We use .get(key, 0) or '' to handle missing keys gracefully without crashing
            data_buffer.sort(key=lambda x: x.get(sort_key, 0))
        except Exception as e:
            log(f"  - WARNING: Sorting failed ({e}). Sending in original order.")
    else:
        log(f"  - No sort key provided. Sending {len(data_buffer)} records in original order.")

    # 3. Publish to Kafka
    log("  - Publishing to Kafka...")
    count = 0
    for data in data_buffer:
        key = None
        
        # Logic to extract the partition key (e.g., App ID)
        if key_name:
            # Special case for Game Catalog format: {"730": {...data...}}
            if key_name == 'appid' and len(data.keys()) == 1:
                 # The key is the first (and only) key in the dictionary
                 key_val = list(data.keys())[0]
            else:
                 # Standard format: {"app_id": "730", ...}
                 key_val = data.get(key_name)
            
            if key_val:
                key = str(key_val).encode('utf-8')

        producer.send(topic, key=key, value=data)
        
        count += 1
        if count % 5000 == 0:
            log(f"    - Sent {count}...")
            producer.flush() # Flush periodically to free up buffer

    producer.flush() # Final flush to ensure all messages are sent
    log(f"  - Finished. Published {count} records.")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if not kafka_producer:
        sys.exit(1)
    
    # ---------------------------------------------------------
    # 1. Publish Game Catalog (Snapshot Data - Order doesn't matter)
    # ---------------------------------------------------------
    log("\n=== STEP 1: Game Catalog ===")
    publish_file_sorted(
        kafka_producer, 
        GAMES_TOPIC, 
        GAMES_FILE_PATH, 
        key_name='appid', 
        sort_key=None # No sorting needed for snapshots
    )
    
    # ---------------------------------------------------------
    # 2. Publish Review Data (MUST SORT by timestamp_created)
    # ---------------------------------------------------------
    log("\n=== STEP 2: Reviews ===")
    if os.path.exists(REVIEWS_DIR_PATH):
        # Sort filenames to be tidy, though not strictly necessary
        files = sorted([f for f in os.listdir(REVIEWS_DIR_PATH) if f.endswith(".jsonl")])
        
        for i, filename in enumerate(files):
            log(f"Processing Review File {i+1}/{len(files)}: {filename}")
            publish_file_sorted(
                kafka_producer, 
                REVIEWS_TOPIC, 
                os.path.join(REVIEWS_DIR_PATH, filename), 
                key_name='recommendationid',
                sort_key='timestamp_created' # <--- CRITICAL: Sort chronological
            )
    else:
        log(f"WARNING: Directory '{REVIEWS_DIR_PATH}' not found.")

    # ---------------------------------------------------------
    # 3. Publish Player Count Data (MUST SORT by event_timestamp)
    # ---------------------------------------------------------
    log("\n=== STEP 3: Player Counts ===")
    publish_file_sorted(
        kafka_producer, 
        PLAYER_COUNTS_TOPIC, 
        PLAYER_COUNTS_FILE_PATH, 
        key_name='app_id',
        sort_key='event_timestamp' # <--- CRITICAL: Sort chronological
    )
    
    kafka_producer.close()
    log("\n--- KAFKA BOOTSTRAP COMPLETE ---")