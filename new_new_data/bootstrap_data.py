import json
import os
import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError, UnknownTopicOrPartitionError

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = '100.111.128.57:32565'

# Data Paths
GAMES_FILE_PATH = 'top_10k_games_by_owners.jsonl'  # Filtered top 10k games by owners
REVIEWS_DIR_PATH = 'top_10_reviews_raw'
PLAYER_COUNTS_FILE_PATH = 'historical_player_counts_monthly.jsonl'

# Topic Names & Configs
# We define configs here to ensure they are recreated correctly
TOPIC_CONFIGS = [
    NewTopic(name='steam-games-raw', num_partitions=3, replication_factor=1, topic_configs={'retention.ms': '-1'}),
    NewTopic(name='steam-reviews-raw', num_partitions=10, replication_factor=1, topic_configs={'retention.ms': '-1'}),
    NewTopic(name='steam-player-counts-raw', num_partitions=3, replication_factor=1, topic_configs={'retention.ms': '-1'})
]

# Topic References
GAMES_TOPIC = 'steam-games-raw'
REVIEWS_TOPIC = 'steam-reviews-raw'
PLAYER_COUNTS_TOPIC = 'steam-player-counts-raw'

def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

# --- PART 1: RESET LOGIC ---

def reset_topics():
    """
    Deletes existing topics to clear old data and re-creates them.
    Handles potential conflicts with Strimzi operator automatically.
    """
    log("--- 1. Resetting Kafka Topics ---")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        
        topic_names = [t.name for t in TOPIC_CONFIGS]
        
        # A. Delete
        log(f"Deleting topics: {topic_names}")
        try:
            admin_client.delete_topics(topics=topic_names)
            log("Deletion requested. Waiting 5 seconds for cleanup...")
            time.sleep(5) 
        except UnknownTopicOrPartitionError:
            log("Topics did not exist yet. Proceeding.")
        except Exception as e:
            log(f"Deletion warning: {e}")

        # B. Create
        log("Re-creating topics...")
        try:
            admin_client.create_topics(new_topics=TOPIC_CONFIGS)
            log("Topics created successfully.")
        except TopicAlreadyExistsError:
            # This is fine; it means Strimzi Operator might have auto-recreated them 
            # immediately after we deleted them. The data is still gone.
            log("Topics already exist (Strimzi may have auto-recreated them). Data is cleared.")
        
        admin_client.close()
        time.sleep(2) # Allow metadata to propagate
        
    except Exception as e:
        log(f"CRITICAL ERROR during topic reset: {e}")
        log("Please check your connection or Kafka cluster status.")
        exit(1)

# --- PART 2: PRODUCER LOGIC ---

def create_producer():
    log(f"Connecting Producer to {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version_auto_timeout_ms=15000,
            request_timeout_ms=30000,
            batch_size=131072, 
            linger_ms=10       
        )
    except Exception as e:
        log(f"CRITICAL: Connection failed: {e}")
        return None

def publish_file_stream(producer, topic, filepath, key_name):
    """Streaming read for Game Catalog (No Sort)."""
    log(f"--- Streaming '{filepath}' to '{topic}' ---")
    if not os.path.exists(filepath):
        log(f"WARNING: File '{filepath}' not found.")
        return

    count = 0
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            try:
                data = json.loads(line)
                
                # Extract Key
                key = None
                if key_name == 'appid' and len(data.keys()) == 1:
                     key_val = list(data.keys())[0]
                else:
                     key_val = data.get(key_name)
                
                if key_val: key = str(key_val).encode('utf-8')

                producer.send(topic, key=key, value=data)
                count += 1
                
                if count % 2000 == 0: log(f"    - Streamed {count} records...")
                    
            except json.JSONDecodeError: continue
    
    producer.flush()
    log(f"  - Finished. Streamed {count} records.")

def publish_file_sorted(producer, topic, filepath, key_name, sort_key, app_id=None, limit=None):
    """Buffered read + Sort for Time Series Data."""
    filename = os.path.basename(filepath)
    log(f"--- Processing '{filename}' for '{topic}' ---")
    
    if not os.path.exists(filepath):
        log(f"WARNING: File '{filepath}' not found.")
        return

    data_buffer = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    data = json.loads(line)
                    # Inject App ID if provided (Consistency Fix)
                    if app_id: data['app_id'] = app_id
                    data_buffer.append(data)
                except json.JSONDecodeError: continue

    if not data_buffer: return

    # Sort Oldest -> Newest
    data_buffer.sort(key=lambda x: x.get(sort_key, 0))
    
    # Limit to most recent records if specified
    original_count = len(data_buffer)
    if limit and len(data_buffer) > limit:
        data_buffer = data_buffer[-limit:]  # Keep last N (most recent)
        log(f"  - Limited from {original_count} to {limit} most recent records.")

    count = 0
    for data in data_buffer:
        key = None
        if key_name:
             key_val = data.get(key_name)
             if key_val: key = str(key_val).encode('utf-8')

        producer.send(topic, key=key, value=data)
        count += 1
    
    producer.flush()
    log(f"  - Finished. Sorted and sent {count} records.")

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    
    # 1. RESET (Delete old data)
    reset_topics()
    
    # 2. CONNECT
    kafka_producer = create_producer()
    if not kafka_producer:
        sys.exit(1)
    
    log("\n--- 2. Starting Data Bootstrap ---")

    # 3. GAME CATALOG (Streamed, Unsorted)
    publish_file_stream(
        kafka_producer, 
        GAMES_TOPIC, 
        GAMES_FILE_PATH, 
        key_name='appid'
    )
    
    # 4. REVIEWS (Buffered, Sorted, AppID Injected)
    if os.path.exists(REVIEWS_DIR_PATH):
        files = sorted([f for f in os.listdir(REVIEWS_DIR_PATH) if f.endswith(".jsonl")])
        for i, filename in enumerate(files):
            try:
                # Extract App ID from filename (e.g. "730.jsonl" -> 730)
                current_app_id = int(filename.split('.')[0])
                
                publish_file_sorted(
                    kafka_producer, 
                    REVIEWS_TOPIC, 
                    os.path.join(REVIEWS_DIR_PATH, filename), 
                    key_name='recommendationid',
                    sort_key='timestamp_created',
                    app_id=current_app_id,
                    limit=50000  # Limit to 50k most recent reviews per game
                )
            except ValueError:
                log(f"Skipping invalid filename: {filename}")
    else:
        log(f"WARNING: Directory '{REVIEWS_DIR_PATH}' not found.")

    # 5. PLAYER COUNTS (Buffered, Sorted)
    publish_file_sorted(
        kafka_producer, 
        PLAYER_COUNTS_TOPIC, 
        PLAYER_COUNTS_FILE_PATH, 
        key_name='app_id',
        sort_key='event_timestamp',
        app_id=None # Already inside the data
    )
    
    kafka_producer.close()
    log("\n--- COMPLETE: Topics Reset & Data Bootstrapped ---")