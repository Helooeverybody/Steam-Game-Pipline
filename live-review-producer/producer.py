# File: live-review-producer/producer.py
import os
import time
import json
import requests
import datetime as dt
import redis
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
POLL_INTERVAL_SECONDS = 60 # Poll every minute

KAFKA_TOPIC = 'steam-reviews-raw'
TARGET_APP_IDS = [
    "578080", "2358720", "1623730", "730", "2246340",
    "1599340", "570", "1091500", "1245620"
]

STEAM_REVIEWS_URL = 'https://store.steampowered.com/appreviews/{}'

def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version_auto_timeout_ms=15000
        )
        log("SUCCESS: Kafka producer connected.")
        return producer
    except Exception as e:
        log(f"CRITICAL: Could not connect to Kafka: {e}"); return None

def create_redis_client():
    try:
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
        client.ping()
        log("SUCCESS: Redis client connected.")
        return client
    except Exception as e:
        log(f"CRITICAL: Could not connect to Redis: {e}"); return None

def main():
    producer = create_kafka_producer()
    redis_client = create_redis_client()
    if not producer or not redis_client: return

    log(f"--- Live Review Producer Started ---")
    
    while True:
        try:
            log("Starting new review polling cycle...")
            for appid in TARGET_APP_IDS:
                params = {'json': 1, 'language': 'english', 'num_per_page': 100, 'filter': 'recent'}
                response = requests.get(STEAM_REVIEWS_URL.format(appid), params=params, timeout=10)

                if response.status_code != 200 or response.json().get('success') != 1: continue

                latest_reviews = response.json().get('reviews', [])
                if not latest_reviews:
                    log(f"  - No recent reviews for App ID {appid}."); continue
                
                current_review_ids = {str(r['recommendationid']) for r in latest_reviews}
                redis_key = f"steam:reviews:{appid}:recent_ids"
                last_seen_ids = redis_client.smembers(redis_key)
                new_review_ids = current_review_ids - last_seen_ids
                
                if new_review_ids:
                    log(f"  - Found {len(new_review_ids)} new review(s) for App ID {appid}!")
                    new_reviews = [r for r in latest_reviews if str(r['recommendationid']) in new_review_ids]
                    for review in new_reviews:
                        review['app_id'] = appid                         
                        message = review
                        producer.send(KAFKA_TOPIC, key=str(review['recommendationid']).encode('utf-8'), value=message)
                    
                    redis_client.delete(redis_key)
                    redis_client.sadd(redis_key, *current_review_ids)
                    redis_client.expire(redis_key, 3600)
                else:
                    log(f"  - No new reviews for App ID {appid}.")

            producer.flush()
            log(f"Cycle complete. Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(POLL_INTERVAL_SECONDS)

        except (KeyboardInterrupt, SystemExit): break
        except Exception as e:
            log(f"CRITICAL: An unexpected error occurred: {e}. Restarting loop after 60s.")
            time.sleep(60)
            
    if producer: producer.close()

if __name__ == "__main__":
    main()