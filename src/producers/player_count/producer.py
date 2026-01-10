# File: player-count-producer/producer.py
import os
import time
import json
import requests
import datetime as dt
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
STEAM_API_KEY = os.getenv('STEAM_API_KEY')
POLL_INTERVAL_SECONDS = 300 # 5 minutes

KAFKA_TOPIC = 'steam-player-counts-raw'
TARGET_APP_IDS = [
    "578080", "2358720", "1623730", "730", "2246340",
    "1599340", "570", "1091500", "1245620"
]

STEAM_PLAYER_COUNT_URL = 'http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/'

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

def main():
    if not STEAM_API_KEY:
        log("CRITICAL: STEAM_API_KEY env var not set. Exiting."); return

    producer = create_kafka_producer()
    if not producer: return

    log(f"--- Live Player Count Producer Started ---")
    
    while True:
        try:
            log("Starting new polling cycle...")
            for appid in TARGET_APP_IDS:
                params = {'appid': appid, 'key': STEAM_API_KEY}
                response = requests.get(STEAM_PLAYER_COUNT_URL, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json().get('response', {})
                    if data.get('result') == 1 and 'player_count' in data:
                        
                        # --- KEY CHANGE: Match the backfill format ---
                        message = {
                            "event_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                            "app_id": appid,
                            "player_count": data['player_count'],
                            "peak_players_monthly": None # Set to null to match schema
                        }
                        # --- END OF CHANGE ---

                        producer.send(KAFKA_TOPIC, key=appid.encode('utf-8'), value=message)
                        log(f"  - App ID {appid}: {data['player_count']} players. Published to Kafka.")
                else:
                    log(f"  - ERROR: Request failed for App ID {appid} with status {response.status_code}.")
                time.sleep(1)

            producer.flush()
            log(f"Cycle complete. Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(POLL_INTERVAL_SECONDS)

        except (KeyboardInterrupt, SystemExit): break
        except Exception as e:
            log(f"CRITICAL: An unexpected error in main loop: {e}"); time.sleep(60)
    
    if producer: producer.close()

if __name__ == "__main__":
    main()