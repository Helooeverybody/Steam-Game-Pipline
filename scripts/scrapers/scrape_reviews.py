# File: scrape_top_10_reviews_resumable.py
import requests
import json
import time
import argparse
import sys
import os
import datetime as dt

# --- CONFIGURATION (relative to repository root - run scripts from repo root) ---
REVIEWS_DIR = 'data/raw/reviews'
# This file stores our scraping progress (the last cursor for each game)
STATE_FILE = 'data/state/scraping_state.json'

TOP_10_APP_IDS = [
    "578080",  # 1. PUBG: BATTLEGROUNDS
    "2358720", # 2. Black Myth: Wukong
    "1623730", # 3. Palworld
    "730",     # 4. Counter-Strike 2
    "2246340", # 5. Monster Hunter Wilds
    "1599340", # 6. Lost Ark
    "570",     # 7. Dota 2
    "1091500", # 8. Cyberpunk 2077
    "1245620", # 9. ELDEN RING
]

# --- API ENDPOINT ---
STEAM_REVIEWS_URL = 'https://store.steampowered.com/appreviews/{}'

# --- UTILITY FUNCTIONS ---
def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def save_json(data, filename):
    try:
        temp_filename = filename + ".tmp"
        with open(temp_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(temp_filename, filename) # Atomic write
    except Exception as e:
        log(f"CRITICAL: Failed to save JSON file '{filename}'. Error: {e}")

def load_json(filename):
    if not os.path.exists(filename): return {} # Return empty dict if not found
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            # Handle case where file is empty
            content = f.read()
            if not content: return {}
            return json.loads(content)
    except (json.JSONDecodeError, IOError):
        log(f"WARNING: Could not parse '{filename}'. Will use a fresh state.")
        return {}

def do_request(url, params=None, retries=5, initial_sleep=2):
    sleep_time = initial_sleep
    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=20)
            if response.status_code == 200: return response.json()
            elif response.status_code == 429: log(f"WARNING: Rate limited (429). Retrying in {sleep_time}s...")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            log(f"WARNING: Request failed: {e}. Retrying in {sleep_time}s...")
        time.sleep(sleep_time)
        sleep_time = min(sleep_time * 2, 60)
    log(f"ERROR: Request failed after {retries} retries for URL: {url}")
    return None

# --- MAIN LOGIC ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A resumable, memory-efficient scraper for raw Steam game review data.')
    parser.add_argument('-l', '--language', type=str, default='english', help='Language to scrape reviews for.')
    args = parser.parse_args()
        
    log(f"--- Top 10 Steam Game Review Scraper (Resumable) ---")
    
    if not os.path.exists(REVIEWS_DIR):
        log(f"Creating output directory: '{REVIEWS_DIR}'")
        os.makedirs(REVIEWS_DIR)
    
    # Load the state file that tracks our progress
    scraping_state = load_json(STATE_FILE)
    
    try:
        for i, appid in enumerate(TOP_10_APP_IDS):
            log(f"\n--- Processing Game {i+1}/{len(TOP_10_APP_IDS)} (App ID: {appid}) ---")
            
            output_filename = os.path.join(REVIEWS_DIR, f"{appid}.jsonl")

            # --- RESUME LOGIC ---
            # Get the starting cursor from our state file. Default to '*' if it's a new game.
            cursor = scraping_state.get(appid, '*')
            
            # Check if the job for this appid is already marked as 'completed'
            if cursor == 'completed':
                log(f"Scraping for App ID {appid} is already marked as complete. Skipping.")
                continue
            # --- END OF RESUME LOGIC ---

            total_reviews = 0
            
            # --- KEY CHANGE: Open file in 'append' mode ---
            with open(output_filename, 'a', encoding='utf-8') as f:
                if cursor == '*':
                     log(f"Starting new scrape for App ID {appid}.")
                else:
                     log(f"Resuming scrape for App ID {appid} from saved cursor.")

                while True:
                    params = {
                        'json': 1,
                        'language': args.language,
                        'filter': 'recent',
                        'num_per_page': 100,
                        'cursor': cursor
                    }

                    data = do_request(STEAM_REVIEWS_URL.format(appid), params)
                    reviews_batch = data.get('reviews') if data and data.get('success') == 1 else None

                    if not reviews_batch:
                        log(f"  - No more reviews found. Marking as complete.")
                        scraping_state[appid] = 'completed'
                        save_json(scraping_state, STATE_FILE)
                        break

                    for review in reviews_batch:
                        f.write(json.dumps(review) + '\n')
                    
                    total_reviews += len(reviews_batch)
                    
                    # --- KEY CHANGE: SAVE STATE AFTER EVERY SUCCESSFUL BATCH ---
                    new_cursor = data.get('cursor')
                    log(f"  - Scraped and saved {len(reviews_batch)} reviews (Total for this session: {total_reviews}). Saving state.")
                    scraping_state[appid] = new_cursor
                    save_json(scraping_state, STATE_FILE)
                    # --- END OF KEY CHANGE ---
                    
                    if not new_cursor:
                        log(f"  - No new cursor returned. Marking as complete.")
                        scraping_state[appid] = 'completed'
                        save_json(scraping_state, STATE_FILE)
                        break

                    cursor = new_cursor
                    time.sleep(1.5)
            
            log(f"Finished session for App ID {appid}.")
            
    except (KeyboardInterrupt, SystemExit):
        log("\nInterruption detected. Progress has been saved. You can restart the script to resume.")

    log("\n--- SCRAPING COMPLETE FOR ALL TARGETS ---")