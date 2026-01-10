# File: scrape_all_apps_memory_safe.py
import requests
import json
import time
import argparse
import sys
import os
import datetime as dt

# --- CONFIGURATION (relative to repository root - run scripts from repo root) ---
APPS_FILE_JSONL = 'data/raw/steam_apps_dataset_raw.jsonl'
APP_LIST_FILE = 'data/cache/applist.cache.json'
FAILED_FILE = 'data/state/failed_apps.json'

# --- API ENDPOINTS ---
STEAM_APP_LIST_URL = 'https://api.steampowered.com/ISteamApps/GetAppList/v2/'
STEAM_APP_DETAILS_URL = 'https://store.steampowered.com/api/appdetails'
STEAMSPY_API_URL = 'https://steamspy.com/api.php'

def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def progress_bar(title, count, total, scraped_count):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 2)
    bar = '█' * filled_len + '░' * (bar_len - filled_len)
    sys.stdout.write(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {title} {bar} {percents}% ({count}/{total}) | Total Scraped: {scraped_count}\r")
    sys.stdout.flush()

def save_json_list(data, filename):
    try:
        if os.path.exists(filename):
            os.replace(filename, filename + '.bak')
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(list(data), f, indent=4, ensure_ascii=False)
    except Exception as e:
        log(f"CRITICAL: Failed to save JSON file '{filename}'. Error: {e}")
        
def load_json_list(filename):
    if not os.path.exists(filename): return set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    except (json.JSONDecodeError, IOError): return set()

def do_request(url, params=None, retries=5, initial_sleep=2):
    sleep_time = initial_sleep
    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200: return response.json()
            elif response.status_code == 429: log(f"WARNING: Rate limited (429). Retrying in {sleep_time}s...")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            log(f"WARNING: Request failed: {e}. Retrying in {sleep_time}s...")
        time.sleep(sleep_time)
        sleep_time *= 2
    log(f"ERROR: Request failed after {retries} retries for URL: {url}")
    return None

def get_all_steam_appids():
    cached_apps = load_json_list(APP_LIST_FILE)
    if cached_apps:
        log(f"Loaded {len(cached_apps)} app IDs from cache.")
        return list(cached_apps)
    log("Fetching master app list from Steam API...")
    data = do_request(STEAM_APP_LIST_URL)
    if data and 'applist' in data and 'apps' in data['applist']:
        app_ids = {str(app['appid']) for app in data['applist']['apps']}
        log(f"Successfully fetched and cached {len(app_ids)} app IDs.")
        save_json_list(app_ids, APP_LIST_FILE)
        return list(app_ids)
    else:
        log("CRITICAL: Failed to fetch master app list."); sys.exit(1)

def get_scraped_appids_from_jsonl(filename):
    scraped_ids = set()
    if not os.path.exists(filename):
        return scraped_ids
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                app_id = list(json.loads(line).keys())[0]
                scraped_ids.add(app_id)
            except (json.JSONDecodeError, IndexError):
                continue
    return scraped_ids

# --- MAIN SCRAPING LOGIC ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A memory-safe scraper for RAW Steam application data with batch writes.')
    parser.add_argument('-s', '--sleep', type=float, default=1.5, help='Base wait time between requests.')
    # Add a new argument for the batch size
    parser.add_argument('-b', '--batchsize', type=int, default=500, help='Number of records to batch before writing to file.')
    args = parser.parse_args()

    log("--- Steam App Data Scraper (Memory-Safe, JSONL with Batch Writes) ---")

    scraped_ids = get_scraped_appids_from_jsonl(APPS_FILE_JSONL)
    failed_ids = load_json_list(FAILED_FILE)
    
    log(f"Loaded {len(scraped_ids)} existing apps from '{APPS_FILE_JSONL}'.")
    log(f"Loaded {len(failed_ids)} failed app IDs from '{FAILED_FILE}'.")
    
    all_appids = get_all_steam_appids()
    appids_to_scrape = [aid for aid in all_appids if aid not in scraped_ids and aid not in failed_ids]
    
    log(f"Starting scrape for {len(appids_to_scrape)} new app IDs.")
    
    total_to_scrape = len(appids_to_scrape)
    total_scraped_ever = len(scraped_ids)
    
    # --- BATCHING LOGIC ---
    # This list will temporarily hold records before they are written to the file
    write_buffer = []

    try:
        # Open the output file in append mode. It will stay open during the run.
        with open(APPS_FILE_JSONL, 'a', encoding='utf-8') as f_out:
            for i, appid in enumerate(appids_to_scrape):
                progress_bar("Scraping Apps", i + 1, total_to_scrape, total_scraped_ever)
                
                steam_raw = do_request(STEAM_APP_DETAILS_URL, {'appids': appid, 'cc': 'us', 'l': 'en'})
                
                if not steam_raw or appid not in steam_raw or not steam_raw[appid].get('success'):
                    failed_ids.add(appid)
                    continue

                app_data = steam_raw[appid]['data']
                steamspy_raw = do_request(STEAMSPY_API_URL, {'request': 'appdetails', 'appid': appid})

                if steamspy_raw:
                    for key, value in steamspy_raw.items():
                        app_data[f'steamspy_{key}'] = value
                
                output_object = {appid: app_data}
                
                # Add the new object to our temporary buffer
                write_buffer.append(json.dumps(output_object))
                
                # --- KEY CHANGE: WRITE THE BATCH ---
                # If the buffer is full, write all its contents to the file and clear it.
                if len(write_buffer) >= args.batchsize:
                    log(f"\nWriting a batch of {len(write_buffer)} records to disk...")
                    f_out.write('\n'.join(write_buffer) + '\n')
                    write_buffer.clear() # Clear the buffer to free memory
                    f_out.flush() # Ensure data is written to disk immediately
                # --- END OF KEY CHANGE ---
                
                total_scraped_ever += 1
                time.sleep(args.sleep)

    except (KeyboardInterrupt, SystemExit):
        log("\nInterruption detected. Saving any remaining records in buffer...")
    finally:
        # --- KEY CHANGE: FINAL WRITE ---
        # Before exiting, make sure to write any remaining records in the buffer
        # that didn't make a full batch.
        if write_buffer:
            log(f"\nWriting final batch of {len(write_buffer)} records to disk...")
            # Re-open the file in append mode just in case it was closed
            with open(APPS_FILE_JSONL, 'a', encoding='utf-8') as f_out_final:
                f_out_final.write('\n'.join(write_buffer) + '\n')
                write_buffer.clear()
        # --- END OF KEY CHANGE ---

        log("\nSaving final list of failed IDs...")
        save_json_list(failed_ids, FAILED_FILE)
        log(f"--- COMPLETE ---")
        log(f"Total apps in dataset: {total_scraped_ever}")
        log(f"Total failed apps: {len(failed_ids)}")