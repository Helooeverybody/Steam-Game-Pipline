# File: scrape_all_apps_raw.py
import requests
import json
import time
import argparse
import sys
import os
import datetime as dt

# --- CONFIGURATION ---
APPS_FILE = 'steam_apps_dataset_raw.json'
APP_LIST_FILE = 'applist.cache.json'
FAILED_FILE = 'failed_apps.json' # For apps that fail API calls completely

# --- API ENDPOINTS ---
STEAM_APP_LIST_URL = 'https://api.steampowered.com/ISteamApps/GetAppList/v2/'
STEAM_APP_DETAILS_URL = 'https://store.steampowered.com/api/appdetails'
STEAMSPY_API_URL = 'https://steamspy.com/api.php'

def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def progress_bar(title, count, total):
    bar_len = 70
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 2)
    bar = '█' * filled_len + '░' * (bar_len - filled_len)
    sys.stdout.write(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {title} {bar} {percents}% ({count}/{total})\r")
    sys.stdout.flush()

def save_json(data, filename):
    try:
        if os.path.exists(filename):
            os.replace(filename, filename + '.bak')
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        log(f"CRITICAL: Failed to save JSON file '{filename}'. Error: {e}")

def load_json(filename):
    if not os.path.exists(filename): return None
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError): return None

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
    cached_apps = load_json(APP_LIST_FILE)
    if cached_apps:
        log(f"Loaded {len(cached_apps)} app IDs from cache.")
        return cached_apps
    log("Fetching master app list from Steam API...")
    data = do_request(STEAM_APP_LIST_URL)
    if data and 'applist' in data and 'apps' in data['applist']:
        app_ids = {str(app['appid']) for app in data['applist']['apps']}
        log(f"Successfully fetched and cached {len(app_ids)} app IDs.")
        save_json(list(app_ids), APP_LIST_FILE)
        return list(app_ids)
    else:
        log("CRITICAL: Failed to fetch master app list."); sys.exit(1)

# --- MAIN SCRAPING LOGIC ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A robust scraper for RAW Steam application data.')
    parser.add_argument('-s', '--sleep', type=float, default=1.5, help='Base wait time between requests.')
    parser.add_argument('-a', '--autosave', type=int, default=100, help='Save progress every N new apps.')
    args = parser.parse_args()

    log("--- Steam App Data Scraper (Raw) ---")

    dataset = load_json(APPS_FILE) or {}
    failed_ids = set(load_json(FAILED_FILE) or [])
    
    log(f"Loaded {len(dataset)} existing apps.")
    log(f"Loaded {len(failed_ids)} failed app IDs.")
    
    all_appids = get_all_steam_appids()
    appids_to_scrape = [aid for aid in all_appids if aid not in dataset and aid not in failed_ids]
    
    log(f"Starting scrape for {len(appids_to_scrape)} new app IDs.")
    
    new_apps_count = 0
    total_to_scrape = len(appids_to_scrape)
    
    try:
        for i, appid in enumerate(appids_to_scrape):
            progress_bar("Scraping Apps", i + 1, total_to_scrape)
            
            # 1. Get Raw Data from Steam
            steam_raw = do_request(STEAM_APP_DETAILS_URL, {'appids': appid, 'cc': 'us', 'l': 'en'})
            
            # If the request fails or API reports failure, mark as failed and skip
            if not steam_raw or appid not in steam_raw or not steam_raw[appid].get('success'):
                failed_ids.add(appid)
                continue

            # This is the raw data for the specific app
            app_data = steam_raw[appid]['data']

            # 2. Get Raw Data from SteamSpy
            steamspy_raw = do_request(STEAMSPY_API_URL, {'request': 'appdetails', 'appid': appid})

            # 3. Merge SteamSpy data into the main object
            # This adds all fields from SteamSpy with a 'steamspy_' prefix to avoid collisions
            if steamspy_raw:
                for key, value in steamspy_raw.items():
                    app_data[f'steamspy_{key}'] = value
            
            # 4. Save the combined raw object to our main dataset
            dataset[appid] = app_data
            new_apps_count += 1
            
            # 5. Autosave
            if args.autosave > 0 and new_apps_count > 0 and new_apps_count % args.autosave == 0:
                log(f"\nAutosaving progress: {len(dataset)} total apps found...")
                save_json(dataset, APPS_FILE)
                save_json(list(failed_ids), FAILED_FILE)
            
            time.sleep(args.sleep)
            
    except (KeyboardInterrupt, SystemExit):
        log("\nInterruption detected. Performing final save...")
    finally:
        log("\nScraping finished or interrupted. Saving all data...")
        save_json(dataset, APPS_FILE)
        save_json(list(failed_ids), FAILED_FILE)
        log(f"--- COMPLETE ---")