# File: scrape_all_apps.py
import requests
import json
import time
import argparse
import sys
import os
import datetime as dt

# --- CONFIGURATION ---
# Output file for all successfully scraped app data
APPS_FILE = 'steam_apps_dataset.json'
# Caches the master list of all Steam app IDs
APP_LIST_FILE = 'applist.cache.json'
# Records app IDs that returned an error or failure, to avoid re-checking them
FAILED_FILE = 'failed_apps.json'

# --- API ENDPOINTS ---
STEAM_APP_LIST_URL = 'https://api.steampowered.com/ISteamApps/GetAppList/v2/'
STEAM_APP_DETAILS_URL = 'https://store.steampowered.com/api/appdetails'
STEAMSPY_API_URL = 'https://steamspy.com/api.php'

def log(message):
    """Prints a formatted log message with a timestamp."""
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def progress_bar(title, count, total):
    """Displays and updates a console progress bar."""
    bar_len = 70
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 2)
    bar = '█' * filled_len + '░' * (bar_len - filled_len)
    sys.stdout.write(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {title} {bar} {percents}% ({count}/{total})\r")
    sys.stdout.flush()

def save_json(data, filename):
    """Saves data to a JSON file, creating a backup of the old file."""
    try:
        if os.path.exists(filename):
            name, ext = os.path.splitext(filename)
            os.replace(filename, name + '.bak')
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        log(f"CRITICAL: Failed to save JSON file '{filename}'. Error: {e}")

def load_json(filename):
    """Loads data from a JSON file, returning None if it doesn't exist."""
    if not os.path.exists(filename):
        return None
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log(f"WARNING: Could not load or parse '{filename}'. Will start fresh. Error: {e}")
        return None

def do_request(url, params=None, retries=5, initial_sleep=2):
    """Makes a web request with a robust retry mechanism and exponential backoff."""
    sleep_time = initial_sleep
    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                 log(f"WARNING: Rate limited (429). Retrying in {sleep_time}s...")
            else:
                 log(f"WARNING: Received status {response.status_code}. Retrying in {sleep_time}s...")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            log(f"WARNING: Request failed: {e}. Retrying in {sleep_time}s...")
        time.sleep(sleep_time)
        sleep_time *= 2
    log(f"ERROR: Request failed after {retries} retries for URL: {url}")
    return None

def get_all_steam_appids():
    """Fetches the master list of all app IDs from Steam."""
    cached_apps = load_json(APP_LIST_FILE)
    if cached_apps:
        log(f"Loaded {len(cached_apps)} app IDs from cache file '{APP_LIST_FILE}'.")
        return cached_apps
    log("Fetching master app list from Steam API...")
    data = do_request(STEAM_APP_LIST_URL)
    if data and 'applist' in data and 'apps' in data['applist']:
        app_ids = {str(app['appid']) for app in data['applist']['apps']}
        log(f"Successfully fetched {len(app_ids)} app IDs. Caching to '{APP_LIST_FILE}'.")
        save_json(list(app_ids), APP_LIST_FILE)
        return list(app_ids)
    else:
        log("CRITICAL: Failed to fetch master app list from Steam.")
        sys.exit(1)

def scrape_app_details(appid, currency, language):
    """Fetches and combines data from Steam and SteamSpy for a single app ID."""
    steam_data = do_request(STEAM_APP_DETAILS_URL, {'appids': appid, 'cc': currency, 'l': language})
    if not steam_data or appid not in steam_data or not steam_data[appid].get('success'):
        return 'fail', None
    
    app_details = steam_data[appid]['data']
    
    steamspy_data = do_request(STEAMSPY_API_URL, {'request': 'appdetails', 'appid': appid})
    
    combined_data = app_details
    if steamspy_data and steamspy_data.get('developer') != "":
        combined_data['steamspy_owners'] = steamspy_data.get('owners', '0..0').replace(',', '')
        combined_data['steamspy_owners_variance'] = steamspy_data.get('owners_variance', 0)
        combined_data['steamspy_players_forever'] = steamspy_data.get('players_forever', 0)
        combined_data['steamspy_players_forever_variance'] = steamspy_data.get('players_forever_variance', 0)
        combined_data['steamspy_players_2weeks'] = steamspy_data.get('players_2weeks', 0)
        combined_data['steamspy_players_2weeks_variance'] = steamspy_data.get('players_2weeks_variance', 0)
        combined_data['steamspy_average_forever'] = steamspy_data.get('average_forever', 0)
        combined_data['steamspy_average_2weeks'] = steamspy_data.get('average_2weeks', 0)
        combined_data['steamspy_median_forever'] = steamspy_data.get('median_forever', 0)
        combined_data['steamspy_median_2weeks'] = steamspy_data.get('median_2weeks', 0)
        combined_data['steamspy_ccu'] = steamspy_data.get('ccu', 0)
        combined_data['steamspy_tags'] = steamspy_data.get('tags', {})
    else:
        # Add placeholders if SteamSpy fails, to maintain a consistent data structure
        combined_data['steamspy_owners'] = '0..0'
        combined_data['steamspy_tags'] = {}
        
    return 'success', combined_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A robust scraper for ALL Steam application data (unfiltered).')
    parser.add_argument('-o', '--outfile', type=str, default=APPS_FILE, help='Output file for the app dataset.')
    parser.add_argument('-s', '--sleep', type=float, default=1.5, help='Base time to wait between requests.')
    parser.add_argument('-r', '--retries', type=int, default=5, help='Number of retries for failed requests.')
    parser.add_argument('-a', '--autosave', type=int, default=100, help='Save progress every N new apps scraped.')
    parser.add_argument('-c', '--currency', type=str, default='us', help='ISO 3166 country code for currency.')
    parser.add_argument('-l', '--language', type=str, default='en', help='Language for text content.')
    args = parser.parse_args()

    log("--- Steam Application Data Scraper (Unfiltered) ---")

    # Load existing data to resume progress
    dataset = load_json(args.outfile) or {}
    failed_ids = set(load_json(FAILED_FILE) or [])
    
    log(f"Loaded {len(dataset)} existing apps from '{args.outfile}'.")
    log(f"Loaded {len(failed_ids)} failed app IDs from '{FAILED_FILE}'.")
    
    all_appids = get_all_steam_appids()
    
    # Filter out app IDs we've already processed to allow resuming
    appids_to_scrape = [aid for aid in all_appids if aid not in dataset and aid not in failed_ids]
    
    log(f"Starting scrape for {len(appids_to_scrape)} new app IDs.")
    
    new_apps_count = 0
    total_to_scrape = len(appids_to_scrape)
    
    try:
        for i, appid in enumerate(appids_to_scrape):
            progress_bar("Scraping Apps", i + 1, total_to_scrape)
            
            status, data = scrape_app_details(appid, args.currency, args.language)
            
            if status == 'success':
                dataset[appid] = data
                new_apps_count += 1
            elif status == 'fail':
                failed_ids.add(appid)
            
            # Autosave progress
            if args.autosave > 0 and new_apps_count > 0 and new_apps_count % args.autosave == 0:
                log(f"\nAutosaving progress: {len(dataset)} total apps found...")
                save_json(dataset, args.outfile)
                save_json(list(failed_ids), FAILED_FILE)
            
            time.sleep(args.sleep)
            
    except (KeyboardInterrupt, SystemExit):
        log("\nInterruption detected. Performing final save...")
    finally:
        # Perform a final save of all data
        log("\nScraping finished or interrupted. Saving all data...")
        save_json(dataset, args.outfile)
        save_json(list(failed_ids), FAILED_FILE)
        log(f"--- COMPLETE ---")
        log(f"Total apps in dataset: {len(dataset)}")
        log(f"Total failed/unsuccessful apps: {len(failed_ids)}")