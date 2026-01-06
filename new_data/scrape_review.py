# File: scrape_top_reviews.py
import requests
import json
import time
import argparse
import sys
import os
import datetime as dt
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURATION ---
REVIEWS_DIR = 'reviews' # Directory to store output files
TOP_N_GAMES = 100

# --- API ENDPOINTS ---
STEAMDB_CHARTS_URL = 'https://steamdb.info/charts/'
STEAM_REVIEWS_URL = 'https://store.steampowered.com/appreviews/{}' # .format(appid)

# --- UTILITY FUNCTIONS (Copied from the other script for standalone use) ---
def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def do_request(url, params=None, retries=5, initial_sleep=2):
    sleep_time = initial_sleep
    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=20)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                 log(f"WARNING: Rate limited (429) for {url}. Retrying in {sleep_time}s...")
            else:
                 log(f"WARNING: Received status {response.status_code}. Retrying in {sleep_time}s...")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            log(f"WARNING: Request failed: {e}. Retrying in {sleep_time}s...")
        time.sleep(sleep_time)
        sleep_time = min(sleep_time * 2, 60) # Cap sleep time at 60s
    log(f"ERROR: Request failed after {retries} retries for URL: {url}")
    return None

def get_top_appids_from_steamdb():
    """Uses undetected-chromedriver to bypass Cloudflare on SteamDB and get top game IDs."""
    log("Fetching top 100 game App IDs from SteamDB...")
    options = uc.ChromeOptions()
    options.add_argument('--headless')
    driver = None
    try:
        driver = uc.Chrome(options=options)
        driver.get(STEAMDB_CHARTS_URL)
        log("  - Waiting for chart data to load...")
        wait = WebDriverWait(driver, 45)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tr.app[data-appid]")))
        
        app_rows = driver.find_elements(By.CSS_SELECTOR, "tr.app[data-appid]")
        appids = [row.get_attribute('data-appid') for row in app_rows[:TOP_N_GAMES]]
        
        log(f"  - Successfully found {len(appids)} App IDs.")
        return appids
    except Exception as e:
        log(f"CRITICAL: Failed to scrape SteamDB for top App IDs. Error: {e}")
        return []
    finally:
        if driver:
            driver.quit()

def scrape_reviews_for_game(appid, language):
    """Scrapes all reviews for a single game and saves them to a .jsonl file."""
    output_filename = os.path.join(REVIEWS_DIR, f'{appid}.jsonl')
    
    # Resume logic: if file exists and is not empty, skip.
    if os.path.exists(output_filename) and os.path.getsize(output_filename) > 0:
        log(f"Reviews for App ID {appid} already exist. Skipping.")
        return True

    log(f"Starting review scrape for App ID: {appid}")
    
    cursor = '*'
    total_reviews_scraped = 0
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        while True:
            params = {
                'json': 1,
                'language': language,
                'num_per_page': 100,
                'cursor': cursor
            }
            
            response_data = do_request(STEAM_REVIEWS_URL.format(appid), params)
            
            if not response_data or response_data.get('success') != 1 or not response_data.get('reviews'):
                log(f"  - No more reviews found or API error for App ID {appid}. Ending scrape.")
                break

            for review in response_data['reviews']:
                f.write(json.dumps(review) + '\n')

            total_reviews_scraped += len(response_data['reviews'])
            log(f"  - Scraped {total_reviews_scraped} reviews so far for App ID {appid}...")
            
            cursor = response_data.get('cursor')
            # The API returns the same cursor when on the last page.
            if not cursor or response_data.get('query_summary', {}).get('num_reviews', 0) < 100:
                break
            
            time.sleep(1.5) # Be respectful between paginated requests

    log(f"Finished scraping for App ID {appid}. Total reviews: {total_reviews_scraped}.")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A focused scraper for Steam game reviews.')
    parser.add_argument('-l', '--language', type=str, default='english', help='Language to scrape reviews for.')
    # NOTE: Proxies would be the next step for parallelization, but that adds significant complexity.
    # This script is designed for a robust, single-worker execution.
    args = parser.parse_args()

    log(f"--- Top {TOP_N_GAMES} Steam Game Review Scraper ---")
    
    if not os.path.exists(REVIEWS_DIR):
        log(f"Creating output directory: '{REVIEWS_DIR}'")
        os.makedirs(REVIEWS_DIR)
        
    top_appids = get_top_appids_from_steamdb()
    
    if not top_appids:
        log("Could not retrieve top App IDs. Exiting.")
        sys.exit(1)
        
    log(f"Beginning review scrape for {len(top_appids)} games.")
    
    for i, appid in enumerate(top_appids):
        log(f"--- Processing game {i+1}/{len(top_appids)} ---")
        try:
            scrape_reviews_for_game(appid, args.language)
        except (KeyboardInterrupt, SystemExit):
            log("\nInterruption detected. Exiting gracefully.")
            break
        except Exception as e:
            log(f"\nAn unexpected error occurred for App ID {appid}: {e}")
            continue # Move to the next game

    log("--- SCRAPING COMPLETE ---")