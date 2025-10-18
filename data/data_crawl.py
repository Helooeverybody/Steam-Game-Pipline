import requests
import json
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- NEW AND IMPORTANT IMPORT ---
import undetected_chromedriver as uc

# --- Selenium Imports (still needed for options, waits, etc.) ---
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURATION ---
STEAM_API_KEY = os.getenv('STEAM_API_KEY')  # Load from environment variable
TOP_GAMES_COUNT = 100
REVIEWS_FOR_TOP_N_GAMES = 10
MAX_REVIEWS_PER_GAME = 200

# --- API ENDPOINTS ---
STEAMDB_TOP_GAMES_URL = 'https://steamdb.info/charts/'
APP_DETAILS_URL = 'https://store.steampowered.com/api/appdetails'
APP_REVIEWS_URL = 'https://store.steampowered.com/appreviews/{}'
PLAYER_SUMMARIES_URL = 'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/'

# --- HELPER FUNCTIONS ---

def get_top_100_game_appids_selenium():
    """
    (UPDATED FUNCTION - UNDETECTED MODE)
    Uses undetected-chromedriver to bypass services like Cloudflare.
    """
    print("Fetching top 100 games using undetected-chromedriver to bypass Cloudflare...")

    # Options are still useful for setting user-agent, etc.
    options = uc.ChromeOptions()
    # You can run this in headless mode, as it's much more effective than standard Selenium's
    # options.add_argument('--headless') 
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36')
    
    driver = None 

    try:
        # --- THIS IS THE KEY CHANGE ---
        # We use uc.Chrome() instead of webdriver.Chrome()
        driver = uc.Chrome(options=options)
        # --- END OF KEY CHANGE ---
        
        driver.get(STEAMDB_TOP_GAMES_URL)
        
        print("  - Page loaded. Waiting for Cloudflare checks (if any) and for chart data...")
        
        # Increase timeout to 45 seconds to give it ample time to solve JS challenges
        wait = WebDriverWait(driver, 45)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tr.app[data-appid]")))

        print("  - Chart data found. Cloudflare bypassed! Extracting App IDs...")
        
        app_rows = driver.find_elements(By.CSS_SELECTOR, "tr.app[data-appid]")
        
        appids = []
        for row in app_rows:
            appids.append(row.get_attribute('data-appid'))
            if len(appids) >= TOP_GAMES_COUNT:
                break
        
        if not appids:
            print("  - ERROR: Could not find any App IDs. The website structure may have changed.")
            return []
        
        print(f"Successfully found {len(appids)} App IDs.")
        return appids

    except Exception as e:
        print(f"An error occurred during the Selenium process: {e}")
        # The browser will close automatically with this library on error.
        return []
        
    finally:
        if driver:
            print("  - Closing the browser.")
            driver.quit()

# The rest of the functions are unchanged as they use the Steam API, which is not protected by Cloudflare.

def get_game_details(appids):
    game_details = {}
    print(f"\nFetching details for {len(appids)} games...")
    for i, appid in enumerate(appids):
        print(f"  - ({i+1}/{len(appids)}) Fetching details for App ID: {appid}")
        try:
            response = requests.get(APP_DETAILS_URL, params={'appids': appid})
            response.raise_for_status()
            data = response.json()
            if data and data.get(appid, {}).get('success'):
                game_details[appid] = data[appid]['data']
            else:
                print(f"    - Failed to get details for App ID: {appid}.")
            time.sleep(1.5)
        except requests.exceptions.RequestException as e:
            print(f"    - An error occurred for App ID {appid}: {e}")
            continue
    return game_details

def get_game_reviews(appid, limit=MAX_REVIEWS_PER_GAME):
    reviews = []
    cursor = '*'
    print(f"Fetching up to {limit} reviews for App ID: {appid}...")
    while len(reviews) < limit:
        params = {'json': 1, 'filter': 'recent', 'language': 'all', 'num_per_page': 100, 'cursor': cursor}
        try:
            response = requests.get(APP_REVIEWS_URL.format(appid), params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('success') and data.get('reviews'):
                reviews.extend(data['reviews'])
                cursor = data.get('cursor')
                print(f"  - Fetched {len(data['reviews'])} reviews. Total so far: {len(reviews)}")
                if not cursor: break
            else: break
            time.sleep(1.5)
        except requests.exceptions.RequestException as e:
            print(f"  - An error occurred while fetching reviews for App ID {appid}: {e}")
            break
    return reviews

def get_public_profiles(steamids):
    public_profiles = {}
    for i in range(0, len(steamids), 100):
        chunk = steamids[i:i+100]
        ids_string = ','.join(chunk)
        params = {'key': STEAM_API_KEY, 'steamids': ids_string}
        print(f"\nFetching profiles for a batch of {len(chunk)} users...")
        try:
            response = requests.get(PLAYER_SUMMARIES_URL, params=params)
            response.raise_for_status()
            data = response.json().get('response', {}).get('players', [])
            for player_data in data:
                if player_data.get('communityvisibilitystate') == 3:
                    public_profiles[player_data['steamid']] = player_data
            print(f"  - Found {len(data)} profiles in batch, {len(public_profiles)} public profiles so far.")
        except requests.exceptions.RequestException as e:
            print(f"  - An error occurred while fetching user profiles: {e}")
        time.sleep(1)
    return public_profiles

# --- MAIN EXECUTION ---
if __name__ == '__main__':
    if not STEAM_API_KEY:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! ERROR: STEAM_API_KEY not found in environment variables !!!")
        print("!!! Please create a .env file with your Steam Web API key   !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    else:
        top_100_appids = get_top_100_game_appids_selenium()
        
        if top_100_appids:
            top_100_game_info = get_game_details(top_100_appids)
            with open('top_100_games_info.json', 'w', encoding='utf-8') as f:
                json.dump(top_100_game_info, f, indent=4, ensure_ascii=False)
            print("\nSaved detailed info for top 100 games to 'top_100_games_info.json'")
            
            top_n_appids = top_100_appids[:REVIEWS_FOR_TOP_N_GAMES]
            all_reviews = {}
            reviewer_steamids = set()
            
            print(f"\n--- Starting review collection for top {REVIEWS_FOR_TOP_N_GAMES} games ---")
            for appid in top_n_appids:
                game_name = top_100_game_info.get(appid, {}).get('name', f"AppID {appid}")
                print(f"\nProcessing reviews for: {game_name}")
                reviews = get_game_reviews(appid)
                all_reviews[appid] = reviews
                for review in reviews:
                    reviewer_steamids.add(review['author']['steamid'])
            
            with open('top_10_games_reviews.json', 'w', encoding='utf-8') as f:
                json.dump(all_reviews, f, indent=4, ensure_ascii=False)
            print("\nSaved reviews for top 10 games to 'top_10_games_reviews.json'")
            
            print(f"\n--- Found {len(reviewer_steamids)} unique reviewers. Fetching public profiles... ---")
            public_reviewer_profiles = get_public_profiles(list(reviewer_steamids))
            
            with open('public_reviewer_profiles.json', 'w', encoding='utf-8') as f:
                json.dump(public_reviewer_profiles, f, indent=4, ensure_ascii=False)
            print(f"\nSaved {len(public_reviewer_profiles)} public reviewer profiles to 'public_reviewer_profiles.json'")

            print("\n--- CRAWLING COMPLETE ---")