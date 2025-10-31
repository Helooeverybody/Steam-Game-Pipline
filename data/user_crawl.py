import json
import requests
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
STEAM_API_KEY = os.getenv('STEAM_API_KEY')  # Load from environment variable
INPUT_PROFILE_FILE = 'public_reviewer_profiles.json'
OUTPUT_GAMES_FILE = 'users_owned_games.json'

# --- API ENDPOINTS ---
OWNED_GAMES_URL = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/'

def load_public_profiles(filename):
    """Loads the previously saved public profiles from the JSON file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            profiles_data = json.load(f)
        # We just need the Steam IDs
        steam_ids = list(profiles_data.keys())
        print(f"Successfully loaded {len(steam_ids)} public user IDs from '{filename}'.")
        return steam_ids
    except FileNotFoundError:
        print(f"ERROR: The input file '{filename}' was not found.")
        print("Please run the first script (steam_crawler.py) to generate this file.")
        return []
    except json.JSONDecodeError:
        print(f"ERROR: Could not parse the JSON in '{filename}'.")
        return []

def get_users_owned_games(steamids):
    """
    Iterates through a list of Steam IDs and attempts to fetch their owned games.
    """
    all_owned_games = {}
    total_users = len(steamids)
    private_profiles = 0
    public_profiles = 0

    print(f"\n--- Starting to fetch owned games for {total_users} users ---")

    for i, steamid in enumerate(steamids):
        params = {
            'key': STEAM_API_KEY,
            'steamid': steamid,
            'format': 'json',
            'include_appinfo': True, # Set to True to get game names and icons
            'include_played_free_games': True
        }
        
        print(f"({i+1}/{total_users}) Fetching games for user: {steamid}")

        try:
            response = requests.get(OWNED_GAMES_URL, params=params)
            response.raise_for_status()
            data = response.json().get('response', {})

            # --- THIS IS THE CRITICAL PRIVACY CHECK ---
            # If the 'games' key is missing or empty, their library is private.
            if data and data.get('games'):
                all_owned_games[steamid] = data['games']
                public_profiles += 1
                print(f"  - SUCCESS: Found {data.get('game_count', 0)} games. User's library is public.")
            else:
                private_profiles += 1
                print("  - INFO: User's game library is private or empty.")
            
            # IMPORTANT: API calls for this endpoint are more limited. Be respectful.
            time.sleep(1.5)

        except requests.exceptions.RequestException as e:
            print(f"  - ERROR: An HTTP error occurred for user {steamid}: {e}")
            continue
    
    print("\n--- Game Fetching Complete ---")
    print(f"Public Libraries Found: {public_profiles}")
    print(f"Private/Empty Libraries: {private_profiles}")
    
    return all_owned_games

# --- MAIN EXECUTION ---
if __name__ == '__main__':
    if not STEAM_API_KEY:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! ERROR: STEAM_API_KEY not found in environment variables !!!")
        print("!!! Please create a .env file with your Steam Web API key   !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    else:
        # 1. Load the list of public Steam IDs from the previous script's output
        public_steamids = load_public_profiles(INPUT_PROFILE_FILE)

        if public_steamids:
            # 2. Crawl the owned games for each user
            owned_games_data = get_users_owned_games(public_steamids)

            # 3. Save the results to a new file
            if owned_games_data:
                with open(OUTPUT_GAMES_FILE, 'w', encoding='utf-8') as f:
                    json.dump(owned_games_data, f, indent=4, ensure_ascii=False)
                print(f"\nSuccessfully saved owned games data to '{OUTPUT_GAMES_FILE}'")