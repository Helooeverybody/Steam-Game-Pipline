# File: backfill_steamcharts_monthly_simple.py
import json
import time
import os
import datetime as dt
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# --- CONFIGURATION (relative to repository root - run scripts from repo root) ---
OUTPUT_FILE_MONTHLY = 'data/raw/historical_player_counts_monthly.jsonl'
TARGET_APP_IDS = [
    "578080",  # 1. PUBG: BATTLEGROUNDS
    "2358720", # 2. Black Myth: Wukong
    "1623730", # 3. Palworld
    "730",     # 4. Counter-Strike 2
    "2246340", # 5. Monster Hunter Wilds
    "1599340", # 6. Lost Ark
    "570",     # 7. Dota 2
    "1091500", # 8. Cyberpunk 2077
    "1245620", # 9. ELDEN RING
    "2923300", # 10. Banana
]

# --- SCRAPING TARGET ---
STEAMCHARTS_URL = 'https://steamcharts.com/app/{}'

def log(message):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def get_already_scraped_ids(filename):
    scraped_ids = set()
    if not os.path.exists(filename): return scraped_ids
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                scraped_ids.add(json.loads(line)['app_id'])
            except (json.JSONDecodeError, KeyError): continue
    return scraped_ids

def scrape_monthly_history(driver, appid):
    """Navigates to a game's SteamCharts page and scrapes the full monthly history table."""
    log(f"  - Navigating to SteamCharts for App ID {appid}...")
    try:
        driver.get(STEAMCHARTS_URL.format(appid))
        
        wait = WebDriverWait(driver, 30)
        
        # --- SIMPLIFIED LOGIC ---
        # We now only wait for the table to be present and then parse immediately.
        log("  - Waiting for the monthly data table to load...")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.common-table")))
        log("  - Table found. Parsing HTML...")
        # --- END OF SIMPLIFIED LOGIC ---

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.select_one("table.common-table")
        
        if not table:
            log("  - WARNING: Could not find the monthly history table."); return None

        monthly_data = []
        # Skip the first summary row ('Last 30 Days')
        for row in table.select('tbody tr:nth-of-type(n+2)'):
            cells = row.select('td')
            if len(cells) < 5: continue

            month_text = cells[0].get_text(strip=True)
            avg_players_text = cells[1].get_text(strip=True).replace(',', '')
            peak_players_text = cells[4].get_text(strip=True).replace(',', '')
            
            try:
                month_dt = dt.datetime.strptime(month_text.strip(), '%B %Y')
                if month_dt.month == 12:
                    end_of_month_dt = month_dt.replace(year=month_dt.year + 1, month=1, day=1) - dt.timedelta(days=1)
                else:
                    end_of_month_dt = month_dt.replace(month=month_dt.month + 1, day=1) - dt.timedelta(days=1)
                
                monthly_data.append({
                    "timestamp": end_of_month_dt.isoformat(),
                    "avg_players": float(avg_players_text),
                    "peak_players": int(peak_players_text)
                })
            except (ValueError, IndexError): continue

        log(f"  - SUCCESS: Extracted {len(monthly_data)} months of data.")
        return monthly_data
            
    except TimeoutException:
        log(f"  - ERROR: Timed out waiting for page elements for App ID {appid}. The game may not be tracked."); return None
    except Exception as e:
        log(f"  - ERROR: An unexpected error occurred for App ID {appid}: {e}"); return None

if __name__ == "__main__":
    log("--- Historical Player Count Backfill (Monthly Only, Simplified) ---")
    
    scraped_appids = get_already_scraped_ids(OUTPUT_FILE_MONTHLY)
    if scraped_appids:
        log(f"Found {len(scraped_appids)} already scraped App IDs. Will skip them.")
    
    appids_to_scrape = [aid for aid in TARGET_APP_IDS if aid not in scraped_appids]
    
    if not appids_to_scrape:
        log("All target App IDs have already been scraped. Exiting."); sys.exit(0)

    log("Initializing browser (Visible Head, Chrome v131)...")
    options = uc.ChromeOptions(); options.add_argument("--start-maximized")
    driver = None
    try:
        driver = uc.Chrome(options=options, version_main=131)
        
        with open(OUTPUT_FILE_MONTHLY, 'a', encoding='utf-8') as f_out:
            for i, appid in enumerate(appids_to_scrape):
                log(f"\n--- Processing Game {i+1}/{len(appids_to_scrape)} (App ID: {appid}) ---")
                
                monthly_data = scrape_monthly_history(driver, appid)
                
                if monthly_data:
                    log(f"  - Writing {len(monthly_data)} records to '{OUTPUT_FILE_MONTHLY}'...")
                    for record in monthly_data:
                        message = {
                            "event_timestamp": record["timestamp"],
                            "app_id": appid,
                            "player_count": record["avg_players"],
                            "peak_players_monthly": record["peak_players"]
                        }
                        f_out.write(json.dumps(message) + '\n')
                    log(f"  - Done.")
                
                time.sleep(3)

    except (KeyboardInterrupt, SystemExit):
        log("\nInterruption detected. Progress has been saved.")
    except Exception as e:
        log(f"\nA critical error occurred: {e}")
        input("Press Enter to close the browser and exit...")
    finally:
        if driver:
            log("Closing browser.")
            driver.quit()

    log("\n--- BACKFILL COMPLETE ---")