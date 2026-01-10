import json
import os

# Configuration
INPUT_FILE = 'steam_apps_dataset_raw.jsonl'
OUTPUT_FILE = 'top_10k_games_by_owners.jsonl'
TOP_N = 10000

def log(message):
    print(f"[INFO] {message}")

def filter_top_games():
    """
    Filters games by steamspy_owners and outputs the top 10k to a new file.
    """
    log(f"Reading games from '{INPUT_FILE}'...")
    
    if not os.path.exists(INPUT_FILE):
        log(f"ERROR: Input file '{INPUT_FILE}' not found!")
        return
    
    games = []
    
    # Read all games
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                game = json.loads(line)
                
                # Extract appid and steamspy_owners
                # Format is usually: {"123456": {"name": "...", "steamspy_owners": ...}}
                if len(game.keys()) == 1:
                    appid = list(game.keys())[0]
                    game_data = game[appid]
                    
                    # Get steamspy_owners, default to 0 if not present
                    owners = game_data.get('steamspy_owners', 0)
                    
                    # Handle different data types
                    if isinstance(owners, str):
                        # Try to parse range like "0 .. 20,000" or "0-20000" or numeric string
                        if '..' in owners:
                            # Take the upper bound of the range
                            try:
                                owners = int(owners.split('..')[1].strip().replace(',', ''))
                            except:
                                owners = 0
                        elif '-' in owners:
                            # Take the upper bound of the range
                            try:
                                owners = int(owners.split('-')[1].strip().replace(',', ''))
                            except:
                                owners = 0
                        else:
                            try:
                                owners = int(owners.replace(',', ''))
                            except:
                                owners = 0
                    elif not isinstance(owners, (int, float)):
                        owners = 0
                    
                    games.append((owners, game))
                    
            except json.JSONDecodeError:
                continue
    
    log(f"Loaded {len(games)} games.")
    
    # Sort by owners (descending) and take top N
    log(f"Sorting by steamspy_owners and selecting top {TOP_N}...")
    games.sort(key=lambda x: x[0], reverse=True)
    top_games = games[:TOP_N]
    
    # Write to output file
    log(f"Writing top {len(top_games)} games to '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for owners, game in top_games:
            f.write(json.dumps(game) + '\n')
    
    log(f"Done! Top {len(top_games)} games saved to '{OUTPUT_FILE}'")
    
    # Print some stats
    if top_games:
        log(f"Highest owners: {top_games[0][0]:,}")
        log(f"Lowest in top {TOP_N}: {top_games[-1][0]:,}")

if __name__ == "__main__":
    filter_top_games()
