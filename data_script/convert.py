# File: convert_to_jsonl.py
import ijson
import json
import os
import datetime as dt

# --- CONFIGURATION ---
# The large JSON file you already have
INPUT_JSON_FILE = 'steam_apps_dataset_raw.json'
# The new JSON Lines file you want to create
OUTPUT_JSONL_FILE = 'steam_apps_dataset_raw.jsonl'

def log(message):
    """Prints a formatted log message with a timestamp."""
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}")

def convert_json_to_jsonl(input_file, output_file):
    """
    Streams a large JSON file and converts it to JSON Lines format
    without loading the entire file into memory.
    """
    if not os.path.exists(input_file):
        log(f"ERROR: Input file '{input_file}' not found. Aborting.")
        return

    log(f"Starting conversion of '{input_file}' to '{output_file}'.")
    log("This may take some time for a large file, but memory usage will be low.")

    count = 0
    try:
        # Open the input file for reading and the output file for writing
        with open(input_file, 'rb') as fin, open(output_file, 'w', encoding='utf-8') as fout:
            # ijson.kvitems streams the key-value pairs from the top-level JSON object
            # The '' prefix means "start at the root of the document"
            parser = ijson.kvitems(fin, '')
            
            for appid, game_data in parser:
                # For each game (key-value pair) found in the input,
                # create the output object and write it as a new line.
                output_object = {appid: game_data}
                fout.write(json.dumps(output_object) + '\n')
                
                count += 1
                if count % 10000 == 0:
                    log(f"  - Converted {count} objects...")

        log(f"\n--- Conversion Complete ---")
        log(f"Successfully converted {count} objects.")
        log(f"Output saved to: '{output_file}'")

    except Exception as e:
        log(f"\nAn error occurred during conversion: {e}")
        log("The output file may be incomplete.")


if __name__ == "__main__":
    convert_json_to_jsonl(INPUT_JSON_FILE, OUTPUT_JSONL_FILE)