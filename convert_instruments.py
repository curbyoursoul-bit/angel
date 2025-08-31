# convert_instruments.py
import json
import csv
from pathlib import Path
from loguru import logger

DATA_DIR = Path("data")
json_file = DATA_DIR / "OpenAPIScripMaster.json"
csv_file = DATA_DIR / "OpenAPIScripMaster.csv"

def convert_json_to_csv():
    if not json_file.exists():
        logger.error(f"❌ JSON file not found: {json_file}")
        return
    
    with open(json_file, "r") as f:
        instruments = json.load(f)

    if not instruments:
        logger.error("❌ JSON file is empty or invalid.")
        return
    
    # Extract headers from keys of first instrument
    headers = instruments[0].keys()

    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(instruments)

    logger.success(f"✅ Converted {json_file} → {csv_file}")

if __name__ == "__main__":
    convert_json_to_csv()
