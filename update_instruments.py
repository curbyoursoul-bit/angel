# update_instruments.py
import requests
from pathlib import Path
from loguru import logger

URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
DEST = Path("data/OpenAPIScripMaster.json")

def update_instruments():
    try:
        DEST.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"📥 Downloading instruments master from Angel One → {URL}")
        resp = requests.get(URL, timeout=60)
        resp.raise_for_status()
        DEST.write_bytes(resp.content)
        logger.success(f"✅ Instruments saved to {DEST}")
    except Exception as e:
        logger.error(f"❌ Failed to download instruments: {e}")
        raise

if __name__ == "__main__":
    update_instruments()
