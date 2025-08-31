# refresh_instruments.py
# Run:  python refresh_instruments.py
# Optional: python refresh_instruments.py --url <override_json_url> --out data\OpenAPIScripMaster.json --csv data\OpenAPIScripMaster.csv

from pathlib import Path
from typing import Optional
import argparse
import json

import pandas as pd
import requests
from loguru import logger


DEFAULT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
DEFAULT_JSON = Path("data/OpenAPIScripMaster.json")
DEFAULT_CSV  = Path("data/OpenAPIScripMaster.csv")


def download_json(url: str, dest: Path) -> Path:
    """Download Angel One instrument master JSON to dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"📥 Downloading instruments → {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)
    logger.success(f"✅ Saved JSON → {dest}")
    return dest


def convert_json_to_csv(src_json: Path, dest_csv: Path) -> Path:
    """Load JSON (array of dicts), normalize, and save as CSV."""

    # read raw JSON (avoid pandas.read_json for very large files with mixed types)
    raw = json.loads(src_json.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Unexpected JSON format: root is not a list")

    df = pd.DataFrame(raw)

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Some dumps use 'exch_seg' & 'token' etc. Keep as-is (your code expects these).
    # Normalize expiry to datetime; keep original in 'expiry' for compatibility
    if "expiry" in df.columns:
        # Make a parallel datetime column for robust querying in code (expiry_dt)
        df["expiry_dt"] = pd.to_datetime(df["expiry"], errors="coerce")

    # Strike scaling: in Angel JSON, option strikes can be *100 (e.g., 5,570,000 for 55,700).
    # We **do not** rescale here; your strategy code already looks for the same format in CSV.
    # If you ever want to rescale, uncomment below:
    # if "strike" in df.columns:
    #     s = pd.to_numeric(df["strike"], errors="coerce")
    #     med = s.dropna().median()
    #     if pd.notna(med) and med > 100000:  # heuristic: likely *100
    #         df["strike"] = s / 100.0

    dest_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dest_csv, index=False)
    logger.success(f"✅ Converted JSON → CSV : {dest_csv}  (rows={len(df):,})")
    return dest_csv


def main(url: str, out_json: Path, out_csv: Path) -> None:
    try:
        download_json(url, out_json)
        convert_json_to_csv(out_json, out_csv)
        logger.success("🎉 Instruments refresh complete.")
    except Exception as e:
        logger.exception(f"❌ Refresh failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    logger.add("logs/refresh.log", rotation="1 week", retention="4 weeks", enqueue=True)

    parser = argparse.ArgumentParser(description="Refresh Angel One instruments (JSON → CSV)")
    parser.add_argument("--url", default=DEFAULT_URL, help="JSON URL to download from")
    parser.add_argument("--out", default=str(DEFAULT_JSON), help="Path to save JSON")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="Path to save CSV")
    args = parser.parse_args()

    main(args.url, Path(args.out), Path(args.csv))
