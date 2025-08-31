# data.py
"""
Utility for managing data files (instruments, holidays, peek).
Run from project root:

  python data.py update-instruments
  python data.py peek-instruments
  python data.py holidays
  python data.py holidays --refresh
"""

import sys
from loguru import logger
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"

def update_instruments():
    from update_instruments import update_instruments as _update
    _update()

def peek_instruments():
    from scripts.peek_instruments import main as _peek
    _peek()

def holidays(refresh=False):
    from ops.holidays import load_holidays_combined, fetch_holidays_live, save_holidays_to_csv
    if refresh:
        live = fetch_holidays_live()
        if not live:
            logger.error("❌ Could not fetch live holidays from NSE")
            return
        save_holidays_to_csv(live)
        logger.success(f"✅ Saved {len(live)} holidays → data/nse_holidays.csv")
    else:
        hols = load_holidays_combined()
        print(f"Loaded {len(hols)} holidays:")
        for d in sorted(hols):
            print(" -", d)

def main(argv=None):
    argv = argv or sys.argv[1:]
    if not argv:
        print(__doc__)
        return 0

    cmd = argv[0]
    if cmd == "update-instruments":
        update_instruments()
    elif cmd == "peek-instruments":
        peek_instruments()
    elif cmd == "holidays":
        holidays("--refresh" in argv)
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
