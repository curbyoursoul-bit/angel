# automation/holiday_checker.py
from __future__ import annotations
from datetime import date
from pathlib import Path
import csv

HOLIDAY_CSV = Path("data/nse_holidays.csv")

def load_holidays() -> set[date]:
    s: set[date] = set()
    if not HOLIDAY_CSV.exists():
        return s
    with HOLIDAY_CSV.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # expect a column named "date" as YYYY-MM-DD
            d = row.get("date") or row.get("Date") or ""
            d = d.strip()
            if not d:
                continue
            try:
                y, m, dd = map(int, d.split("-"))
                s.add(date(y, m, dd))
            except Exception:
                continue
    return s

def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:  # Sat/Sun
        return False
    return d not in load_holidays()
