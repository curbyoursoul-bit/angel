# ops/holidays.py
from __future__ import annotations
import csv, datetime as dt, json, os, time
from pathlib import Path
from typing import Set, Iterable, Tuple, Optional
import urllib.request, urllib.error

# --- Timezone (pytz optional) ----------------------------------------------
try:
    import pytz  # type: ignore
    IST = pytz.timezone("Asia/Kolkata")
except Exception:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# Anchor paths to project root
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
HOLIDAY_CSV = DATA_DIR / "nse_holidays.csv"
CACHE_FILE = DATA_DIR / ".nse_holidays.cache.json"  # private cache
CACHE_TTL_SECONDS = int(os.getenv("NSE_HOLIDAY_CACHE_TTL", "21600"))  # 6h

# --- helpers ---------------------------------------------------------------
def _parse_dates_strs(date_strs: Iterable[str]) -> Set[dt.date]:
    out: Set[dt.date] = set()
    for s in date_strs:
        s = (str(s) or "").strip()
        if not s:
            continue
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y"):
            try:
                out.add(dt.datetime.strptime(s, fmt).date())
                break
            except Exception:
                continue
    return out

def _now_ist() -> dt.datetime:
    return dt.datetime.now(tz=IST)

# --- CSV load/save ---------------------------------------------------------
def load_holidays() -> Set[dt.date]:
    """Load holidays from local CSV (first column 'date')."""
    out: Set[dt.date] = set()
    if not HOLIDAY_CSV.exists():
        return out
    with HOLIDAY_CSV.open("r", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = True
        for row in r:
            if not row:
                continue
            if header and (row[0].strip().lower() == "date"):
                header = False
                continue
            header = False
            s = (row[0] or "").strip()
            if not s or s.startswith("#"):
                continue
            out |= _parse_dates_strs([s])
    return out

def save_holidays_to_csv(dates: Set[dt.date]) -> None:
    """Write dates back to CSV (sorted, with header)."""
    HOLIDAY_CSV.parent.mkdir(parents=True, exist_ok=True)
    with HOLIDAY_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date"])
        for d in sorted(dates):
            w.writerow([d.isoformat()])

# --- Cache helpers ---------------------------------------------------------
def _load_cache() -> Optional[Set[dt.date]]:
    try:
        if not CACHE_FILE.exists():
            return None
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        ts = float(raw.get("ts", 0))
        if time.time() - ts > CACHE_TTL_SECONDS:
            return None
        return _parse_dates_strs(raw.get("dates", []))
    except Exception:
        return None

def _save_cache(dates: Set[dt.date]) -> None:
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {"ts": time.time(), "dates": [d.isoformat() for d in sorted(dates)]}
        CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass  # best effort

# --- Live fetch from NSE ---------------------------------------------------
NSE_JSON_ENDPOINTS: Tuple[str, ...] = (
    "https://www.nseindia.com/api/holiday-master?type=trading",
    "https://www.nseindia.com/api/holiday-master?type=clearing",
)
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

def _priming_request() -> None:
    """
    NSE sometimes requires a prior request to set cookies before API calls.
    We ignore failures; this is best-effort only.
    """
    try:
        req = urllib.request.Request(
            "https://www.nseindia.com/",
            headers={"User-Agent": _UA, "Connection": "keep-alive"},
        )
        urllib.request.urlopen(req, timeout=8).read(64)
    except Exception:
        pass

def _fetch_json(url: str, timeout: int = 20) -> Optional[dict]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": _UA,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return json.loads(body)
    except Exception:
        return None

def _retry_fetch(url: str, attempts: int = 3, sleep_sec: float = 0.7) -> Optional[dict]:
    for i in range(attempts):
        data = _fetch_json(url)
        if data:
            return data
        time.sleep(sleep_sec * (1.0 + 0.5 * i))
    return None

def _extract_dates_from_payload(payload: dict) -> Set[dt.date]:
    """
    Known shapes:
      { "CM": [ { "tradingDate": "26-Jan-2025", ... }, ... ], "FO": [...], ... }
      or { "Trading": [ {... "holidayDate": "2025-01-26"} ] }
    We scan all list values, and try keys: tradingDate, holidayDate, date.
    """
    out: Set[dt.date] = set()
    if not isinstance(payload, dict):
        return out
    for value in payload.values():
        if isinstance(value, list):
            for row in value:
                if not isinstance(row, dict):
                    continue
                s = row.get("tradingDate") or row.get("holidayDate") or row.get("date")
                if s:
                    out |= _parse_dates_strs([str(s)])
    return out

def fetch_holidays_live(timeout: int = 20) -> Set[dt.date]:
    """
    Fetch holiday dates from NSE. Best-effort:
     - cookie priming
     - retry each endpoint
     - merge & dedupe across endpoints
    """
    _priming_request()
    dates: Set[dt.date] = set()
    for url in NSE_JSON_ENDPOINTS:
        data = _retry_fetch(url, attempts=3)
        if not data:
            continue
        dates |= _extract_dates_from_payload(data)
        if dates:
            # usually first endpoint is enough; still keep merging
            pass
    return dates

# --- Combined loader -------------------------------------------------------
def load_holidays_combined(persist_csv_if_updated: bool = False) -> Set[dt.date]:
    """
    Prefer cached/live NSE list; fallback to CSV; union them.
    If `persist_csv_if_updated=True`, writes union back to CSV.
    """
    cached = _load_cache()
    live = fetch_holidays_live() if cached is None else None
    live_or_cached = cached if cached is not None else (live or set())

    csv_local = load_holidays()
    unioned = (live_or_cached or set()) | csv_local

    # refresh cache if we fetched live
    if live is not None and live:
        _save_cache(live)

    if persist_csv_if_updated:
        current = load_holidays()
        if unioned != current:
            save_holidays_to_csv(unioned)

    return unioned

# --- trading-day helpers ---------------------------------------------------
def is_trading_day_ist(d: dt.date, holidays: Set[dt.date]) -> bool:
    return d.weekday() < 5 and d not in holidays

def next_trading_day_ist(start: Optional[dt.date], holidays: Set[dt.date]) -> dt.date:
    """Return the next trading day on/after `start` (IST)."""
    cur = start or _now_ist().date()
    while not is_trading_day_ist(cur, holidays):
        cur = cur + dt.timedelta(days=1)
    return cur

def today_trading_status() -> dict:
    """Small utility to quickly check today’s status (uses cache/live+CSV union)."""
    hols = load_holidays_combined()
    today = _now_ist().date()
    return {
        "today": today.isoformat(),
        "is_trading_day": is_trading_day_ist(today, hols),
        "next_trading_day": next_trading_day_ist(today, hols).isoformat(),
        "total_known_holidays": len(hols),
    }
