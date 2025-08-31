from __future__ import annotations
"""
Fetch OHLCV from Angel One SmartAPI and save as CSV for backtests.

Usage (Windows cmd):
  python -m scripts.fetch_smartapi_ohlcv --symbol "NIFTY BANK" --interval 5min --days 30 --out data\BANKNIFTY_5m.csv --debug
  # or force token directly:
  python -m scripts.fetch_smartapi_ohlcv --token 99926009 --exchange NSE --interval 5min --days 30 --out data\BANKNIFTY_5m.csv --debug

Required env (in .env at repo root or set in shell):
  API_KEY=.....
  CLIENT_CODE=.....
  PASSWORD=.....         # falls back to PIN/MPIN if PASSWORD missing
  TOTP_SECRET=BASE32SECRET

Install once:
  pip install smartapi-python pyotp python-dotenv
"""

import os, sys, csv, time, argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

# -------------------- load .env robustly --------------------
def _load_env():
    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path)
        return
    except Exception:
        pass
    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.split("#", 1)[0].strip().strip('"').strip("'")
            os.environ.setdefault(k.strip(), v)
    except Exception:
        pass

_load_env()

# -------------------- tz + time parse -----------------------
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = None

def iso_ist(ts_str: str) -> str:
    """Accept 'YYYY-MM-DD HH:MM[:SS]' OR ISO8601 ('...T...+05:30') -> ISO with seconds."""
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None and IST:
            dt = dt.replace(tzinfo=IST)
        return dt.isoformat(timespec="seconds")
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(ts_str, fmt)
            if IST:
                dt = dt.replace(tzinfo=IST)
            return dt.isoformat(timespec="seconds")
        except Exception:
            continue
    return ts_str

# -------------------- SmartAPI import -----------------------
SmartConnect = None
try:
    from SmartApi import SmartConnect   # common
except Exception:
    try:
        from smartapi import SmartConnect
    except Exception:
        pass

# -------------------- intervals & tokens --------------------
INTERVAL_MAP = {
    "1min":"ONE_MINUTE","3min":"THREE_MINUTE","5min":"FIVE_MINUTE","10min":"TEN_MINUTE",
    "15min":"FIFTEEN_MINUTE","30min":"THIRTY_MINUTE","60min":"ONE_HOUR","1h":"ONE_HOUR",
    "day":"ONE_DAY","1d":"ONE_DAY",
}

# Known index tokens (override with --token if needed)
# NOTE: Your account returns data for NIFTY BANK as 99926009 via instruments CSV.
KNOWN_TOKENS = {
    "BANKNIFTY": ("NSE", "99926009"),
    "NIFTYBANK": ("NSE", "99926009"),  # handles "NIFTY BANK" after space removal
    "NIFTY":     ("NSE", "256265"),    # NIFTY 50
    "NIFTY50":   ("NSE", "256265"),
    "FINNIFTY":  ("NSE", "257801"),
}

# Some environments require NSE index candles via NSE_INDICES
INDEX_EXCH_ALTS = ["NSE", "NSE_INDICES"]

# -------------------- helpers --------------------------------
def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def load_token_from_csv(instruments_csv: str, symbol_contains: str) -> Optional[Tuple[str, str]]:
    p = Path(instruments_csv)
    if not p.exists():
        return None
    key = symbol_contains.lower()
    with p.open("r", newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            syms = [row.get(k, "") for k in ("symbol", "tradingsymbol", "name")]
            if any(key in str(s).lower() for s in syms):
                exch = row.get("exchange") or row.get("exch") or "NSE"
                token = row.get("token") or row.get("symboltoken") or row.get("tokenNumber")
                if token:
                    return (str(exch), str(token))
    return None

def chunk_ranges(start_dt: datetime, end_dt: datetime, max_days: int) -> List[Tuple[datetime, datetime]]:
    out = []
    cur = start_dt
    step = timedelta(days=max_days)
    while cur < end_dt:
        nxt = min(cur + step, end_dt)
        out.append((cur, nxt))
        cur = nxt
    return out

def _call_historic(sc, params):
    if hasattr(sc, "getCandleData"):
        return sc.getCandleData(params)
    if hasattr(sc, "getCandleDataV2"):
        return sc.getCandleDataV2(params)
    raise RuntimeError("SmartConnect has no getCandleData* method")

def fetch_candles(sc: "SmartConnect", exchange: str, symboltoken: str, interval: str,
                  start_dt: datetime, end_dt: datetime, throttle_s: float = 0.35, debug: bool=False) -> List[List]:
    frm = start_dt.strftime("%Y-%m-%d %H:%M")
    to  = end_dt.strftime("%Y-%m-%d %H:%M")
    params = {"exchange": exchange, "symboltoken": symboltoken, "interval": interval,
              "fromdate": frm, "todate": to}
    try:
        resp = _call_historic(sc, params)
        data = None
        if isinstance(resp, dict):
            data = resp.get("data") or resp.get("Data")
            if isinstance(data, dict) and "candles" in data:
                data = data["candles"]
        else:
            data = resp
        if debug:
            n = len(data or [])
            print(f"[fetch] {exchange}:{symboltoken} -> {n} rows")
        if not data:
            return []
        time.sleep(throttle_s)
        return data
    except Exception as e:
        if debug:
            print(f"[fetch] ERROR {exchange}:{symboltoken} -> {e}")
        return []

def write_csv(rows: List[List], out_csv: str):
    ensure_dir(out_csv)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for r in rows:
            ts_str, o, h, l, c, v = r[:6]
            w.writerow([iso_ist(ts_str), o, h, l, c, v])

# -------------------- main ---------------------------------
def main():
    ap = argparse.ArgumentParser(description="Fetch OHLCV from Angel One SmartAPI and save CSV for backtests.")
    ap.add_argument("--symbol", default="BANKNIFTY", help="BANKNIFTY / 'NIFTY BANK' / NIFTY / FINNIFTY (ignored if --token given)")
    ap.add_argument("--exchange", default=None, help="Override exchange (e.g. NSE or NSE_INDICES)")
    ap.add_argument("--token", default=None, help="Direct symboltoken (overrides lookup)")
    ap.add_argument("--instruments_csv", default="data/OpenAPIScripMaster.csv", help="For token lookup if needed")
    ap.add_argument("--interval", default="5min", choices=list(INTERVAL_MAP.keys()))
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--out", default="data/BANKNIFTY_5m.csv")
    ap.add_argument("--chunk_days", type=int, default=30, help="Angel caps ~30 days per intraday request")
    ap.add_argument("--dry_run", action="store_true")
    ap.add_argument("--debug", action="store_true", default=False)

    args = ap.parse_args()

    # Resolve (exchange, token)
    exch, token = None, None
    if args.token:
        exch = args.exchange or "NSE"
        token = str(args.token)
    else:
        key = args.symbol.upper().replace(" ", "")
        if key in KNOWN_TOKENS:
            exch, token = KNOWN_TOKENS[key]
        if (exch is None or token is None) and args.instruments_csv:
            hit = load_token_from_csv(args.instruments_csv, args.symbol)
            if hit:
                exch, token = hit
        if exch is None or token is None:
            print(f"[fetch] ERROR: Could not resolve token for '{args.symbol}'. Provide --token or a proper instruments CSV.", file=sys.stderr)
            sys.exit(2)
        if args.exchange:
            exch = args.exchange

    ivl = INTERVAL_MAP[args.interval]
    now_local = datetime.now(IST) if IST else datetime.now()
    start_dt = now_local - timedelta(days=args.days)
    end_dt = now_local
    chunks = chunk_ranges(start_dt, end_dt, args.chunk_days)

    print(f"[fetch] {args.symbol} ({exch}, token={token}) | {args.interval} | {len(chunks)} chunk(s) over last {args.days}d")
    print(f"[fetch] Writing -> {args.out}")

    # Load credentials
    api_key     = os.getenv("API_KEY", "")
    client_code = os.getenv("CLIENT_CODE", "")
    password    = os.getenv("PASSWORD") or os.getenv("PIN") or os.getenv("MPIN") or ""
    totp_secret = os.getenv("TOTP_SECRET", "")

    if args.dry_run:
        ensure_dir(args.out)
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["timestamp","open","high","low","close","volume"])
        print(f"[fetch] Dry-run. Creds present? API={bool(api_key)} CLIENT={bool(client_code)} PASS/PIN={bool(password)} TOTP={bool(totp_secret)}")
        return

    if SmartConnect is None:
        print("[fetch] ERROR: SmartAPI SDK not found. Install with: pip install smartapi-python", file=sys.stderr)
        sys.exit(2)

    if not (api_key and client_code and password and totp_secret):
        print("[fetch] ERROR: Missing API_KEY / CLIENT_CODE / PASSWORD(or PIN/MPIN) / TOTP_SECRET in environment.", file=sys.stderr)
        print(f"         Seen -> API={bool(api_key)} CLIENT={bool(client_code)} PASS/PIN={bool(password)} TOTP={bool(totp_secret)}", file=sys.stderr)
        sys.exit(2)

    # TOTP
    try:
        import pyotp
        totp = pyotp.TOTP(totp_secret).now()
    except Exception:
        print("[fetch] ERROR: pyotp not installed or invalid TOTP_SECRET. Install: pip install pyotp", file=sys.stderr)
        sys.exit(2)

    sc = SmartConnect(api_key=api_key)
    try:
        login_res = sc.generateSession(client_code, password, totp)
        ok = False
        if isinstance(login_res, dict):
            st = str(login_res.get("status", "")).lower()
            ok = (login_res.get("status") is True) or (st in {"success","ok","true"})
        if not ok:
            print(f"[fetch] ERROR: login failed: {login_res}", file=sys.stderr)
            sys.exit(2)

        # Try primary exchange first; if zero rows and looks like an index, try alternates
        all_rows: List[List] = []
        try_exchanges = [exch]
        if token in {"256265","260105","257801","99926009"} or "NIFTY" in args.symbol.upper():
            for alt in INDEX_EXCH_ALTS:
                if alt not in try_exchanges:
                    try_exchanges.append(alt)

        for ex in try_exchanges:
            if args.debug:
                print(f"[fetch] Trying exchange={ex}")
            rows_accum: List[List] = []
            for (frm, to) in chunks:
                rows = fetch_candles(sc, ex, token, ivl, frm, to, debug=args.debug)
                rows_accum.extend(rows)
            if rows_accum:
                all_rows = rows_accum
                exch = ex
                break

        # Dedup & sort
        seen = set(); uniq = []
        for r in all_rows:
            if not r:
                continue
            key = r[0]
            if key in seen: 
                continue
            seen.add(key); uniq.append(r)
        uniq.sort(key=lambda x: x[0])

        if not uniq:
            raise RuntimeError("Historic API returned 0 rows. Likely causes: wrong token/exchange for index, missing Historic Data permission, or temporary rate-limit.")

        write_csv(uniq, args.out)
        print(f"[fetch] Wrote {len(uniq)} rows to {args.out}")

    finally:
        try:
            sc.terminateSession(client_code)
        except Exception:
            pass

if __name__ == "__main__":
    main()
