# scripts/test_hist.py
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger
from core.login import restore_or_login
from utils.history import get_recent_candles
from utils.resolve import resolve_nse_token

INTERVAL_ALIASES = {
    "1m":"ONE_MINUTE","3m":"THREE_MINUTE","5m":"FIVE_MINUTE","10m":"TEN_MINUTE",
    "15m":"FIFTEEN_MINUTE","30m":"THIRTY_MINUTE","60m":"ONE_HOUR","1h":"ONE_HOUR",
    "day":"ONE_DAY","1d":"ONE_DAY",
}

def _norm_interval(s: str) -> str:
    s = (s or "").strip().upper()
    return INTERVAL_ALIASES.get(s.lower(), s)

def build_cli():
    p = argparse.ArgumentParser(description="Quick historical candles probe")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--symbol", help="NSE symbol to resolve (e.g., RELIANCE)")
    g.add_argument("--token", help="Known symboltoken (string)")

    p.add_argument("--exchange", default="NSE", help="NSE/NFO (default NSE)")
    p.add_argument("--interval", default="FIFTEEN_MINUTE",
                   help="FIFTEEN_MINUTE / FIVE_MINUTE / 5m / 15m / ONE_DAY …")
    p.add_argument("--bars", type=int, default=300, help="How many candles to request")
    p.add_argument("--csv", help="Optional path to save candles as CSV")
    p.add_argument("--verbose", action="store_true")
    return p

def _diag(df: pd.DataFrame) -> None:
    if df.empty:
        print("No data. error:", df.attrs.get("error"))
        return
    # ensure time index and IST
    ts = pd.to_datetime(df["time"] if "time" in df.columns else df.index, errors="coerce", utc=True)
    if ts.tz is None: ts = ts.tz_localize("UTC")
    ts = ts.tz_convert("Asia/Kolkata")
    df2 = df.copy()
    df2.index = ts

    # gaps (non-uniform step detection)
    dt = df2.index.to_series().diff().dropna()
    gaps = dt[dt > dt.mode().iloc[0] * 1.01] if not dt.empty else pd.Series(dtype="timedelta64[ns]")
    nans = df2[["open","high","low","close"]].isna().sum().sum()

    print(f"rows={len(df2)}, error={df.attrs.get('error')}, NaNs_in_OHLC={int(nans)}, gaps={len(gaps)}")
    print(df2.tail(5).to_string(index=False))

def main(argv=None):
    args = build_cli().parse_args(argv)
    logger.remove()
    logger.add(sys.stdout, level=("DEBUG" if args.verbose else "INFO"))

    s = restore_or_login()

    exch = args.exchange.upper().strip()
    interval = _norm_interval(args.interval)

    if args.symbol:
        try:
            tsym, token = resolve_nse_token(args.symbol)
        except Exception as e:
            print(f"resolve_nse_token failed for {args.symbol}: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        tsym, token = None, args.token

    df = get_recent_candles(s, exchange=exch, symboltoken=str(token), interval=interval, bars=int(args.bars))

    _diag(df)

    if args.csv and not df.empty:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.csv, index=False)
        print(f"Saved CSV → {args.csv}")

if __name__ == "__main__":
    main()
