# scripts/check_hist_equity.py
from __future__ import annotations
import argparse, sys, time
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime
import pandas as pd
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.login import restore_or_login
from utils.history import get_recent_candles
from utils.instruments import load_instruments

DEFAULTS = {
    "exchange": "NSE",
    "interval": "FIFTEEN_MINUTE",  # matches your utils.history
    "bars": 300,
}

def _build_cli():
    p = argparse.ArgumentParser(description="Check recent historical candles for an NSE equity")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--symbol", help="Equity symbol (e.g., RELIANCE, TCS)")
    g.add_argument("--token", help="Symbol token (string from instruments master)")
    p.add_argument("--exchange", default=DEFAULTS["exchange"], help="Exchange segment (default NSE)")
    p.add_argument("--interval", default=DEFAULTS["interval"], help="Candle interval (e.g., ONE_MINUTE, FIFTEEN_MINUTE)")
    p.add_argument("--bars", type=int, default=DEFAULTS["bars"], help="Number of bars to fetch (default 300)")
    p.add_argument("--from", dest="from_ts", help="Start time (YYYY-MM-DD HH:MM, IST). Utils may ignore if unsupported.")
    p.add_argument("--to", dest="to_ts", help="End time (YYYY-MM-DD HH:MM, IST)")
    p.add_argument("--csv", dest="csv_path", help="Write result to CSV path if provided")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    return p

def _resolve_token(symbol: str, exchange: str) -> Optional[str]:
    """Lookup token for an equity symbol from instruments file."""
    try:
        df = load_instruments()
        # Typical columns: exch_seg/symbol/symboltoken, tradingsymbol for derivatives, etc.
        cols = {c.lower(): c for c in df.columns}
        exch_col = cols.get("exch_seg") or cols.get("exchange") or "exch_seg"
        sym_col  = cols.get("symbol") or cols.get("tradingsymbol") or "symbol"
        tok_col  = cols.get("symboltoken") or cols.get("token") or "symboltoken"

        c = df[(df[exch_col].str.upper() == exchange.upper()) & (df[sym_col].str.upper() == symbol.upper())]
        if not c.empty:
            tok = str(c.iloc[0][tok_col]).strip()
            return tok or None
        return None
    except Exception as e:
        logger.warning(f"instruments lookup failed: {e}")
        return None

def _parse_ist(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    raise ValueError(f"Invalid datetime format: {s} (want 'YYYY-MM-DD HH:MM')")

def _retry_fetch(smart, *, exchange: str, token: str, interval: str, bars: int,
                 from_ts: Optional[datetime], to_ts: Optional[datetime], attempts: int = 3) -> pd.DataFrame:
    last_err = None
    for i in range(1, attempts + 1):
        try:
            # Your utils.history.get_recent_candles signature may ignore from/to; that’s okay.
            df = get_recent_candles(
                smart,
                exchange=exchange,
                symboltoken=token,
                interval=interval,
                bars=bars,
                from_datetime=from_ts,  # if your util accepts; otherwise harmless
                to_datetime=to_ts,
            )
            if isinstance(df, pd.DataFrame):
                return df
            raise RuntimeError(f"Unexpected return type: {type(df)}")
        except Exception as e:
            last_err = e
            logger.warning(f"fetch attempt {i}/{attempts} failed: {e}")
            time.sleep(0.4 * i)
    raise RuntimeError(f"Could not fetch candles after {attempts} attempts: {last_err}")

def _summarize(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], int]:
    if df.empty:
        return None, None, 0
    ts_col = None
    for cand in ("time", "timestamp", "datetime", "Date", "date"):
        if cand in df.columns:
            ts_col = cand
            break
    if ts_col is None:
        return None, None, len(df)
    try:
        s = pd.to_datetime(df[ts_col])
        return s.min().isoformat(), s.max().isoformat(), len(df)
    except Exception:
        return None, None, len(df)

def main(argv=None):
    args = _build_cli().parse_args(argv)
    _lvl = "DEBUG" if args.verbose else "INFO"
    logger.remove()
    logger.add(sys.stdout, level=_lvl)

    exchange = args.exchange.upper()
    interval = args.interval.upper()
    bars     = int(args.bars)

    token = args.token
    if not token and args.symbol:
        token = _resolve_token(args.symbol, exchange)
        if not token:
            logger.error(f"Could not resolve token for {args.symbol} on {exchange}. Try --token explicitly.")
            sys.exit(2)
        logger.info(f"Resolved {args.symbol} ({exchange}) -> token={token}")

    from_ts = _parse_ist(args.from_ts)
    to_ts   = _parse_ist(args.to_ts)

    smart = restore_or_login()

    df = _retry_fetch(
        smart,
        exchange=exchange,
        token=token,
        interval=interval,
        bars=bars,
        from_ts=from_ts,
        to_ts=to_ts,
    )

    err = df.attrs.get("error") if hasattr(df, "attrs") else None
    start_iso, end_iso, nrows = _summarize(df)

    print(f"\nExchange={exchange}  Token={token}  Interval={interval}  Bars={bars}")
    if start_iso and end_iso:
        print(f"Span: {start_iso}  →  {end_iso}")
    print(f"rows={nrows}  error={err}\n")

    if not df.empty:
        # show last 5 rows neatly
        tail_cols = [c for c in ("time", "timestamp", "datetime", "date", "open", "high", "low", "close", "volume") if c in df.columns]
        print(df[tail_cols].tail(5).to_string(index=False))

    if args.csv_path:
        Path(args.csv_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.csv_path, index=False)
        print(f"\nSaved to {args.csv_path}")

if __name__ == "__main__":
    main()
