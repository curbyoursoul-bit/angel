# scripts/orb_scan.py
from __future__ import annotations

import os, math, csv, sys, time, argparse
from typing import List, Tuple, Optional, Dict, Set
import pandas as pd
from loguru import logger

from core.login import restore_or_login
from utils.instruments import pick_nse_equity_tokens
from utils.history import get_recent_candles

# ---------- defaults via env (still supported) ----------
ENV_SYMBOLS  = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY")
ENV_INTERVAL = os.getenv("STRAT_INTERVAL", "FIFTEEN_MINUTE")
ENV_BARS     = int(os.getenv("STRAT_BARS", "600"))
ENV_ORB_MIN  = int(os.getenv("STRAT_ORB_MIN", "15"))
ENV_OUT_CSV  = os.getenv("ORB_SCAN_OUT", "data/orb_signals.csv")
ENV_CONFIRM  = float(os.getenv("STRAT_ORB_CONFIRM_BPS", "5")) / 10000.0  # 5 bps default

_INTERVAL_MIN = {
    "ONE_MINUTE": 1, "THREE_MINUTE": 3, "FIVE_MINUTE": 5, "TEN_MINUTE": 10,
    "FIFTEEN_MINUTE": 15, "THIRTY_MINUTE": 30, "ONE_HOUR": 60, "ONE_DAY": 1440,
}

IST = "Asia/Kolkata"

# ---------- CLI ----------
def _build_cli():
    p = argparse.ArgumentParser(description="ORB scanner for NSE equities (IST, 09:15–15:30)")
    p.add_argument("--symbols", default=ENV_SYMBOLS, help="CSV list (default from STRAT_SYMBOLS)")
    p.add_argument("--interval", default=ENV_INTERVAL, help="Candle interval (e.g. FIFTEEN_MINUTE)")
    p.add_argument("--bars", type=int, default=ENV_BARS, help="Bars to fetch (default from STRAT_BARS)")
    p.add_argument("--orb-min", type=int, default=ENV_ORB_MIN, help="Opening range minutes (default STRAT_ORB_MIN)")
    p.add_argument("--confirm-bps", type=float, default=ENV_CONFIRM*10000.0,
                   help="Break confirmation in basis points (default STRAT_ORB_CONFIRM_BPS or 5)")
    p.add_argument("--exchange", default="NSE", help="Exchange segment (default NSE)")
    p.add_argument("--out", default=ENV_OUT_CSV, help="Output CSV (default ORB_SCAN_OUT)")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    return p

# ---------- helpers ----------
def _session_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    # time column or index -> tz-aware IST
    if "time" in df.columns:
        ts = pd.to_datetime(df["time"], errors="coerce", utc=True)
    else:
        ts = pd.to_datetime(df.index, errors="coerce", utc=True)
    # if not UTC, assume naive local and localize; then convert to IST
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")
    ts = ts.dt.tz_convert(IST)

    w = df.copy()
    w.index = ts
    for c in ("open", "high", "low", "close", "volume"):
        if c in w.columns:
            w[c] = pd.to_numeric(w[c], errors="coerce")
    w = w.dropna(subset=["open","high","low","close"])
    # clamp to trading hours
    start_h, start_m = 9, 15
    end_h, end_m = 15, 30
    mask = (w.index.time >= pd.Timestamp(hour=start_h, minute=start_m).time()) & \
           (w.index.time <= pd.Timestamp(hour=end_h, minute=end_m).time())
    return w.loc[mask]

def _split_by_session(df: pd.DataFrame) -> List[pd.DataFrame]:
    if df is None or df.empty:
        return []
    days = sorted({d for d in df.index.date})
    out = []
    for d in days:
        start = pd.Timestamp(d, tz=IST) + pd.Timedelta(hours=9, minutes=15)
        end   = pd.Timestamp(d, tz=IST) + pd.Timedelta(hours=15, minutes=30)
        one = df.loc[start:end]
        if not one.empty:
            out.append(one.sort_index())
    return out

def _opening_range(df: pd.DataFrame, minutes: int, interval_str: str) -> Optional[Tuple[float,float,int]]:
    mins = _INTERVAL_MIN.get(interval_str.upper(), 5)
    bars_needed = max(1, math.ceil(minutes / mins))
    if len(df) < bars_needed:
        return None
    head = df.sort_index().iloc[:bars_needed]
    return float(head["high"].max()), float(head["low"].min()), bars_needed

def _first_breakout(sess: pd.DataFrame, hi: float, lo: float, start_idx: int, pad_bps: float = 0.0005
                    ) -> Optional[Tuple[pd.Timestamp,str,float]]:
    """
    pad_bps=0.0005 ~ 5 bps confirmation: price must exceed hi*(1+pad) / below lo*(1-pad).
    """
    up = hi * (1.0 + pad_bps)
    dn = lo * (1.0 - pad_bps)
    tail = sess.sort_index().iloc[start_idx:]
    for ts, row in tail.iterrows():
        c = float(row["close"])
        if c > up:
            return ts, "BUY", c
        if c < dn:
            return ts, "SELL", c
    return None

def _exit_price_same_day_close(sess: pd.DataFrame) -> float:
    return float(sess.sort_index().iloc[-1]["close"])

def _pnl(side: str, entry: float, exit_px: float) -> float:
    return (exit_px - entry) if side == "BUY" else (entry - exit_px)

def _retry_candles(smart, *, exchange: str, token: str, interval: str, bars: int, attempts: int = 3) -> pd.DataFrame:
    last = None
    for i in range(1, attempts + 1):
        try:
            df = get_recent_candles(smart, exchange, token, interval, bars=bars)
            if isinstance(df, pd.DataFrame):
                return df
            raise RuntimeError(f"Unexpected return type: {type(df)}")
        except Exception as e:
            last = e
            logger.warning(f"get_recent_candles failed (attempt {i}/{attempts}): {e}")
            time.sleep(0.35 * i)
    raise RuntimeError(f"Could not fetch candles: {last}")

def _load_existing_keys(csv_path: str) -> Set[Tuple[str,str]]:
    keys: Set[Tuple[str,str]] = set()
    try:
        if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
            with open(csv_path, "r", newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    d = (row.get("date") or "").strip()
                    s = (row.get("symbol") or "").strip().upper()
                    if d and s:
                        keys.add((d, s))
    except Exception:
        pass
    return keys

# ---------- main ----------
def main(argv=None):
    args = _build_cli().parse_args(argv)
    logger.remove()
    logger.add(lambda m: print(m, end=""), level=("DEBUG" if args.verbose else "INFO"))

    symbols = [s.strip().upper() for s in args.symbols.replace(" ", "").split(",") if s.strip()]
    interval = args.interval.upper()
    bars = int(args.bars)
    orb_min = int(args.orb_min)
    confirm_bps = float(args.confirm_bps) / 10000.0
    out_csv = args.out
    exchange = args.exchange.upper()

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    exists = os.path.exists(out_csv)
    existing_keys = _load_existing_keys(out_csv)

    # header if new file
    if not exists:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "date","symbol","side","entry_time","entry_close","exit_close",
                "pnl_same_day","orb_high","orb_low","orb_minutes","interval","confirm_bps"
            ])

    smart = restore_or_login()
    sym2tok: Dict[str,str] = pick_nse_equity_tokens(symbols)
    if not sym2tok:
        logger.error("No NSE equity tokens resolved.")
        return 2

    totals: List[float] = []
    saved = 0

    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        for sym in symbols:
            tok = sym2tok.get(sym)
            if not tok:
                logger.error(f"{sym}: token not found")
                continue

            try:
                raw = _retry_candles(smart, exchange=exchange, token=tok, interval=interval, bars=bars)
            except Exception as e:
                logger.error(f"{sym}: fetch failed: {e}")
                continue

            if raw is None or raw.empty:
                reason = getattr(raw, "attrs", {}).get("error") if isinstance(raw, pd.DataFrame) else "unknown"
                logger.error(f"{sym}: empty candles (reason={reason})")
                continue

            sess_df = _session_df(raw)
            sessions = _split_by_session(sess_df)
            if not sessions:
                logger.info(f"{sym}: no in-session bars found in window")
                continue

            print(f"\n=== {sym} ===")
            for s in sessions[-10:]:
                day = str(s.index[0].date())  # yyyy-mm-dd
                # skip if already logged this (date, sym)
                if (day, sym) in existing_keys:
                    print(f"{day}  already logged; skipping")
                    continue

                rng = _opening_range(s, orb_min, interval)
                if not rng:
                    print(f"{day}  insufficient bars for ORB[{orb_min}m]")
                    continue
                hi, lo, k = rng
                hit = _first_breakout(s, hi, lo, k, pad_bps=confirm_bps)
                if hit:
                    ts, side, entry_close = hit
                    exit_close = _exit_price_same_day_close(s)
                    pnl = _pnl(side, entry_close, exit_close)
                    totals.append(pnl)
                    print(f"{day}  ORB[{orb_min}m] ({lo:.2f},{hi:.2f}) -> {side} @ {ts.time()} "
                          f"entry={entry_close:.2f} | exit={exit_close:.2f} pnl={pnl:.2f}")
                    w.writerow([
                        day, sym, side, str(ts), f"{entry_close:.2f}", f"{exit_close:.2f}", f"{pnl:.2f}",
                        f"{hi:.2f}", f"{lo:.2f}", orb_min, interval, f"{confirm_bps*10000:.1f}"
                    ])
                    saved += 1
                    existing_keys.add((day, sym))
                else:
                    print(f"{day}  ORB[{orb_min}m] ({lo:.2f},{hi:.2f}) -> no breakout")

    if totals:
        s = sum(totals)
        print(f"\nSaved {saved} signals to {out_csv}. Sum PnL (toy) = {s:.2f}, mean = {s/len(totals):.2f}")
    else:
        print("\nNo signals saved.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
