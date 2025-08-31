
from __future__ import annotations
import os, sys
import pandas as pd
from typing import Any, Dict, Optional, Union, Tuple
from datetime import datetime, timedelta

NAME = "ema_x_5_20_banknifty_5m"
META = {
    "template": "ema_crossover",
    "fast": 5,
    "slow": 20,
    "symbol": "BANKNIFTY",
    "timeframe": "5m"
}

# ---------- internals ----------
def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _signal(df: pd.DataFrame, fast: int, slow: int) -> str:
    if "close" not in df.columns or len(df) < max(fast, slow) + 2:
        return "NO_OP"
    ema_f = _ema(df["close"], fast)
    ema_s = _ema(df["close"], slow)
    prev_up = bool(ema_f.iloc[-2] > ema_s.iloc[-2])
    curr_up = bool(ema_f.iloc[-1] > ema_s.iloc[-1])
    if (not prev_up) and curr_up:
        return "BUY"
    if prev_up and (not curr_up):
        return "SELL"
    return "HOLD"

# ---------- pick up engine CLI overrides from sys.argv ----------
def _discover_runtime_args():
    """
    Returns (interval:str|None, bars:int|None, symbols:str|None)
    """
    argv = sys.argv or []
    interval = None
    bars = None
    symbols = None
    try:
        if "--interval" in argv:
            i = argv.index("--interval")
            interval = argv[i + 1]
        if "--bars" in argv:
            i = argv.index("--bars")
            bars = int(argv[i + 1])
        if "--symbols" in argv:
            i = argv.index("--symbols")
            symbols = argv[i + 1]
    except Exception:
        pass
    # env fallbacks if your engine exports them
    interval = interval or os.getenv("ENGINE_INTERVAL") or os.getenv("INTERVAL")
    if bars is None:
        try:
            bars = int(os.getenv("ENGINE_BARS") or os.getenv("BARS") or "0")
        except Exception:
            bars = None
    symbols = symbols or os.getenv("ENGINE_SYMBOLS") or os.getenv("SYMBOLS")
    return interval, bars, symbols

# ---------- candle loader (only if we were passed SmartConnect) ----------
_INTERVAL_MAP = {
    "1m": "ONE_MINUTE",
    "3m": "THREE_MINUTE",
    "5m": "FIVE_MINUTE",
    "10m": "TEN_MINUTE",
    "15m": "FIFTEEN_MINUTE",
    "30m": "THIRTY_MINUTE",
    "60m": "ONE_HOUR",
    "1h": "ONE_HOUR",
    "day": "ONE_DAY",
    "1d": "ONE_DAY",
}

def _guess_token_for_symbol(symbol: str) -> Optional[Tuple[str, str, str]]:
    """
    Returns (exchange, symboltoken, tradingsymbol).
    Try NSE index first; if empty, fallback to NFO nearest FUT.
    """
    symbol_u = symbol.upper()

    try:
        from utils.instruments import _read_instruments_df  # type: ignore
        df = _read_instruments_df()

        # 1) NSE index
        nse_idx = df[
            (df.get("exch_seg", "").astype(str).str.upper() == "NSE")
            & (
                (df.get("symbol", "").astype(str).str.upper() == symbol_u)
                | (df.get("tradingsymbol", "").astype(str).str.upper() == symbol_u)
                | (df.get("name", "").astype(str).str.upper().str.contains(symbol_u, na=False))
            )
        ]
        if len(nse_idx):
            row = nse_idx.iloc[0]
            tok = str(row.get("token") or row.get("symboltoken") or "").strip()
            tsym = str(row.get("tradingsymbol") or symbol_u)
            if tok:
                return ("NSE", tok, tsym)

        # 2) NFO FUT (nearest expiry >= today)
        nfo = df[
            (df.get("exch_seg", "").astype(str).str.upper() == "NFO")
            & (df.get("name", "").astype(str).str.upper().str.contains(symbol_u, na=False))
            & (df.get("instrumenttype", "").astype(str).str.upper().str.contains("FUT", na=False))
        ].copy()

        if "expiry" in nfo.columns:
            def _parse_exp(x):
                try: return pd.to_datetime(x)
                except Exception: return pd.NaT
            nfo["expiry_dt"] = nfo["expiry"].apply(_parse_exp)
            today = pd.Timestamp.today().normalize()
            nfo = nfo.sort_values(by=["expiry_dt"], key=lambda s: s.fillna(pd.Timestamp.max))
            nfo_valid = nfo[nfo["expiry_dt"].fillna(pd.Timestamp.min) >= today]
            pick = nfo_valid.iloc[0] if len(nfo_valid) else (nfo.iloc[0] if len(nfo) else None)
        else:
            pick = nfo.iloc[0] if len(nfo) else None

        if pick is not None:
            tok = str(pick.get("token") or pick.get("symboltoken") or "").strip()
            tsym = str(pick.get("tradingsymbol") or f"{symbol_u}")
            if tok:
                return ("NFO", tok, tsym)

    except Exception:
        pass

    # 3) Hard fallbacks (dataset dependent)
    if symbol_u == "BANKNIFTY":
        return ("NSE", "26009", "BANKNIFTY")
    if symbol_u == "NIFTY":
        return ("NSE", "256265", "NIFTY")
    return None

def _load_df_from_smart(smart: Any, symbol: str, interval_txt: str, bars: int) -> pd.DataFrame:
    interval = _INTERVAL_MAP.get(interval_txt.lower(), "FIVE_MINUTE")
    intraday = interval != "ONE_DAY"

    if intraday:
        lookback_days = max(10, min(30, int(max(bars or 300, 300) / 60)))
    else:
        lookback_days = max(365, int((bars or 300) * 2))

    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=lookback_days)

    res = _guess_token_for_symbol(symbol)
    if not res:
        return pd.DataFrame()
    exchange, token, tsym = res

    def _fetch(exch: str, tok: str) -> pd.DataFrame:
        params = {
            "exchange": exch,             # NSE or NFO
            "symboltoken": tok,
            "interval": interval,
            "fromdate": from_dt.strftime("%Y-%m-%d 09:15"),
            "todate": to_dt.strftime("%Y-%m-%d %H:%M"),
        }
        data = None
        try:
            resp = smart.getCandleData(params)
            data = (resp or {}).get("data")
        except Exception:
            try:
                resp = smart.getCandleData(params)
                data = (resp or {}).get("data")
            except Exception:
                data = None

        if not data or not isinstance(data, (list, tuple)):
            return pd.DataFrame()

        rows = []
        for r in data:
            if not isinstance(r, (list, tuple)) or len(r) < 6:
                continue
            rows.append({
                "time": pd.to_datetime(r[0]),
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
            })
        out = pd.DataFrame(rows).sort_values("time").reset_index(drop=True)
        if bars and len(out) > bars:
            out = out.iloc[-bars:].reset_index(drop=True)
        return out

    df = _fetch(exchange, token)

    # If empty and first try was NSE index, fallback to NFO FUT
    if (df is None or df.empty) and exchange == "NSE":
        try:
            from utils.instruments import _read_instruments_df  # type: ignore
            ins = _read_instruments_df()
            nfo = ins[
                (ins.get("exch_seg", "").astype(str).str.upper() == "NFO")
                & (ins.get("name", "").astype(str).str.upper().str.contains(symbol.upper(), na=False))
                & (ins.get("instrumenttype", "").astype(str).str.upper().str.contains("FUT", na=False))
            ].copy()
            if "expiry" in nfo.columns:
                def _parse_exp(x):
                    try: return pd.to_datetime(x)
                    except Exception: return pd.NaT
                nfo["expiry_dt"] = nfo["expiry"].apply(_parse_exp)
                today = pd.Timestamp.today().normalize()
                nfo = nfo.sort_values(by=["expiry_dt"], key=lambda s: s.fillna(pd.Timestamp.max))
                nfo_valid = nfo[nfo["expiry_dt"].fillna(pd.Timestamp.min) >= today]
                pick = nfo_valid.iloc[0] if len(nfo_valid) else (nfo.iloc[0] if len(nfo) else None)
            else:
                pick = nfo.iloc[0] if len(nfo) else None
            if pick is not None:
                tok2 = str(pick.get("token") or pick.get("symboltoken") or "").strip()
                if tok2:
                    df = _fetch("NFO", tok2)
        except Exception:
            pass

    return df

# ---------- public run() ----------
def run(arg: Union[pd.DataFrame, Any]) -> Dict[str, Any] | None:
    """
    - If a pandas DataFrame is passed, use it directly.
    - If a SmartConnect (or any object) is passed, fetch candles and run.
    Honors engine CLI flags (if present): --interval/--bars/--symbols.
    """
    fast = META["fast"]
    slow = META["slow"]

    # defaults (from template params)
    symbol = META["symbol"]
    timeframe = META["timeframe"]
    bars = 300

    cli_interval, cli_bars, cli_symbols = _discover_runtime_args()
    if cli_symbols:
        symbol = cli_symbols.split(",")[0].strip() or symbol
    if cli_interval:
        timeframe = cli_interval
    if isinstance(cli_bars, int) and cli_bars > 0:
        bars = cli_bars

    if isinstance(arg, pd.DataFrame):
        df = arg
        attempted = f"df-passed({len(df)})"
    else:
        df = _load_df_from_smart(arg, symbol, timeframe, bars)
        attempted = f"{symbol}:{timeframe} rows={0 if df is None else len(df)}"
        if (df is None or df.empty) and timeframe.lower() not in ("1d", "day"):
            df = _load_df_from_smart(arg, symbol, "1d", max(bars, 200))
            attempted += f" -> retry:1d rows={0 if df is None else len(df)}"

    if df is None or df.empty:
        return {"name": NAME, "meta": {**META, "symbol": symbol, "timeframe": timeframe},
                "signal": "NO_OP", "reason": "no_candles", "debug": attempted}

    sig = _signal(df, fast, slow)
    return {"name": NAME, "meta": {**META, "symbol": symbol, "timeframe": timeframe}, "signal": sig}
