# scripts/test_candles.py
from __future__ import annotations
import sys
from core.login import restore_or_login
from utils.resolve import resolve_nse_token
from utils.history import get_recent_candles

def main():
    s = restore_or_login()

    try:
        ts, token = resolve_nse_token("RELIANCE")
    except Exception as e:
        print(f"resolve_nse_token failed: {e}", file=sys.stderr)
        sys.exit(1)

    df = get_recent_candles(
        s,
        exchange="NSE",
        symboltoken=token,
        interval="FIVE_MINUTE",  # use your enum-like string, not "5m"
        bars=20,
    )

    if df.empty:
        print(f"No candles returned. Error? {df.attrs.get('error')}")
        return

    print(f"rows={len(df)}")
    print(df.tail(5).to_string(index=False))

if __name__ == "__main__":
    main()
