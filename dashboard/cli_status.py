# dashboard/cli_status.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path

TRADES_CSV = Path("data/trades.csv")

def main():
    print("Angel Auto Trader — CLI Status")
    print("Now:", datetime.now().isoformat(timespec="seconds"))
    if TRADES_CSV.exists():
        lines = TRADES_CSV.read_text(encoding="utf-8", errors="ignore").strip().splitlines()
        print(f"Trades logged: {max(0, len(lines)-1)}")
    else:
        print("Trades CSV not found yet.")
    # TODO: show open positions, daily P&L, risk flags
if __name__ == "__main__":
    main()
