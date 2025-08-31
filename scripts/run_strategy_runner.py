# scripts/run_strategy_runner.py
from __future__ import annotations
import argparse
from core.strategy_runner import main as runner_main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-s","--strategies", required=True, help="comma-separated strategy names")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    mode = "LIVE" if args.live else "DRY"
    runner_main(strategies=args.strategies, mode=mode)

if __name__ == "__main__":
    main()
