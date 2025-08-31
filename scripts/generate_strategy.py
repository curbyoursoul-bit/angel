# scripts/generate_strategy.py
from __future__ import annotations
import argparse, json
from loguru import logger
from agent.autocode import generate_strategy

def main():
    ap = argparse.ArgumentParser(description="Generate a strategy from a template")
    ap.add_argument("--name", required=True, help="Output strategy name (no extension)")
    ap.add_argument("--template", required=True, choices=["ema_crossover", "bollinger_rsi"])
    ap.add_argument("--params", default="{}", help='JSON dict of params e.g. {"fast":5,"slow":20}')
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    try:
        params = json.loads(args.params)
        path = generate_strategy(args.name, args.template, params, overwrite=args.overwrite)
        logger.success(f"Created: {path}")
    except Exception as e:
        logger.error(e)
        raise SystemExit(2)

if __name__ == "__main__":
    main()
