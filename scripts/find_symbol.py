# scripts/find_symbol.py
from __future__ import annotations
import argparse, json, sys
from typing import Any, Dict, List

from utils.resolve import debug_candidates, resolve_nse_token

def _best_match(symbol: str) -> Dict[str, Any]:
    """
    Wrap resolve_nse_token() but never raise; return a structured payload.
    Expected success shape: {"ok": True, "tradingsymbol": "...", "token": "..."}
    """
    out: Dict[str, Any] = {"ok": False, "error": None, "tradingsymbol": None, "token": None}
    try:
        ts, token = resolve_nse_token(symbol)  # your helper
        out.update({"ok": True, "tradingsymbol": ts, "token": token})
    except Exception as e:
        out["error"] = str(e)
    return out

def _norm_candidate(item: Any) -> Dict[str, Any]:
    """
    Normalize a candidate into a dict for consistent JSON/text display.
    Supports str/tuple/dict from debug_candidates().
    """
    if isinstance(item, dict):
        # Keep common keys if present; include raw for completeness
        d = {
            "tradingsymbol": item.get("tradingsymbol") or item.get("symbol") or item.get("name"),
            "symboltoken": item.get("symboltoken") or item.get("token"),
            "exchange": item.get("exchange") or item.get("exch_seg"),
        }
        d["raw"] = item
        return d
    if isinstance(item, (list, tuple)) and len(item) >= 2:
        ts, token = item[0], item[1]
        return {"tradingsymbol": str(ts), "symboltoken": str(token), "raw": item}
    # fallback for strings or other shapes
    return {"display": str(item), "raw": item}

def _build_cli():
    p = argparse.ArgumentParser(description="Find NSE tradingsymbol & token by fuzzy query")
    p.add_argument("symbol", help="Query string, e.g. RELIANCE, TCS, INFY")
    p.add_argument("--limit", type=int, default=15, help="Max candidates to display (default 15)")
    p.add_argument("--json", action="store_true", help="Emit structured JSON")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    return p

def main(argv: List[str] | None = None) -> int:
    args = _build_cli().parse_args(argv)

    # 1) Best match (non-fatal)
    best = _best_match(args.symbol)

    # 2) Candidate list (never throw)
    try:
        hits = debug_candidates(args.symbol, limit=int(args.limit))
    except Exception as e:
        hits = []
        best.setdefault("errors", []).append(f"debug_candidates failed: {e}")

    cand_norm = [_norm_candidate(h) for h in (hits or [])]

    if args.json:
        payload = {"query": args.symbol, "best_match": best, "candidates": cand_norm}
        dump = json.dumps(payload, indent=(2 if args.pretty else None), ensure_ascii=False)
        print(dump)
    else:
        if best["ok"]:
            print(f"BEST MATCH: tradingsymbol={best['tradingsymbol']}, token={best['token']}")
        else:
            emsg = f"resolve_nse_token failed: {best.get('error') or 'no match'}"
            print(emsg)

        if cand_norm:
            print("\nCandidates:")
            for i, c in enumerate(cand_norm, 1):
                if "tradingsymbol" in c or "symboltoken" in c:
                    ts = c.get("tradingsymbol")
                    tok = c.get("symboltoken")
                    exch = c.get("exchange")
                    meta = f" [{exch}]" if exch else ""
                    print(f"{i:02d}. {ts} ({tok}){meta}")
                else:
                    print(f"{i:02d}. {c.get('display')}")
        else:
            print("No candidates.")

    # Exit code: 0 if we have either a best match or any candidates, else 1
    ok = bool(best.get("ok")) or bool(cand_norm)
    return 0 if ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
