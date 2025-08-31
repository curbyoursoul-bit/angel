# scripts/peek_broker_state.py
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.login import restore_or_login


# -------- helpers --------
def _as_dict(x: Any) -> Dict[str, Any]:
    if isinstance(x, dict): return x
    if isinstance(x, str):
        try: return json.loads(x)
        except Exception: return {"status": None, "raw": x}
    return {"status": None, "raw": x}

def _ok(d: Dict[str, Any]) -> bool:
    s = d.get("status")
    if isinstance(s, bool): return s
    if isinstance(s, str): return s.strip().lower() in {"true","success","ok"}
    if str(d.get("message","")).strip().upper() == "SUCCESS": return True
    return False

def _fetch_orderbook(smart) -> List[Dict[str, Any]]:
    fn = getattr(smart, "orderBook", None)
    if not callable(fn): return []
    d = _as_dict(fn())
    data = d.get("data")
    return data if isinstance(data, list) else []

def _positions_any(smart) -> List[Dict[str, Any]]:
    # Try common method names
    for name in ("positionBook", "positions", "position", "getPositions"):
        fn = getattr(smart, name, None)
        if callable(fn):
            try:
                resp = fn()
                d = _as_dict(resp)
                data = d.get("data", resp if isinstance(resp, list) else None)
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    for k in ("netPositions","positions","NetPositions"):
                        arr = data.get(k)
                        if isinstance(arr, list):
                            return arr
            except Exception:
                continue
    return []

def _summarize_orders(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_status, by_variety = {}, {}
    for r in rows:
        st = str(r.get("status") or r.get("orderstatus") or "").strip().lower()
        vt = str(r.get("variety") or "NORMAL").strip().upper()
        by_status[st] = by_status.get(st, 0) + 1
        by_variety[vt] = by_variety.get(vt, 0) + 1
    return {"count": len(rows), "by_status": by_status, "by_variety": by_variety}

def _summarize_positions(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    tot_legs = len(rows)
    open_legs = 0
    segs = {}
    for r in rows:
        try:
            q = int(r.get("netqty") or r.get("netQty") or r.get("net_quantity") or 0)
            if q != 0: open_legs += 1
        except Exception:
            pass
        seg = str(r.get("exchange") or r.get("exch_seg") or r.get("exch") or "NSE").upper()
        segs[seg] = segs.get(seg, 0) + 1
    return {"count": tot_legs, "open_qty_legs": open_legs, "by_segment": segs}

def _redact_tokens(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k,v in obj.items():
            if str(k).lower() in {"symboltoken","token"}:
                out[k] = "***"
            else:
                out[k] = _redact_tokens(v)
        return out
    if isinstance(obj, list):
        return [_redact_tokens(x) for x in obj]
    return obj


# -------- CLI --------
def build_cli():
    p = argparse.ArgumentParser(description="Peek SmartAPI order book & positions safely")
    p.add_argument("--json", action="store_true", help="Emit one JSON object (summary + raw)")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    p.add_argument("--no-redact", action="store_true", help="Do not redact symbol tokens in raw dump")
    p.add_argument("--orders-only", action="store_true", help="Only fetch orders")
    p.add_argument("--positions-only", action="store_true", help="Only fetch positions")
    p.add_argument("--verbose", action="store_true")
    return p

def main(argv=None):
    args = build_cli().parse_args(argv)
    logger.remove()
    logger.add(lambda m: print(m, end=""), level=("DEBUG" if args.verbose else "INFO"))

    s = restore_or_login()

    ob_rows, pos_rows = [], []
    if not args.positions_only:
        try:
            ob_rows = _fetch_orderbook(s)
        except Exception as e:
            logger.warning(f"orderBook failed: {e}")
    if not args.orders_only:
        try:
            pos_rows = _positions_any(s)
        except Exception as e:
            logger.warning(f"positions fetch failed: {e}")

    ob_sum = _summarize_orders(ob_rows) if ob_rows else {"count": 0}
    pos_sum = _summarize_positions(pos_rows) if pos_rows else {"count": 0}

    result = {
        "summary": {"orders": ob_sum, "positions": pos_sum},
        "raw": {
            "order_book": ob_rows,
            "positions": pos_rows,
        },
    }

    if not args.no_redact:
        result["raw"] = _redact_tokens(result["raw"])

    if args.json or args.pretty:
        print(json.dumps(result, indent=(2 if args.pretty else None), ensure_ascii=False))
        return 0

    print("\n=== SUMMARY ===")
    print(json.dumps(result["summary"], indent=2, ensure_ascii=False))
    print("\n=== ORDER BOOK (raw) ===")
    print(json.dumps(result["raw"]["order_book"], indent=2, ensure_ascii=False))
    print("\n=== POSITIONS (raw) ===")
    print(json.dumps(result["raw"]["positions"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
