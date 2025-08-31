# tools/positions.py
from __future__ import annotations
import os, sys, json, argparse, csv
from typing import Any, Dict, List, Optional
from loguru import logger

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.login import restore_or_login
try:
    from tools.base import Tool
except Exception:
    class Tool:  # minimal shim if Tool base isn't available
        name = "positions"
        def run(self, fn: str, **kwargs): raise NotImplementedError

# ---------------------------- helpers ---------------------------------

_METHOD_CANDIDATES = [
    "position", "positions", "getPositions",
    "positionBook", "getPosition",
]

def _first_ok(smart, names=_METHOD_CANDIDATES):
    for nm in names:
        fn = getattr(smart, nm, None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                pass
    return None

def _coerce_int(x) -> int:
    try:
        return int(float(str(x)))
    except Exception:
        return 0

def _coerce_float(x) -> float:
    try:
        return float(str(x))
    except Exception:
        return 0.0

def _is_option_symbol(tsym: str) -> bool:
    u = (tsym or "").upper()
    return ("CE" in u or "PE" in u) and any(k in u for k in ("NIFTY", "BANKNIFTY", "FINNIFTY","MIDCPNIFTY"))

def _normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    exch = (r.get("exchange") or r.get("exch_seg") or r.get("Exchange") or "").upper()
    tsym = r.get("tradingsymbol") or r.get("TradingSymbol") or r.get("symbolname") or r.get("symbol")
    token = r.get("symboltoken") or r.get("symbolToken") or r.get("token")

    # Prefer provided exchange; if blank and symbol looks like option, assume NFO
    if not exch and _is_option_symbol(tsym or ""):
        exch = "NFO"

    out = {
        "tradingsymbol": tsym,
        "symboltoken":   str(token or ""),
        "exchange":      exch,
        "producttype":   (r.get("producttype") or r.get("productType") or r.get("ProductType") or "").upper(),
        "buyqty":        _coerce_int(r.get("buyqty")  or r.get("buyQty")),
        "sellqty":       _coerce_int(r.get("sellqty") or r.get("sellQty")),
        "netqty":        _coerce_int(r.get("netqty")  or r.get("netQty") or r.get("net_quantity")),
        "avgprice":      _coerce_float(r.get("avgprice") or r.get("avgPrice") or r.get("averageprice") or r.get("AveragePrice")),
        "pnl":           _coerce_float(r.get("pnl") or r.get("mtom") or r.get("unrealized") or r.get("realized")),
    }
    return out

def _extract_rows(resp: Any) -> List[Dict[str, Any]]:
    """Tolerate many SmartAPI shapes; return list of raw rows (dicts)."""
    rows: List[Dict[str, Any]] = []
    if isinstance(resp, list):
        rows = resp
    elif isinstance(resp, dict):
        data = resp.get("data") or resp.get("Data") or {}
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            # common nests
            for k in ("netPositions", "net", "day", "positions", "NetPosition", "DayPosition"):
                v = data.get(k)
                if isinstance(v, list):
                    rows += v
    return [r for r in rows if isinstance(r, dict)]

def fetch_positions() -> List[Dict[str, Any]]:
    s = restore_or_login()
    resp = _first_ok(s) or {}
    raw_rows = _extract_rows(resp)
    return [_normalize_row(r) for r in raw_rows]

def _summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Per-symbol net summary: netqty, avgprice (as-is), pnl sum."""
    agg: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = f"{r.get('exchange','')}/{r.get('tradingsymbol','')}"
        a = agg.setdefault(key, {"exchange": r.get("exchange",""), "tradingsymbol": r.get("tradingsymbol",""),
                                 "netqty": 0, "pnl": 0.0, "avgprice": r.get("avgprice", 0.0)})
        a["netqty"] += int(r.get("netqty") or 0)
        a["pnl"] += float(r.get("pnl") or 0.0)
        # keep last seen avgprice (broker meaning varies)
        a["avgprice"] = r.get("avgprice", a["avgprice"])
    # flatten
    out = [{"exchange": v["exchange"], "tradingsymbol": v["tradingsymbol"], "netqty": v["netqty"],
            "avgprice": v["avgprice"], "pnl": round(v["pnl"], 2)} for v in agg.values()]
    # sort: options first by symbol, then equities
    out.sort(key=lambda x: (0 if _is_option_symbol(x["tradingsymbol"] or "") else 1, x["tradingsymbol"] or ""))
    return out

def _write_csv(rows: List[Dict[str, Any]], path: str) -> str:
    cols = ["exchange","tradingsymbol","symboltoken","producttype","buyqty","sellqty","netqty","avgprice","pnl"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path

# ---------------------------- Tool wrapper ----------------------------------

class PositionsTool(Tool):
    name = "positions"

    # Tool.run in tools.base will auto-route _fetch / _summary / _csv

    def _fetch(self, *, exchange: Optional[str] = None, include_zero: bool = False) -> Dict[str, Any]:
        rows = fetch_positions()
        if exchange:
            rows = [p for p in rows if (p.get("exchange") or "").upper() == exchange.upper()]
        if not include_zero:
            rows = [p for p in rows if int(p.get("netqty") or 0) != 0]
        return {"ok": True, "data": rows}

    def _summary(self, *, exchange: Optional[str] = None, include_zero: bool = False) -> Dict[str, Any]:
        out = self._fetch(exchange=exchange, include_zero=include_zero)
        if not out.get("ok"):
            return out
        rows: List[Dict[str, Any]] = out["data"]  # type: ignore
        return {"ok": True, "data": _summary(rows)}

    def _csv(self, *, path: str, exchange: Optional[str] = None, include_zero: bool = False) -> Dict[str, Any]:
        out = self._fetch(exchange=exchange, include_zero=include_zero)
        if not out.get("ok"):
            return out
        wrote = _write_csv(out["data"], path)  # type: ignore
        return {"ok": True, "data": {"path": wrote}}

# ---------------------------- CLI entry -------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Dump positions (optionally filtered).")
    ap.add_argument("--exchange", help="Filter by exchange, e.g. NFO or NSE")
    ap.add_argument("--include-zero", action="store_true", help="Include zero-qty rows")
    ap.add_argument("--summary", action="store_true", help="Print per-symbol summary")
    ap.add_argument("--csv", help="Write CSV to path")
    ap.add_argument("--json", action="store_true", help="Print JSON (default)")
    args = ap.parse_args()

    rows = fetch_positions()
    if args.exchange:
        rows = [p for p in rows if (p.get("exchange") or "").upper() == args.exchange.upper()]
    if not args.include_zero:
        rows = [p for p in rows if p.get("netqty", 0) != 0]

    output = _summary(rows) if args.summary else rows

    if args.csv:
        path = _write_csv(rows if not args.summary else output, args.csv)  # type: ignore[arg-type]
        logger.info(f"Wrote {path}")

    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    main()
