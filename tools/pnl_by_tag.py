# tools/pnl_by_tag.py
from __future__ import annotations
from typing import Iterable, Dict, Any, List
import csv

_NULLS = {None, "", "None"}

def _pick(row: Dict[str, Any], *keys: str, default: str = "") -> str:
    for k in keys:
        v = row.get(k)
        if v not in _NULLS:
            return v
    return default

def _pick_num(row: Dict[str, Any], *keys: str) -> float:
    for k in keys:
        v = row.get(k)
        if v not in _NULLS:
            try:
                return float(v)
            except Exception:
                pass
    return 0.0

def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Coalesce broker field variants into a stable, analysis-friendly schema."""
    row = row or {}
    out = {
        # core
        "source":          _pick(row, "source"),
        "tradingsymbol":   _pick(row, "tradingsymbol", "symbol", "Symbol", "tradingSymbol"),
        "symboltoken":     _pick(row, "symboltoken", "token", "SymbolToken"),
        "transactiontype": _pick(row, "transactiontype", "side", "buy_sell", "TransactionType"),
        "price":           _pick(row, "price", "Price", "OrderPrice"),
        "quantity":        _pick(row, "quantity", "Quantity", "OrderQty"),
        "when":            _pick(row, "when", "timestamp", "time", "orderTime", "OrderTime", "created_at"),
        "tag_text":        _pick(row, "tag_text", "ordertag", "OrderTag", "tag"),

        # fill/status
        "status":          _pick(row, "status", "orderstatus", "OrderStatus", "Status"),
        "filledqty":       _pick(row, "filledqty", "FilledQty", "traded_quantity", "TradedQuantity", "filledQuantity"),
        "avgprice":        _pick(row, "avgprice", "AvgPrice", "trade_price", "TradePrice", "averageprice"),

        # diagnostics / remarks
        "remarks":         _pick(row, "remarks", "remark", "message", "Message", "reason", "Reason", "StatusMessage", "ErrorMessage"),
        "raw_status":      _pick(row, "status", "Status"),
        "raw_orderstatus": _pick(row, "orderstatus", "OrderStatus"),
    }

    # normalized helpers
    out["status_norm"]   = (out["status"] or "").upper()
    out["side_norm"]     = (out["transactiontype"] or "").upper()
    out["filledqty_num"] = _pick_num(row, "filledqty", "FilledQty", "traded_quantity", "TradedQuantity", "filledQuantity")
    out["avgprice_num"]  = _pick_num(row, "avgprice", "AvgPrice", "trade_price", "TradePrice", "averageprice")
    out["price_num"]     = _pick_num(row, "price", "Price", "OrderPrice")
    out["quantity_num"]  = _pick_num(row, "quantity", "Quantity", "OrderQty")
    return out

def write_csv_rich(rows: Iterable[Dict[str, Any]], path: str) -> str:
    """
    Write rows with a rich, union-of-keys header including normalized fields.
    Safe if some rows miss columns (extrasaction='ignore').
    Returns the written path.
    """
    rows_list: List[Dict[str, Any]] = [r or {} for r in rows]
    norm_rows = [_normalize_row(r) for r in rows_list]

    preferred = [
        "source","tradingsymbol","symboltoken","transactiontype","side_norm",
        "status","status_norm","raw_status","raw_orderstatus",
        "filledqty","filledqty_num","avgprice","avgprice_num",
        "price","price_num","quantity","quantity_num",
        "remarks","when","tag_text"
    ]
    extras = sorted(set().union(*(set(n.keys()) for n in norm_rows)) - set(preferred)) if norm_rows else []
    fieldnames = preferred + extras

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for n in norm_rows:
            w.writerow(n)
    return path
