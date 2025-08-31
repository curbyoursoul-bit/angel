#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse, csv, json, sys
from collections import defaultdict, deque, Counter
from typing import Dict, Any, Iterable, List, Tuple, Optional
from datetime import datetime

_NULLS = {None, "", "None", "null", "NULL"}

# ---------- helpers: resilient pick/parse ----------

def pick(row: Dict[str, Any], *keys: str, default: str = "") -> str:
    for k in keys:
        v = row.get(k)
        if v not in _NULLS:
            return v
    return default

def pick_num(row: Dict[str, Any], *keys: str) -> float:
    for k in keys:
        v = row.get(k)
        if v not in _NULLS:
            try:
                return float(v)
            except Exception:
                pass
    return 0.0

def _to_side(s: str) -> str:
    s = (s or "").strip().upper()
    if s in {"B", "BUY", "BUYER", "1"}:
        return "BUY"
    if s in {"S", "SELL", "SELLER", "-1"}:
        return "SELL"
    return ""

def _parse_when(row: Dict[str, Any]) -> Tuple[float, str]:
    """
    Return (epoch_seconds, original_string). Tries multiple keys & formats.
    If cannot parse, returns (0.0, raw_or_blank) so stable sort keeps file order.
    """
    raw = pick(row, "when","timestamp","time","orderTime","OrderTime","created_at")
    s = (raw or "").strip()
    if not s:
        return 0.0, ""
    # try epoch
    try:
        # int/float epoch seconds or ms
        f = float(s)
        if f > 1e12:  # ms
            return f/1000.0, s
        return f, s
    except Exception:
        pass
    # try common string formats (add as needed)
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d-%b-%Y %H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.timestamp(), s
        except Exception:
            continue
    # last resort: date only
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.timestamp(), s
        except Exception:
            continue
    return 0.0, s

# ---------- normalization & filtering ----------

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "source":          pick(row, "source"),
        "tradingsymbol":   pick(row, "tradingsymbol", "symbol", "Symbol", "tradingSymbol"),
        "symboltoken":     pick(row, "symboltoken", "token", "SymbolToken"),
        "transactiontype": pick(row, "transactiontype", "side", "buy_sell", "TransactionType"),
        "status":          pick(row, "status", "orderstatus", "OrderStatus", "Status"),
        "price":           pick(row, "price", "Price", "OrderPrice"),
        "quantity":        pick(row, "quantity", "Quantity", "OrderQty"),
        "avgprice":        pick(row, "avgprice", "AvgPrice", "trade_price", "TradePrice", "averageprice"),
        "filledqty":       pick(row, "filledqty", "FilledQty", "traded_quantity", "TradedQuantity", "filledQuantity"),
        "remarks":         pick(row, "remarks", "remark", "message", "Message", "reason", "Reason", "StatusMessage", "ErrorMessage"),
        "when":            pick(row, "when", "timestamp", "time", "orderTime", "OrderTime", "created_at"),
        "tag_text":        pick(row, "tag_text", "ordertag", "OrderTag", "tag"),
    }
    out["status_norm"]   = (out["status"] or "").upper()
    out["side_norm"]     = _to_side(out["transactiontype"])
    out["price_num"]     = pick_num(row, "price", "Price", "OrderPrice")
    out["quantity_num"]  = pick_num(row, "quantity", "Quantity", "OrderQty")
    out["avgprice_num"]  = pick_num(row, "avgprice", "AvgPrice", "trade_price", "TradePrice", "averageprice")
    out["filledqty_num"] = pick_num(row, "filledqty", "FilledQty", "traded_quantity", "TradedQuantity", "filledQuantity")
    when_epoch, when_raw = _parse_when(row)
    out["when_epoch"]    = when_epoch
    out["when_raw"]      = when_raw
    return out

def load_symbols_from_tag(tag: str, tag_dir: str = "data/tags") -> List[str]:
    import json, os
    path = os.path.join(tag_dir, f"{tag}.json")
    try:
        with open(path, encoding="utf-8") as f:
            items = json.load(f)
    except Exception:
        return []
    syms: List[str] = []
    for it in items:
        if isinstance(it, str):
            syms.append(it)
        elif isinstance(it, dict):
            ts = it.get("tradingsymbol")
            if ts:
                syms.append(ts)
    return syms

def choose_rows(rows: Iterable[Dict[str, Any]], source: str, symbols: Optional[Iterable[str]]) -> List[Dict[str, Any]]:
    out = []
    syms = {s for s in (symbols or [])} if symbols else None
    for r in rows:
        if source and r.get("source","") != source:
            continue
        if syms is not None and r.get("tradingsymbol") not in syms:
            continue
        out.append(r)
    return out

# ---------- fill detection ----------

def infer_is_fill(nr: Dict[str, Any]) -> Tuple[bool, float, float]:
    """
    Decide if this row represents a filled execution and return (is_fill, qty, px).
    Priority:
      1) status_norm == COMPLETE and (filledqty_num>0 or quantity_num>0)
      2) filledqty_num > 0 (even if status missing)
      3) fallback: quantity_num>0 and price/avgprice > 0 (treat as filled)
    """
    qty = max(nr["filledqty_num"], nr["quantity_num"])
    px  = nr["avgprice_num"] or nr["price_num"]

    if nr["status_norm"] == "COMPLETE" and qty > 0 and px > 0:
        return True, qty, px
    if nr["filledqty_num"] > 0 and px > 0:
        return True, nr["filledqty_num"], px
    if qty > 0 and px > 0:
        return True, qty, px
    return False, 0.0, 0.0

# ---------- PnL engines ----------

def compute_pnl_simple(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, float]], int]:
    """
    Notional diff model: realized = sell_notional - buy_notional (ignores inventory).
    """
    acc: Dict[str, Dict[str, float]] = defaultdict(lambda: dict(
        buy_qty=0.0, sell_qty=0.0, buy_notional=0.0, sell_notional=0.0
    ))
    trade_count = 0
    for r in rows:
        nr = normalize_row(r)
        is_fill, qty, px = infer_is_fill(nr)
        if not is_fill:
            continue
        side = nr["side_norm"]
        sym  = nr["tradingsymbol"] or "UNKNOWN"
        if side == "BUY":
            acc[sym]["buy_qty"]      += qty
            acc[sym]["buy_notional"] += px * qty
            trade_count += 1
        elif side == "SELL":
            acc[sym]["sell_qty"]      += qty
            acc[sym]["sell_notional"] += px * qty
            trade_count += 1
    return acc, trade_count

def compute_pnl_fifo(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, float]], int]:
    """
    FIFO realized PnL: match sells to prior buys in time order; leaves open inventory.
    """
    # collect fills per symbol in chronological order
    fills_per_sym: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        nr = normalize_row(r)
        is_fill, qty, px = infer_is_fill(nr)
        if not is_fill or nr["side_norm"] not in {"BUY","SELL"}:
            continue
        fills_per_sym[nr["tradingsymbol"] or "UNKNOWN"].append({
            "side": nr["side_norm"], "qty": qty, "px": px, "t": nr["when_epoch"]
        })

    out: Dict[str, Dict[str, float]] = {}
    trade_count = 0

    for sym, fills in fills_per_sym.items():
        fills.sort(key=lambda x: x["t"])  # stable even if t=0.0
        buy_q = deque()   # queue of (qty_remaining, price)
        realized = 0.0
        buy_qty = sell_qty = buy_notional = sell_notional = 0.0

        for f in fills:
            q = float(f["qty"])
            p = float(f["px"])
            if f["side"] == "BUY":
                buy_q.append([q, p])
                buy_qty      += q
                buy_notional += q * p
                trade_count  += 1
            else:  # SELL
                sell_qty      += q
                sell_notional += q * p
                trade_count   += 1
                remain = q
                while remain > 1e-9 and buy_q:
                    bq, bp = buy_q[0]
                    used = min(bq, remain)
                    realized += used * (p - bp)
                    bq -= used
                    remain -= used
                    if bq <= 1e-9:
                        buy_q.popleft()
                    else:
                        buy_q[0][0] = bq
                # If remain > 0 and no buys, we sold short; treat remaining as realized vs zero-cost basis
                if remain > 1e-9:
                    # Choose to treat as realized against zero basis (or skip); here we count it:
                    realized += remain * (p - 0.0)

        open_qty = 0.0
        open_notional = 0.0
        for bq, bp in buy_q:
            open_qty += bq
            open_notional += bq * bp

        out[sym] = {
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "realized": realized,
            "open_qty": open_qty,
            "open_buy_notional": open_notional,
            "open_avg_cost": (open_notional / open_qty) if open_qty > 1e-9 else 0.0,
        }

    return out, trade_count

def to_summary_simple(symbols_acc: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, Any], float]:
    out: Dict[str, Any] = {}
    total = 0.0
    for sym, v in symbols_acc.items():
        realized = v["sell_notional"] - v["buy_notional"]
        out[sym] = {
            "buy_qty":       v["buy_qty"],
            "sell_qty":      v["sell_qty"],
            "buy_notional":  v["buy_notional"],
            "sell_notional": v["sell_notional"],
            "realized":      realized,
            "open_qty":      v["buy_qty"] - v["sell_qty"],
            "open_buy_notional": max(v["buy_notional"] - min(v["sell_notional"], v["buy_notional"]), 0.0),
            "open_avg_cost": 0.0,  # unknown in simple model
        }
        total += realized
    return out, total

# ---------- CLI / I/O ----------

def _read_csv_rows(path: str) -> List[Dict[str, Any]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _write_csv_summary(path: str, sym_map: Dict[str, Any]) -> None:
    cols = ["symbol","buy_qty","sell_qty","buy_notional","sell_notional","realized","open_qty","open_buy_notional","open_avg_cost"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for sym, v in sym_map.items():
            w.writerow([
                sym,
                f"{v.get('buy_qty',0.0):.4f}",
                f"{v.get('sell_qty',0.0):.4f}",
                f"{v.get('buy_notional',0.0):.2f}",
                f"{v.get('sell_notional',0.0):.2f}",
                f"{v.get('realized',0.0):.2f}",
                f"{v.get('open_qty',0.0):.4f}",
                f"{v.get('open_buy_notional',0.0):.2f}",
                f"{v.get('open_avg_cost',0.0):.4f}",
            ])

def main():
    ap = argparse.ArgumentParser(description="Broker-agnostic PnL summarizer with optional FIFO matching")
    ap.add_argument("--csv", required=True, help="Path to exported CSV (from pnl_by_tag --csv-all or similar)")
    ap.add_argument("--source", choices=["orderbook","tradebook",""], default="", help="Filter by source column")
    ap.add_argument("--symbols", help="Comma-separated symbols to include")
    ap.add_argument("--tag", help="Tag name (loads symbols from data/tags/<tag>.json)")
    ap.add_argument("--fifo", action="store_true", help="Use FIFO matching (vs notional-diff)")
    ap.add_argument("--json", action="store_true", help="Emit JSON (compact)")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    ap.add_argument("--out-csv", help="Optional path to write per-symbol summary CSV")
    args = ap.parse_args()

    raw_rows = _read_csv_rows(args.csv)

    # symbols selection
    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    elif args.tag:
        try:
            symbols = load_symbols_from_tag(args.tag)
        except Exception:
            symbols = None  # non fatal

    # normalize and filter early for diag & engines
    norm_rows = [normalize_row(r) for r in raw_rows]
    chosen_norm = []
    syms_set = set(symbols) if symbols else None
    for nr in norm_rows:
        if args.source and (nr.get("source","") != args.source):
            continue
        if syms_set is not None and (nr.get("tradingsymbol") not in syms_set):
            continue
        chosen_norm.append(nr)

    # quick diag
    sym_counts = Counter(nr.get("tradingsymbol","") for nr in chosen_norm)
    first_cols = list(raw_rows[0].keys()) if raw_rows else []

    # compute
    if args.fifo:
        # need original rows for inference but FIFO sorts by timestamp already in normalize_row
        # chosen_norm are normalized; rebuild minimal dicts to feed compute_pnl_fifo path
        # reuse raw rows (compute_pnl_fifo normalizes internally), but re-filter to keep parity
        filtered_raw = []
        for r, nr in zip(raw_rows, norm_rows):
            if nr not in chosen_norm:
                # zip isn't a 1-1 filter test; fallback: re-run quick checks
                if args.source and (pick(r, "source") != args.source):
                    continue
                if syms_set is not None and (pick(r, "tradingsymbol","symbol","tradingSymbol") not in syms_set):
                    continue
            filtered_raw.append(r)
        acc_map, trade_count = compute_pnl_fifo(filtered_raw)
        symbols_out, realized_total = acc_map, sum(v.get("realized",0.0) for v in acc_map.values())
    else:
        # simple model on the filtered normalized rows
        # convert back to raw-ish dicts shaped like input for reuse of your path
        raw_like = []
        for nr in chosen_norm:
            raw_like.append(dict(
                source=nr["source"],
                tradingsymbol=nr["tradingsymbol"],
                symboltoken=nr["symboltoken"],
                transactiontype=nr["side_norm"],
                status=nr["status_norm"],
                price=nr["price_num"],
                quantity=nr["quantity_num"],
                avgprice=nr["avgprice_num"],
                filledqty=nr["filledqty_num"],
                when=nr["when_raw"],
            ))
        acc, trade_count = compute_pnl_simple(raw_like)
        symbols_out, realized_total = to_summary_simple(acc)

    result = {
        "mode": "FIFO" if args.fifo else "NOTIONAL_DIFF",
        "source": args.source or "mixed",
        "symbols": symbols_out,
        "realized_pnl": realized_total,
        "trades_count": trade_count,
        "diag": {
            "row_count": len(chosen_norm),
            "symbols_seen": dict(sym_counts.most_common()),
            "columns": first_cols,
        },
    }

    # helpful notes
    notes = []
    has_status = any(k in first_cols for k in ("status","orderstatus","OrderStatus")) if first_cols else False
    has_fill   = any(k in first_cols for k in ("filledqty","FilledQty","TradedQuantity","traded_quantity")) if first_cols else False
    has_avg    = any(k in first_cols for k in ("avgprice","AvgPrice","TradePrice","trade_price","averageprice")) if first_cols else False
    if not (has_status or has_fill or has_avg):
        notes.append("No status/filledqty/avgprice columns present; fills inferred only if price & quantity are > 0.")
    if not chosen_norm:
        notes.append("No matching rows after filters (check --source / --symbols / --tag).")
    if args.fifo:
        notes.append("FIFO realized ignores still-open quantity; see per-symbol 'open_*' fields for inventory.")
    if notes:
        result["notes"] = notes

    if args.out_csv:
        _write_csv_summary(args.out_csv, symbols_out)

    if args.json or args.pretty:
        print(json.dumps(result, indent=(2 if args.pretty else None), ensure_ascii=False))
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
