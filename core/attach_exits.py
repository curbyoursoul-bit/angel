# core/attach_exits.py
from __future__ import annotations

import csv
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from loguru import logger

from utils.market_hours import IST
from utils.ltp_fetcher import get_ltp
from utils.oco_registry import new_group_id, record_primary, record_stop, record_target
from utils.order_exec import (
    _make_sl_buy_for_short,
    _make_tp_buy_for_short,
    _short_tag,
    _place,
    _log_trade_row,
)
from config import AUTO_STOPS_ENABLED, AUTO_TARGETS_ENABLED, DRY_RUN, TRADE_LOG_CSV

# Ensure all referenced functions and variables exist in the imported modules.
# If any are missing, implement stubs or import them accordingly.

_DONE_STATUSES = {
    "complete", "completed", "filled", "fully filled",
    "put order success", "put order complete"
}

# ----------------------------
# small utilities / adapters
# ----------------------------
def _now_ist() -> datetime:
    return datetime.now(IST)

def _as_dt_ist(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return IST.localize(dt) if dt.tzinfo is None else dt.astimezone(IST)
        except Exception:
            continue
    return None

def _row(row: Dict[str, Any], *keys: str, default: Any = "") -> Any:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
        lk = k.lower()
        if lk in row and row[lk] not in (None, ""):
            return row[lk]
    return default

def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _is_recent(dt: Optional[datetime], lookback: timedelta) -> bool:
    if dt is None:
        return False
    return (_now_ist() - dt) <= lookback

def _estimate_entry_price(smart, row: Dict[str, Any], exchange: str, symbol: str, token: str) -> float:
    avg_price = _as_float(_row(row, "averageprice", "avgprice"), 0.0)
    if avg_price > 0:
        return avg_price
    px_hint = _as_float(_row(row, "price", "limit_price", "entry_price"), 0.0)
    if px_hint > 0:
        return px_hint
    if not token:
        return 0.0
    try:
        return float(get_ltp(smart, exchange, symbol, token))
    except Exception:
        return 0.0

# ----------------------------
# backoff wrappers (LIVE)
# ----------------------------
def _fetch_with_backoff(fn, *, tries: int = 5, base: float = 0.6):
    last = None
    for i in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            last = e
            if any(k in msg for k in ("exceed", "rate", "timeout", "pool")):
                time.sleep(base * i)
                continue
            raise
    raise last or RuntimeError("fetch failed")

# ----------------------------
# filter logic
# ----------------------------
def _skip_reason(row: Dict[str, Any], lookback: timedelta, symbol_filter: Optional[str]) -> Tuple[bool, str]:
    status = str(_row(row, "status", default="complete")).strip().lower()
    if status not in _DONE_STATUSES:
        return True, f"status={status!r} not filled"

    side = str(_row(row, "transactiontype", "transactionType", default="SELL")).upper()
    if side != "SELL":
        return True, f"side={side} not SELL"

    symbol = str(_row(row, "tradingsymbol", "tradingSymbol", "symbol"))
    if not symbol:
        return True, "missing symbol"

    if symbol_filter and symbol_filter.upper() not in symbol.upper():
        return True, f"symbol {symbol} not matching filter {symbol_filter}"

    qty = int(_as_float(_row(row, "quantity", "qty", default=0), 0.0))
    if qty <= 0:
        return True, f"qty={qty} <= 0"

    ts_str = str(_row(
        row,
        "ordergenerationtime", "updatetime", "exchtime",
        "order_time", "filledtime", "ts", "ts_ist",
        default=""
    ))
    ts = _as_dt_ist(ts_str)
    if not _is_recent(ts, lookback):
        return True, f"stale (ts={ts_str or 'NA'})"

    return False, "ok"

# ----------------------------
# DRY source: trade log
# ----------------------------
def _dry_recent_shorts_from_log(lookback_minutes: int, symbol_filter: Optional[str]) -> List[Dict[str, Any]]:
    """
    Harvest recent DRY short entries from our trade log.
    - Prefer genuine SELL primaries (note == 'preview_primary').
    - If none, infer a SELL primary from BUY attach rows within lookback by grouping on (ordertag, symbol).
      We treat any mode containing 'DRY' as DRY (e.g., 'DRY RUN'); if mode is blank but the row looks like a DRY preview,
      accept it as well.
    """
    rows: List[Dict[str, Any]] = []
    cutoff = _now_ist() - timedelta(minutes=max(1, lookback_minutes))

    try:
        st = os.stat(TRADE_LOG_CSV)
        size = f"{st.st_size}B"
        mtime = datetime.fromtimestamp(st.st_mtime, IST).strftime("%Y-%m-%d %H:%M:%S %Z")
        logger.info(
            f"attach_exits[DRY]: reading trade log file={TRADE_LOG_CSV} "
            f"size={size} mtime={mtime} cutoff={cutoff.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
    except Exception:
        logger.info(f"attach_exits[DRY]: reading trade log file={TRADE_LOG_CSV} (stat unavailable)")

    scanned = kept_sell = inferred_from_buy = 0
    skips_old = skips_mode = skips_side = skips_qty = skips_symbol = 0

    buy_groups: Dict[Tuple[str, str], Dict[str, Any]] = {}

    try:
        with open(TRADE_LOG_CSV, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                scanned += 1

                # --- permissive DRY detection ---
                mode_raw = str(r.get("mode", "")).strip()
                mode_upper = mode_raw.upper()
                looks_dry_mode = ("DRY" in mode_upper) if mode_upper else None  # None = unknown
                # proceed unless it's explicitly a non-DRY marker like 'LIVE'
                if looks_dry_mode is False and mode_upper not in ("",):
                    # explicitly non-DRY (e.g., LIVE)
                    skips_mode += 1
                    continue

                sym = r.get("tradingsymbol") or r.get("symbol") or ""
                if not sym:
                    skips_symbol += 1
                    continue
                if symbol_filter and symbol_filter.upper() not in sym.upper():
                    skips_symbol += 1
                    continue

                qty = int(_as_float(r.get("qty", 0) or r.get("quantity", 0), 0.0))
                if qty <= 0:
                    skips_qty += 1
                    continue

                ts_raw = (r.get("ts") or r.get("ts_ist") or "").strip()
                ts = _as_dt_ist(ts_raw) if ts_raw else None
                if not ts or ts < cutoff:
                    skips_old += 1
                    continue

                side = str(r.get("side", "")).upper()
                note = (r.get("note") or "").strip().lower()
                ordertag = (r.get("ordertag") or sym[:19]).strip()

                # Treat DRY-looking primaries even if mode is blank
                is_preview_primary = (side == "SELL" and note == "preview_primary")
                if is_preview_primary and (looks_dry_mode or looks_dry_mode is None):
                    rows.append({
                        "status": "complete",
                        "transactiontype": "SELL",
                        "tradingsymbol": sym,
                        "symboltoken": r.get("symboltoken", "") or "",
                        "exchange": (r.get("exchange") or "NFO").upper(),
                        "ordertype": str(r.get("ordertype") or "LIMIT").upper(),
                        "producttype": (r.get("producttype") or "INTRADAY").upper(),
                        "quantity": qty,
                        "averageprice": r.get("price") or r.get("limit_price") or "0",
                        "ordertag": ordertag,
                        "ordergenerationtime": ts_raw,
                        "price": r.get("price") or r.get("limit_price") or "0",
                    })
                    kept_sell += 1
                    continue

                # BUY exits hint → infer missing SELL primary (accept if tag starts with OCO- or attach_* note)
                if side == "BUY" and (ordertag.upper().startswith("OCO-") or note in (
                    "attach_stop_dry", "attach_target_dry", "attach_stop", "attach_target"
                )):
                    key = (ordertag, sym)
                    buy_groups[key] = {
                        "symbol": sym,
                        "qty": qty,
                        "ts_raw": ts_raw,
                        "exchange": (r.get("exchange") or "NFO").upper(),
                        "producttype": (r.get("producttype") or "INTRADAY").upper(),
                        "symboltoken": r.get("symboltoken", "") or "",
                        "price_hint": r.get("entry_price") or r.get("limit_price") or r.get("price") or "0",
                        "ordertag": ordertag,
                    }
                else:
                    # If mode explicitly looks non-DRY and we didn't accept it, count as mode skip,
                    # otherwise classify as side skip (matches previous counters)
                    if looks_dry_mode is False:
                        skips_mode += 1
                    else:
                        skips_side += 1

    except FileNotFoundError:
        logger.debug(f"Trade log not found for DRY scan: {TRADE_LOG_CSV}")

    if kept_sell == 0 and buy_groups:
        for (_tag, sym), ex in buy_groups.items():
            rows.append({
                "status": "complete",
                "transactiontype": "SELL",
                "tradingsymbol": sym,
                "symboltoken": ex["symboltoken"],
                "exchange": ex["exchange"],
                "ordertype": "LIMIT",
                "producttype": ex["producttype"],
                "quantity": ex["qty"],
                "averageprice": ex["price_hint"],
                "ordertag": ex["ordertag"],
                "ordergenerationtime": ex["ts_raw"],
                "price": ex["price_hint"],
            })
            inferred_from_buy += 1

    logger.info(
        f"attach_exits[DRY]: scanned={scanned} kept_sell={kept_sell} inferred_from_buy={inferred_from_buy} "
        f"skips(old={skips_old},mode={skips_mode},side={skips_side},qty={skips_qty},symbol={skips_symbol})"
    )
    return rows

# ----------------------------
# LIVE scanner: order book
# ----------------------------
def scan_recent_short_fills(smart, *, lookback_minutes: int, symbol_filter: Optional[str]) -> List[Dict[str, Any]]:
    lookback = timedelta(minutes=max(1, int(lookback_minutes)))
    if DRY_RUN:
        return _dry_recent_shorts_from_log(lookback_minutes, symbol_filter)

    try:
        ob = _fetch_with_backoff(smart.orderBook)
    except Exception as e:
        logger.error(f"attach_exits scan: failed to fetch order book after retries: {e}")
        return []

    data = (ob or {}).get("data")
    if not isinstance(data, list):
        return []

    out: List[Dict[str, Any]] = []
    for row in data:
        skip, reason = _skip_reason(row, lookback, symbol_filter)
        row["_attach_ok"] = (not skip)
        row["_attach_reason"] = reason
        if not skip:
            out.append(row)
    return out

def verbose_dump(smart, *, lookback_minutes: int, symbol_filter: Optional[str]) -> None:
    rows = scan_recent_short_fills(smart, lookback_minutes=lookback_minutes, symbol_filter=symbol_filter)
    if not rows:
        logger.info("attach_exits: VERBOSE — no eligible rows found.")
        return
    logger.info(f"attach_exits: VERBOSE — {len(rows)} eligible row(s):")
    for r in rows:
        symbol = _row(r, "tradingsymbol", "tradingSymbol", "symbol")
        side = _row(r, "transactiontype", "transactionType")
        qty = _row(r, "quantity", "qty")
        status = _row(r, "status")
        ts = _row(r, "ordergenerationtime", "updatetime", "exchtime", "order_time", "filledtime", "ts", "ts_ist")
        logger.info(f"  symbol={symbol} side={side} qty={qty} status={status} ts={ts}")

# ----------------------------
# main entry
# ----------------------------
def attach_exits_for_recent_shorts(
    smart,
    *,
    lookback_minutes: int = 20,
    symbol_filter: Optional[str] = None,
    verbose: bool = False,
) -> int:
    if not (AUTO_STOPS_ENABLED or AUTO_TARGETS_ENABLED):
        logger.info("AUTO_STOPS_ENABLED/AUTO_TARGETS_ENABLED both disabled — nothing to attach.")
        return 0

    if verbose:
        verbose_dump(smart, lookback_minutes=lookback_minutes, symbol_filter=symbol_filter)

    rows = scan_recent_short_fills(smart, lookback_minutes=lookback_minutes, symbol_filter=symbol_filter)
    if not rows:
        logger.info("attach_exits: nothing to attach (recent shorts not found).")
        return 0

    seen_keys = set()
    attached_count = 0

    for row in rows:
        symbol = str(_row(row, "tradingsymbol", "tradingSymbol", "symbol"))
        exchange = str(_row(row, "exchange", default="NFO")).upper() or "NFO"
        producttype = str(_row(row, "producttype", "productType", default="INTRADAY")).upper()
        token = str(_row(row, "symboltoken", "token"))
        qty = int(_as_float(_row(row, "quantity", "qty", default=0), 0.0))
        if not symbol or qty <= 0:
            continue

        ts_key = str(_row(row, "ordergenerationtime", "ts", "ts_ist", default="")).strip()
        key = (symbol, qty, ts_key)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        if symbol.endswith("-EQ") and exchange == "NFO":
            exchange = "NSE"

        base_px = _estimate_entry_price(smart, row, exchange, symbol, token)

        try:
            oco_gid = new_group_id(symbol)
        except Exception:
            oco_gid = f"OCO-{symbol[:8]}"

        primary = {
            "tradingsymbol": symbol,
            "symboltoken": token,
            "exchange": exchange,
            "producttype": producttype,
            "quantity": qty,
            "ordertag": _short_tag(oco_gid),
        }

        placed_any = False

        if AUTO_STOPS_ENABLED:
            stop_o = _make_sl_buy_for_short(primary, base_px)
            stop_o["ordertag"] = _short_tag(oco_gid)
            if DRY_RUN:
                logger.info(f"[ATTACH-DRY] STOP for {symbol}: {stop_o}")
                _log_trade_row("DRY", stop_o, None, "attach_stop_dry")
                placed_any = True
            else:
                sok, soid, sresp = _place(smart, stop_o)
                _log_trade_row("LIVE", stop_o, soid, "attached_stop" if sok else "stop_failed")
                if sok:
                    placed_any = True
                    try: record_primary(oco_gid, primary)
                    except Exception: pass
                    try: record_stop(oco_gid, soid or "", stop_o)
                    except Exception as e: logger.warning(f"record_stop failed: {e}")
                else:
                    logger.error(f"STOP failed for {symbol}: {sresp}")

        if AUTO_TARGETS_ENABLED:
            tp_o = _make_tp_buy_for_short(primary, base_px)
            tp_o["ordertag"] = _short_tag(oco_gid)
            if DRY_RUN:
                logger.info(f"[ATTACH-DRY] TARGET for {symbol}: {tp_o}")
                _log_trade_row("DRY", tp_o, None, "attach_target_dry")
                placed_any = True
            else:
                tok, toid, tresp = _place(smart, tp_o)
                _log_trade_row("LIVE", tp_o, toid, "attached_target" if tok else "target_failed")
                if tok:
                    placed_any = True
                    try: record_target(oco_gid, toid or "", tp_o)
                    except Exception as e: logger.warning(f"record_target failed: {e}")
                else:
                    logger.error(f"TARGET failed for {symbol}: {tresp}")

        if placed_any:
            attached_count += 1
            logger.success(f"exits attached → {symbol} qty={qty} oco={oco_gid}")

    if attached_count == 0:
        logger.info("attach_exits: nothing attached after filtering.")
    return attached_count
