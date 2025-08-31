# execution/eod_squareoff.py
from __future__ import annotations
from loguru import logger
def square_off_all(smart) -> None:
    try:
        try: positions = smart.position()        # type: ignore
        except Exception:
            try: positions = smart.getPosition() # type: ignore
            except Exception: positions = None
        rows = positions.get("data") if isinstance(positions,dict) else positions
        if not rows: logger.info("No open positions."); return
        from execution.order_manager import OrderManager
        om = OrderManager(smart)
        for p in rows:
            qty = int(float(p.get("netqty") or p.get("netQty") or 0))
            if qty==0: continue
            side = "SELL" if qty>0 else "BUY"
            order = {
                "exchange": p.get("exchange") or p.get("exch_seg"),
                "tradingsymbol": p.get("tradingsymbol"),
                "symboltoken": p.get("symboltoken") or p.get("token"),
                "transactiontype": side, "ordertype": "MARKET",
                "producttype": p.get("producttype") or p.get("productType") or "INTRADAY",
                "duration": "DAY", "quantity": abs(qty), "variety": "NORMAL",
            }
            om.place(order)
        logger.info("EOD square-off complete.")
    except Exception as e:
        logger.error(f"square_off_all fatal: {e}")
