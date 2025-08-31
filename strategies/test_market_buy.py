# strategies/test_market_buy.py
from __future__ import annotations
from typing import List, Dict
from utils.resolve import resolve_nse_token

# Change these two if you prefer a different symbol / qty
SYMBOL = "RELIANCE"   # will resolve to RELIANCE-EQ automatically
QTY    = 1

def run(smart) -> List[Dict]:
    ts, token = resolve_nse_token(SYMBOL)
    order = {
        "variety": "NORMAL",
        "tradingsymbol": ts,
        "symboltoken": token,
        "transactiontype": "BUY",
        "exchange": "NSE",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "price": 0,
        "squareoff": 0,
        "stoploss": 0,
        "quantity": QTY,
        "amo": "YES",   # runner also adds this when you pass --amo; double-safe
    }
    return [order]
