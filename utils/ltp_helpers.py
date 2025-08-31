from __future__ import annotations
from typing import Any, Dict, List

def ensure_tokens(exe, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ensure each order has symboltoken; DRY_RUN preview fills it if missing.
    Mutates orders in-place (as your preview does).
    """
    if any(not od.get("symboltoken") for od in orders):
        exe.run("angel", "place_orders", orders=orders, mode="DRY_RUN")
    return orders

def ltp_from_order(exe, od: Dict[str, Any]) -> Dict[str, Any]:
    """Call Angel LTP using an order dict that already has symboltoken."""
    return exe.run(
        "angel", "ltp",
        exchange=od["exchange"],
        tradingsymbol=od["tradingsymbol"],
        symboltoken=str(od["symboltoken"]),
    )

def index_ltp_from_csv(exe, exch: str, symbol: str, csv_path: str = r"data\OpenAPIScripMaster.csv") -> Dict[str, Any]:
    """
    Optional helper for index LTP. Your CSV may not include NSE index rows.
    Returns {ok,data,error}.
    """
    try:
        import pandas as pd
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        return {"ok": False, "data": None, "error": f"read csv failed: {e}"}

    alias = {"BANKNIFTY": "NIFTY BANK", "NIFTY": "NIFTY 50"}
    sym = alias.get(symbol.upper(), symbol)

    sel = df[
        (df["exch_seg"].astype(str).str.upper() == exch.upper())
        & (df["instrumenttype"].astype(str).str.upper() == "INDEX")
        & (df["symbol"].astype(str).str.upper() == sym.upper())
    ].head(1)

    if sel.empty:
        return {"ok": False, "data": None, "error": f"index '{sym}' not found in {csv_path}"}
from __future__ import annotations

def ensure_tokens(exe, orders):
    """Ensure each order has symboltoken; run DRY_RUN preview to enrich if missing."""
    if any(not od.get("symboltoken") for od in orders):
        exe.run("angel", "place_orders", orders=orders, dry_run=True)
    return orders

def ltp_from_order(exe, od):
    return exe.run(
        "angel", "ltp",
        exchange=od["exchange"],
        tradingsymbol=od["tradingsymbol"],
        symboltoken=str(od["symboltoken"]),
    )
def index_ltp_from_csv(exe, exch, sym, csv_path=r"data\OpenAPIScripMaster.csv"):
    """ Helper for index LTP via Angel tokens. Returns {ok,data,error}."""
    try:
        import pandas as pd
        df = pd.read_csv(csv_path, low_memory=False)            
    except Exception as e:
        return {"ok": False, "data": None, "error": f"read csv failed: {e}"}
    alias = {"BANKNIFTY": "NIFTY BANK", "NIFTY": "NIFTY 50"}
    sym = alias.get(sym.upper(), sym)
    sel = df[
        (df["exch_seg"].astype(str).str.upper() == exch.upper())
        & (df["instrumenttype"].astype(str).str.upper() == "INDEX")
        & (df["symbol"].astype(str).str.upper() == sym.upper())
    ].head(1)
    if sel.empty:
        return {"ok": False, "data": None, "error": f"index '{sym}' not found in {csv_path}"}
    token = str(sel.iloc[0]["token"])
    return exe.run("angel", "ltp", exchange=exch.upper(), tradingsymbol=sym, symboltoken=token)
