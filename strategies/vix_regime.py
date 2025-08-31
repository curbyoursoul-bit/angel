from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from loguru import logger
from utils.instruments import load_instruments

IST = timezone(timedelta(hours=5, minutes=30))

def _ltp(smart, exchange: str, tradingsymbol: str, symboltoken: str) -> Optional[float]:
    try:
        res = smart.ltpData(exchange, tradingsymbol, str(symboltoken))
        if isinstance(res, dict) and res.get("status"):
            return float((res.get("data") or {}).get("ltp"))
    except TypeError:
        try:
            res = smart.ltpData({"exchange": exchange, "tradingsymbol": tradingsymbol, "symboltoken": str(symboltoken)})
            if isinstance(res, dict) and res.get("status"):
                return float((res.get("data") or {}).get("ltp"))
        except Exception:
            pass
    except Exception:
        pass
    return None

def run(smart) -> List[Dict[str, Any]]:
    vix_sym = os.getenv("VIX_SYMBOL", "INDIAVIX")
    vix_tok = os.getenv("VIX_TOKEN", "")
    vix_high = float(os.getenv("VIX_HIGH", "18"))
    vix_low  = float(os.getenv("VIX_LOW", "13"))

    v = None
    if vix_tok:
        v = _ltp(smart, "NSE", vix_sym, vix_tok)

    if v is None:
        try:
            df = load_instruments()
            sub = df[(df["exch_seg"].str.upper() == "NSE")]
            hit = (sub["symbol"].astype(str).str.upper() == vix_sym.upper()) if "symbol" in sub.columns \
                  else (sub["name"].astype(str).str.upper() == vix_sym.upper())
            hit = sub[hit]
            if not hit.empty:
                row = hit.iloc[0].to_dict()
                ts = str(row.get("tradingsymbol") or row.get("symbol") or row.get("name") or vix_sym)
                tok = str(row.get("symboltoken") or row.get("token") or "")
                if tok:
                    v = _ltp(smart, "NSE", ts, tok)
        except Exception:
            pass

    if v is None:
        logger.warning("vix_regime: VIX not available (set VIX_TOKEN / VIX_SYMBOL).")
        return []

    regime = "HIGH" if v >= vix_high else ("LOW" if v <= vix_low else "MID")
    logger.info(f"vix_regime: VIX={v:.2f} → regime={regime} (LOW≤{vix_low} < MID < {vix_high}≤HIGH)")
    return []
