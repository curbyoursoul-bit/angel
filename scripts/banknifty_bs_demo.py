# scripts/banknifty_bs_demo.py
from __future__ import annotations
import argparse
from dataclasses import dataclass
from typing import Optional, Tuple
from datetime import date
from loguru import logger

from core.login import restore_or_login
from utils.ltp_fetcher import get_banknifty_ltp, get_ltp
from utils.expiry import next_thursday
from utils.instruments import load_instruments, find_option_token
from utils.black_scholes import bs_with_expiry_date

DEFAULT_RATE = 0.065   # 6.5% annualized
DEFAULT_IV   = 0.20    # 20% annualized
DEFAULT_EX   = "NFO"

@dataclass
class Row:
    side: str
    symbol: str
    token: str
    lotsize: int
    theo: float
    mkt: Optional[float]
    edge: Optional[float]
    delta: float
    gamma: float
    vega: float
    theta_day: float

def _fmt(x: Optional[float], nd=2) -> str:
    if x is None:
        return "—"
    return f"{x:.{nd}f}"

def _guess_strike(spot: float, step: int = 100) -> int:
    try:
        s = float(spot)
    except Exception:
        return 0
    # round to nearest step (BANKNIFTY is typically 100)
    return int(round(s / step) * step)

def _solve_iv_from_price(spot: float, strike: float, expiry: date, rate: float, option: str, price: float) -> Optional[float]:
    """Best-effort implied vol via bisection; returns annualized IV or None."""
    try:
        lo, hi = 0.01, 2.50
        for _ in range(40):
            mid = (lo + hi) / 2.0
            theo = bs_with_expiry_date(spot=spot, strike=strike, expiry=expiry, rate=rate, iv=mid, option=option, q=0.0).price
            if abs(theo - price) < 0.01:
                return mid
            if theo > price:
                hi = mid
            else:
                lo = mid
        return (lo + hi) / 2.0
    except Exception:
        return None

def build_cli():
    p = argparse.ArgumentParser(description="BANKNIFTY BS demo: theoretical vs market + Greeks")
    p.add_argument("--underlying", default="BANKNIFTY", help="Underlying index (default BANKNIFTY)")
    p.add_argument("--exchange", default=DEFAULT_EX, help="Exchange segment (default NFO)")
    p.add_argument("--rate", type=float, default=DEFAULT_RATE, help="Risk-free annual rate (e.g., 0.065)")
    p.add_argument("--iv", type=float, default=DEFAULT_IV, help="Initial IV guess (annualized)")
    p.add_argument("--expiry", help="Expiry date YYYY-MM-DD (default: next Thursday)")
    p.add_argument("--strike", type=int, help="Override strike (default: round(spot, 100))")
    p.add_argument("--step", type=int, default=100, help="Strike step for rounding (default 100)")
    p.add_argument("--show-iv", action="store_true", help="Also compute implied vol from market price")
    return p

def _pick_expiry(user_expiry: Optional[str]) -> date:
    if user_expiry:
        from datetime import datetime as _dt
        return _dt.strptime(user_expiry, "%Y-%m-%d").date()
    return next_thursday()

def _find_option(df, ul: str, expiry: date, strike: int, side: str):
    info = find_option_token(df, ul, expiry, strike, side)
    if not info:
        raise RuntimeError(f"Token not found for {ul} {expiry} {strike} {side}")
    return info

def main():
    args = build_cli().parse_args()

    smart = restore_or_login()

    # spot
    spot = get_banknifty_ltp(smart) if args.underlying.upper() == "BANKNIFTY" else None
    if spot is None:
        # try generic LTP for the underlying symbol (NSE index tokens often not tradable; fall back to BANKNIFTY util)
        spot = get_banknifty_ltp(smart)
    if spot is None:
        logger.error("Could not fetch spot LTP; aborting.")
        return

    expiry = _pick_expiry(args.expiry)
    strike = args.strike or _guess_strike(spot, args.step)

    print(f"\nUnderlying : {args.underlying}   spot={spot:.2f}")
    print(f"Expiry     : {expiry}")
    print(f"ATM strike : {strike}\n")

    df = load_instruments()
    rows: list[Row] = []

    for side in ("CE", "PE"):
        try:
            tok = _find_option(df, args.underlying.upper(), expiry, strike, side)
            ts   = tok["tradingsymbol"]
            tkn  = tok["symboltoken"]
            lots = int(tok.get("lotsize") or 25)
        except Exception as e:
            logger.error(f"{side}: instrument lookup failed: {e}")
            continue

        try:
            theo = bs_with_expiry_date(
                spot=spot, strike=strike, expiry=tok.get("expiry") or expiry,
                rate=args.rate, iv=args.iv, option=side, q=0.0
            )
            mkt = get_ltp(smart, args.exchange, ts, tkn)
            edge = (mkt - theo.price) if (mkt is not None) else None

            row = Row(
                side=side,
                symbol=ts,
                token=tkn,
                lotsize=lots,
                theo=float(theo.price),
                mkt=float(mkt) if mkt is not None else None,
                edge=(float(edge) if edge is not None else None),
                delta=float(theo.delta),
                gamma=float(theo.gamma),
                vega=float(theo.vega),
                theta_day=float(theo.theta) / 365.0,
            )
            rows.append(row)

            # Optional IV from market
            if args.show_iv and mkt is not None:
                iv_star = _solve_iv_from_price(spot, strike, tok.get("expiry") or expiry, args.rate, side, float(mkt))
                if iv_star is not None:
                    print(f"[{side}] Implied IV ≈ {iv_star:.3%}")

        except Exception as e:
            logger.error(f"{side}: BS/LTP failed: {e}")
            continue

    if not rows:
        print("No rows to show (instrument/LTP failure).")
        return

    # pretty print
    hdr = "SIDE  SYMBOL                              THEO    MKT    EDGE   Δ       Γ         ν       θ/day  LOT"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(
            f"{r.side:<4}  {r.symbol:<34} "
            f"{_fmt(r.theo):>6}  {_fmt(r.mkt):>6}  {_fmt(r.edge):>+6}  "
            f"{r.delta:>+6.3f}  {r.gamma:>9.6f}  {r.vega:>7.2f}  {r.theta_day:>7.2f}  {r.lotsize:>3}"
        )

    print("\nNotes:")
    print("  - Δ/Γ/ν are for one unit; θ/day shown as per-day decay approximation.")
    print("  - Edge = Market LTP − Theoretical price (positive ⇒ rich vs model).")

if __name__ == "__main__":
    main()
