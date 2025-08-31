# scripts/bs_demo.py
from __future__ import annotations
import argparse
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

from utils.black_scholes import BlackScholes, bs_with_expiry_date  # keep both paths if you have them


@dataclass
class ResultRow:
    call: float
    put: float
    delta_call: float
    delta_put: float
    gamma: float
    vega_pct: float
    theta_call_day: float
    theta_put_day: float


def _solve_iv_from_price(
    spot: float, strike: float, expiry: date, rate: float, q: float, target: float, option: str
) -> Optional[float]:
    """
    Best-effort implied vol (annualized) via bisection on the Black–Scholes price.
    Requires bs_with_expiry_date(…, iv=…, option="CE"/"PE").
    """
    try:
        lo, hi = 0.01, 2.50
        for _ in range(50):
            mid = (lo + hi) / 2.0
            theo = bs_with_expiry_date(
                spot=spot, strike=strike, expiry=expiry, rate=rate, iv=mid, option=("CE" if option.upper().startswith("C") else "PE"), q=q
            ).price
            if abs(theo - target) < 1e-2:
                return mid
            if theo > target:
                hi = mid
            else:
                lo = mid
        return (lo + hi) / 2.0
    except Exception:
        return None


def _parse_args():
    p = argparse.ArgumentParser(description="Black–Scholes demo: prices + Greeks (+ optional IV solve)")
    p.add_argument("--spot", type=float, default=49250, help="Underlying spot")
    p.add_argument("--strike", type=float, default=49300, help="Strike")
    # either expiry_days OR expiry_date (YYYY-MM-DD)
    p.add_argument("--expiry-days", type=float, default=3, help="Days to expiry (calendar days)")
    p.add_argument("--expiry-date", type=str, help="Expiry date YYYY-MM-DD (overrides --expiry-days)")
    p.add_argument("--iv", type=float, default=0.20, help="Volatility (annualized, e.g. 0.20)")
    p.add_argument("--rate", type=float, default=0.065, help="Risk-free rate (annualized)")
    p.add_argument("--div", type=float, default=0.0, help="Dividend/Carry yield q (annualized)")
    p.add_argument("--solve-iv", choices=["call", "put"], help="Solve implied vol from --target-price for leg")
    p.add_argument("--target-price", type=float, help="Market price used for IV solve (requires --solve-iv)")
    return p.parse_args()


def _compute(args) -> ResultRow:
    if args.expiry_date:
        expiry = datetime.strptime(args.expiry_date, "%Y-%m-%d").date()
        # Preferbs_with_expiry_date if available for accurate year fraction:
        bs = bs_with_expiry_date(spot=args.spot, strike=args.strike, expiry=expiry, rate=args.rate, iv=args.iv, q=args.div)
        # Recompute both legs via date-based API for consistency:
        call = bs_with_expiry_date(args.spot, args.strike, expiry, args.rate, args.iv, "CE", q=args.div)
        put  = bs_with_expiry_date(args.spot, args.strike, expiry, args.rate, args.iv, "PE", q=args.div)
        return ResultRow(
            call=call.price,
            put=put.price,
            delta_call=call.delta,
            delta_put=put.delta,
            gamma=call.gamma,                 # gamma same for calls/puts in BS
            vega_pct=call.vega,               # assuming your .vega is per 1% vol; keep your lib’s convention
            theta_call_day=call.theta / 365,  # convert annual to per-day if needed
            theta_put_day=put.theta / 365,
        )
    else:
        # Fallback to your original class (days-based)
        bs = BlackScholes(spot=args.spot, strike=args.strike, expiry_days=args.expiry_days, volatility=args.iv, rate=args.rate, dividend=args.div)
        res = bs.price_and_greeks()
        return ResultRow(
            call=res.call,
            put=res.put,
            delta_call=res.delta_call,
            delta_put=res.delta_put,
            gamma=res.gamma,
            vega_pct=res.vega_pct,
            theta_call_day=res.theta_call_day,
            theta_put_day=res.theta_put_day,
        )


def main():
    args = _parse_args()

    # Optional IV solve
    solved_iv: Optional[float] = None
    if args.solve_iv and args.target_price is not None and args.expiry_date:
        expiry = datetime.strptime(args.expiry_date, "%Y-%m-%d").date()
        solved_iv = _solve_iv_from_price(
            spot=args.spot,
            strike=args.strike,
            expiry=expiry,
            rate=args.rate,
            q=args.div,
            target=args.target_price,
            option=args.solve_iv,
        )
        if solved_iv is not None:
            print(f"Implied vol ({args.solve_iv}) ≈ {solved_iv:.3%}")
            args.iv = solved_iv  # use solved IV for final table

    row = _compute(args)

    print("\nBlack–Scholes summary")
    print(f"  Spot={args.spot:.2f}  Strike={args.strike:.2f}  IV={args.iv:.2%}  Rate={args.rate:.2%}  q={args.div:.2%}")
    if args.expiry_date:
        print(f"  Expiry date: {args.expiry_date}")
    else:
        print(f"  Expiry days: {args.expiry_days}")

    hdr = "LEG   PRICE     DELTA     GAMMA        VEGA(1%)   THETA/day"
    print(hdr)
    print("-" * len(hdr))
    print(f"CALL  {row.call:8.2f}  {row.delta_call:8.4f}  {row.gamma:10.6f}  {row.vega_pct:10.2f}  {row.theta_call_day:9.2f}")
    print(f"PUT   {row.put:8.2f}  {row.delta_put:8.4f}  {row.gamma:10.6f}  {row.vega_pct:10.2f}  {row.theta_put_day:9.2f}")

    if solved_iv is not None:
        print("\nNote: Prices above are recomputed using the solved IV.")

if __name__ == "__main__":
    main()
