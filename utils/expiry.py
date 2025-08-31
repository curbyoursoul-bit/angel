# utils/expiry.py
from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Optional, Iterable, Set, Dict

# Weekday numbers: Mon=0 … Sun=6
WEEKDAY_BY_UNDERLYING: Dict[str, int] = {
    "NIFTY": 3,       # Thursday
    "BANKNIFTY": 2,   # Wednesday
    "FINNIFTY": 1,    # Tuesday
}
THURSDAY = 3

def _to_date(d: Optional[date | datetime] = None) -> date:
    if d is None: return datetime.now().date()
    return d.date() if isinstance(d, datetime) else d

def _is_business_day(d: date, holidays: Set[date]) -> bool:
    return d.weekday() < 5 and d not in holidays

def _shift_to_prev_business_day(d: date, holidays: Set[date]) -> date:
    while not _is_business_day(d, holidays):
        d -= timedelta(days=1)
    return d

def get_next_weekly_expiry(
    ref: Optional[date | datetime] = None,
    *,
    include_today_if_expiry: bool = True,
    weekday: int = THURSDAY,
    holidays: Optional[Iterable[date]] = None,
    holiday_shift: bool = False,
) -> date:
    """
    Next weekly expiry for a given weekday (default Thu).
    If holiday_shift=True, shift to previous business day using provided holidays.
    """
    today = _to_date(ref)
    holidays_set: Set[date] = set(holidays or ())

    if today.weekday() == weekday:
        target = today if include_today_if_expiry else today + timedelta(days=7)
    else:
        days_ahead = (weekday - today.weekday()) % 7 or 7
        target = today + timedelta(days=days_ahead)

    return _shift_to_prev_business_day(target, holidays_set) if holiday_shift else target

def next_thursday(ref: Optional[date | datetime] = None, include_today_if_thu: bool = True) -> date:
    # Backwards compatible alias
    return get_next_weekly_expiry(ref, include_today_if_expiry=include_today_if_thu, weekday=THURSDAY)

def weekly_expiry_for(
    underlying: str,
    ref: Optional[date | datetime] = None,
    *,
    include_today: bool = True,
    holidays: Optional[Iterable[date]] = None,
    holiday_shift: bool = False,
) -> date:
    """
    Convenience wrapper picking the correct weekday from the underlying.
    Unknown underlyings default to Thursday.
    """
    wd = WEEKDAY_BY_UNDERLYING.get(underlying.upper(), THURSDAY)
    return get_next_weekly_expiry(ref, include_today_if_expiry=include_today, weekday=wd,
                                  holidays=holidays, holiday_shift=holiday_shift)

def last_thursday_of_month(ref: Optional[date | datetime] = None,
                           *, holidays: Optional[Iterable[date]] = None,
                           holiday_shift: bool = True) -> date:
    """
    Monthly expiry (last Thursday), optionally shifted back for holidays/weekends.
    """
    d = _to_date(ref).replace(day=1)
    # move to first day of next month
    if d.month == 12:
        nxt = d.replace(year=d.year+1, month=1)
    else:
        nxt = d.replace(month=d.month+1)
    # step back to last day of current month, then to last Thursday
    last_day = nxt - timedelta(days=1)
    back = (last_day.weekday() - THURSDAY) % 7
    target = last_day - timedelta(days=back)
    if holiday_shift:
        target = _shift_to_prev_business_day(target, set(holidays or ()))
    return target
