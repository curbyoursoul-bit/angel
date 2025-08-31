# automation/scheduler.py
from __future__ import annotations
import threading, time
from datetime import datetime, time as dtime
from typing import Callable, Optional
from loguru import logger
import pytz

from automation.holiday_checker import is_trading_day

IST = pytz.timezone("Asia/Kolkata")

class Repeater(threading.Thread):
    def __init__(self, fn: Callable[[], None], interval_sec: int, name: str, daemon: bool=True):
        super().__init__(daemon=daemon, name=name)
        self.fn = fn; self.interval_sec = max(1, int(interval_sec))
        self._stop = threading.Event()
    def run(self):
        while not self._stop.is_set():
            try: self.fn()
            except Exception as e: logger.warning(f"[sched] {self.name}: {e}")
            self._stop.wait(self.interval_sec)
    def stop(self): self._stop.set()

def _now_ist() -> datetime:
    return datetime.now(IST)

def within_market_hours(now: Optional[datetime]=None) -> bool:
    now = now or _now_ist()
    if not is_trading_day(now.date()):
        return False
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= now <= end

def run_market_loop(on_open: Callable[[], None],
                    on_tick: Callable[[], None],
                    on_close: Callable[[], None],
                    tick_interval_sec: int = 10):
    """
    Calls on_open once after market opens, on_tick periodically during the session,
    and on_close once after session ends. Auto-skips holidays/weekends.
    """
    opened = False; closed = False
    logger.info("[sched] market loop starting")
    while True:
        now = _now_ist()
        if within_market_hours(now):
            if not opened:
                logger.info("[sched] market OPEN — calling on_open()")
                try: on_open()
                finally: opened = True; closed = False
            try: on_tick()
            except Exception as e: logger.warning(f"[sched] on_tick: {e}")
            time.sleep(max(1, tick_interval_sec))
        else:
            if opened and not closed:
                logger.info("[sched] market CLOSE — calling on_close()")
                try: on_close()
                finally: closed = True; opened = False
            # sleep until next minute
            time.sleep(30)
