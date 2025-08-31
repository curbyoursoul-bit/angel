# ops/schedule.py
from __future__ import annotations
import os, sys, time, datetime, threading, atexit, signal, json, random
from pathlib import Path
from typing import Tuple, Set
from loguru import logger

# --- timezone (pytz optional) -----------------------------------------------
try:
    import pytz  # type: ignore
    IST = pytz.timezone("Asia/Kolkata")
except Exception:
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

from agent.agent import Agent
from agent.types import Goal
from ops.holidays import (
    load_holidays_combined,
    is_trading_day_ist,
    fetch_holidays_live,
    save_holidays_to_csv,
)

# ---------------------------------------------------------------------------
# Config / paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOCK_FILE = LOG_DIR / "scheduler.lock"
HEARTBEAT_FILE = LOG_DIR / "scheduler.heartbeat.json"

# Times (HH:MM IST) – env overridable
def _env_hhmm(key: str, default_h: int, default_m: int) -> Tuple[int, int]:
    val = os.getenv(key, "")
    if val:
        try:
            h, m = [int(x) for x in val.split(":", 1)]
            return h, m
        except Exception:
            pass
    return default_h, default_m

OPEN_TIME   = _env_hhmm("SCHED_OPEN_TIME",   9, 20)   # 09:20
SQUARE_TIME = _env_hhmm("SCHED_SQUARE_TIME", 15, 20)  # 15:20
EOD_TIME    = _env_hhmm("SCHED_EOD_TIME",    15, 31)  # 15:31

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logger(verbose: bool = False) -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=("DEBUG" if verbose else "INFO"),
        enqueue=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )

# ---------------------------------------------------------------------------
# Cross-platform PID helpers
# ---------------------------------------------------------------------------
def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        # Prefer psutil if available
        import psutil  # type: ignore
        return psutil.pid_exists(pid) and psutil.Process(pid).is_running()
    except Exception:
        pass
    if os.name == "nt":
        # Windows fallback
        import subprocess
        try:
            out = subprocess.check_output(
                ["tasklist", "/FI", f"PID eq {pid}"],
                stderr=subprocess.DEVNULL,
                creationflags=0x08000000,  # CREATE_NO_WINDOW
            ).decode("utf-8", errors="ignore")
            return str(pid) in out
        except Exception:
            return False
    else:
        # POSIX: kill(pid,0) -> OSError if not alive
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            return False

def _kill_pid(pid: int) -> None:
    if pid <= 0:
        return
    try:
        if os.name == "nt":
            import subprocess
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, creationflags=0x08000000)
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Single-instance lock
# ---------------------------------------------------------------------------
def _acquire_lock() -> None:
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_FILE.exists():
        try:
            pid = int((LOCK_FILE.read_text() or "0").strip() or "0")
        except Exception:
            pid = 0
        if pid and _pid_alive(pid):
            logger.error(f"Another scheduler instance appears to be running (lock at {LOCK_FILE} pid={pid}). Exiting.")
            sys.exit(1)
        else:
            logger.warning(f"Stale lock detected (pid={pid}); removing {LOCK_FILE}")
            try:
                LOCK_FILE.unlink()
            except Exception:
                pass
    # atomic create
    fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

def _release_lock() -> None:
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------
def _now_ist() -> datetime.datetime:
    return datetime.datetime.now(tz=IST)

def _today_ist() -> datetime.date:
    return _now_ist().date()

def _hhmm(dt: datetime.datetime) -> Tuple[int, int]:
    return (dt.hour, dt.minute)

def _is_due(now: datetime.datetime, target: Tuple[int, int], window_sec: int = 90) -> bool:
    tgt = now.replace(hour=target[0], minute=target[1], second=0, microsecond=0)
    delta = (now - tgt).total_seconds()
    return 0 <= delta <= window_sec

# Market-hours guard (with env bypass)
def market_open_now(holidays: Set[datetime.date]) -> bool:
    if str(os.getenv("BYPASS_MARKET_HOURS", "")).strip().lower() in {"1", "true", "yes", "y"}:
        return True
    now = _now_ist()
    if now.weekday() >= 5 or now.date() in holidays:
        return False
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= now <= end

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def _run_goals(goals, mode="DRY_RUN", caps=None):
    try:
        agent = Agent(mode=mode, caps=(caps or {"MAX_QTY": 1}))
        agent.loop(goals)
    except Exception as e:
        logger.exception(f"Goal runner crashed: {e}")

def _spawn(goals, mode: str, caps: dict):
    # small random jitter to avoid thundering herd if multiple schedulers exist
    time.sleep(random.uniform(0.1, 0.6))
    t = threading.Thread(target=_run_goals, args=(goals,), kwargs={"mode": mode, "caps": caps}, daemon=True)
    t.start()

# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------
def _write_heartbeat(state: dict) -> None:
    try:
        payload = {
            "ts": int(time.time()),
            "time_ist": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            **state,
        }
        HEARTBEAT_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main_loop(*, verbose: bool = False, mode: str = "DRY_RUN"):
    _setup_logger(verbose)
    logger.info("Starting daily scheduler (IST-aware)")
    holidays = load_holidays_combined()
    logger.info(f"Loaded {len(holidays)} NSE holiday(s) (live/cache + CSV)")

    last_run = {"open": None, "square": None, "eod": None}
    last_midnight = _today_ist()

    while True:
        now = _now_ist()
        today = now.date()

        # Midnight rollover: refresh holidays once per day
        if today != last_midnight:
            try:
                holidays = load_holidays_combined()
                last_midnight = today
                logger.info(f"Refreshed holidays ({len(holidays)}) at midnight")
            except Exception as e:
                logger.warning(f"Holiday refresh failed: {e}")

        # Skip non-trading days
        if not is_trading_day_ist(today, holidays):
            _write_heartbeat({"status": "idle_non_trading"})
            time.sleep(60)
            continue

        # OPEN task
        if last_run["open"] != today and (_hhmm(now) == OPEN_TIME or _is_due(now, OPEN_TIME)):
            last_run["open"] = today
            logger.info(f">>> OPEN task: run_atm_straddle ({mode})")
            _spawn(
                [Goal(text="run_atm_straddle", params={"underlying": "BANKNIFTY", "lots": 1, "strike_step": 100})],
                mode=mode,
                caps={"MAX_QTY": 1},
            )

        # SQUARE task
        if last_run["square"] != today and (_hhmm(now) == SQUARE_TIME or _is_due(now, SQUARE_TIME)):
            last_run["square"] = today
            logger.info(f">>> SQUARE task: square_off_all ({mode}, INTRADAY)")
            _spawn(
                [Goal(text="square_off_all", params={"mode": mode, "include_products": ["INTRADAY"]})],
                mode=mode,
                caps={"MAX_QTY": 10},
            )

        # EOD report
        if last_run["eod"] != today and (_hhmm(now) == EOD_TIME or _is_due(now, EOD_TIME)):
            last_run["eod"] = today
            logger.info(f">>> EOD task: eod_report ({mode})")
            _spawn(
                [Goal(text="eod_report", params={"day": today.isoformat()})],
                mode=mode,
                caps={"MAX_QTY": 2},
            )

        _write_heartbeat({"status": "running", "last_run": last_run})
        time.sleep(5)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, subprocess

    # Accept your earlier alias/typo before parsing
    if "--liveSafety" in sys.argv and "--live-safety" not in sys.argv:
        sys.argv = [("--live-safety" if a == "--liveSafety" else a) for a in sys.argv]

    p = argparse.ArgumentParser(description="Angel Auto Trader — Daily Scheduler (IST)")
    p.add_argument("--once", choices=["open", "square", "eod"], help="Run a single task immediately and exit")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    p.add_argument("--stop", action="store_true", help="Stop the running scheduler using the lock PID and exit")
    p.add_argument("--force", action="store_true", help="Ignore an existing lock and start anyway (not recommended)")
    p.add_argument("--refresh-holidays", action="store_true",
                   help="Fetch holidays from NSE and write data/nse_holidays.csv, then exit")
    p.add_argument("--live", action="store_true", help="Run tasks in LIVE mode instead of DRY_RUN")
    p.add_argument("--live-safety", dest="live_safety", action="store_true",
                   help="Refuse to run LIVE if market is closed/holiday")
    args = p.parse_args()

    _setup_logger(args.verbose)
    MODE = "LIVE" if args.live else "DRY_RUN"

    # Refresh holiday file and exit
    if args.refresh_holidays:
        live = fetch_holidays_live()
        if not live:
            logger.error("Could not fetch from NSE (blocked or schema change). Try again later.")
            sys.exit(2)
        save_holidays_to_csv(live)
        logger.success(f"Wrote {len(live)} holidays to data/nse_holidays.csv")
        sys.exit(0)

    # Stop command
    if args.stop:
        try:
            pid = int((LOCK_FILE.read_text() or "0").strip() or "0")
        except Exception:
            pid = 0
        if pid and _pid_alive(pid):
            _kill_pid(pid)
            logger.success(f"Sent stop to scheduler PID {pid}")
        else:
            logger.info("No live scheduler found (or stale PID).")
        try:
            if LOCK_FILE.exists():
                LOCK_FILE.unlink()
                logger.info(f"Removed lock {LOCK_FILE}")
        except Exception:
            pass
        sys.exit(0)

    # One-shot tasks (no lock)
    holidays_for_guard = load_holidays_combined()
    if args.once:
        if MODE == "LIVE" and args.live_safety and not market_open_now(holidays_for_guard):
            logger.warning("LIVE blocked by --live-safety: market is closed or today is a holiday.")
            sys.exit(3)

        logger.info(f"Running single task: {args.once} ({MODE})")
        if args.once == "open":
            _run_goals([Goal(text="run_atm_straddle", params={"underlying": "BANKNIFTY", "lots": 1, "strike_step": 100})],
                       mode=MODE, caps={"MAX_QTY": 1})
        elif args.once == "square":
            _run_goals([Goal(text="square_off_all", params={"mode": MODE, "include_products": ["INTRADAY"]})],
                       mode=MODE, caps={"MAX_QTY": 10})
        else:
            _run_goals([Goal(text="eod_report", params={"day": _today_ist().isoformat()})],
                       mode=MODE, caps={"MAX_QTY": 2})
        sys.exit(0)

    # LIVE safety for daemon start
    if MODE == "LIVE" and args.live_safety and not market_open_now(holidays_for_guard):
        logger.warning("LIVE daemon blocked by --live-safety: market is closed or today is a holiday.")
        sys.exit(3)

    # Graceful shutdown on signals
    def _shutdown(_sig, _frm):
        logger.info("Shutting down scheduler (signal received).")
        _release_lock()
        try:
            HEARTBEAT_FILE.unlink(missing_ok=True)  # py>=3.8
        except Exception:
            pass
        sys.exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _shutdown)
        except Exception:
            pass

    if not args.force:
        _acquire_lock()
        atexit.register(_release_lock)
    else:
        logger.warning("--force specified: skipping lock acquisition")

    main_loop(verbose=args.verbose, mode=MODE)
