# ops/log_rotation.py
from __future__ import annotations
from pathlib import Path
from loguru import logger
import sys

def setup_logging(
    log_dir: str = "logs",
    base_name: str = "app",
    rotation: str = "100 MB",   # rotate when file > 100MB
    retention: str = "14 days", # keep 14 days
    level: str = "INFO"
) -> None:
    """
    Configure loguru with console + rotating file sinks.
    """
    logger.remove()  # clear default sink
    # console
    logger.add(sys.stdout, level=level,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}:{function}:{line}</cyan> | <level>{message}</level>")
    # file
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logfile = Path(log_dir) / f"{base_name}.log"
    logger.add(
        logfile.as_posix(),
        rotation=rotation,
        retention=retention,
        level=level,
        enqueue=True,  # safe for threads
        backtrace=True,
        diagnose=False,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
    )
