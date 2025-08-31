from __future__ import annotations
from typing import List, Dict, Any
from loguru import logger

def run(smart) -> List[Dict[str, Any]]:
    logger.info("news_reactor: no news source configured — skipping.")
    return []
