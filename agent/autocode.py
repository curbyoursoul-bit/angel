# agent/autocode.py
from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from loguru import logger
from string import Template

from agent.templates import ALLOWLIST

ROOT = Path(__file__).resolve().parents[1]
GEN_DIR = ROOT / "strategies" / "generated"
GEN_DIR.mkdir(parents=True, exist_ok=True)

def _sanitize_name(name: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch in ("_", "-")) else "_" for ch in name.strip())
    if not safe:
        safe = f"gen_{int(datetime.now().timestamp())}"
    return safe

def generate_strategy(
    name: str,
    template_key: str,
    params: Dict[str, Any],
    overwrite: bool = False,
) -> Path:
    """
    Render a new strategy python file under strategies/generated/{name}.py
    Returns the created path.
    """
    if template_key not in ALLOWLIST:
        raise ValueError(f"Unsupported template '{template_key}'. Allowed: {sorted(ALLOWLIST.keys())}")

    # Required common metadata (used by templates)
    defaults = {
        "name": _sanitize_name(name),
        "symbol": params.get("symbol", "BANKNIFTY"),
        "timeframe": params.get("timeframe", "5m"),
    }
    merged = {**defaults, **params}

    tmpl: Template = ALLOWLIST[template_key]
    code: str = tmpl.substitute(**merged)

    out_path = GEN_DIR / f"{defaults['name']}.py"
    if out_path.exists() and not overwrite:
        raise FileExistsError(f"{out_path.name} already exists. Pass overwrite=True to replace.")

    out_path.write_text(code, encoding="utf-8")
    logger.info(f"Generated strategy: {out_path}")
    return out_path
