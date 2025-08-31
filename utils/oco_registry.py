# utils/oco_registry.py
from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, List
from loguru import logger

from config import OCO_REGISTRY_JSON

_REG_PATH = Path(OCO_REGISTRY_JSON)


# -------------------- storage --------------------

def _load() -> Dict[str, Any]:
    if _REG_PATH.exists():
        try:
            text = _REG_PATH.read_text(encoding="utf-8") or "{}"
            data = json.loads(text)
            if isinstance(data, dict):
                return data
            logger.warning("OCO registry: root is not a dict; recreating.")
        except Exception as e:
            logger.warning(f"OCO registry load failed, recreating: {e}")
    return {}


def _save(data: Dict[str, Any]) -> None:
    _REG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _REG_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(_REG_PATH)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


# -------------------- core API --------------------

def new_group_id(tradingsymbol: str) -> str:
    """
    Create a new OCO group bucket and return its id.
    """
    gid = f"OCO-{tradingsymbol}-{uuid.uuid4().hex[:8]}"
    reg = _load()
    reg[gid] = {
        "tradingsymbol": str(tradingsymbol or "").upper(),
        "created_at": _now(),
        "closed": False,
        "closed_reason": None,
        "closed_at": None,
        "primary": None,   # raw primary order payload you placed
        "stop": None,      # {"order_id": "...", "order": {...}}
        "target": None,    # {"order_id": "...", "order": {...}}
        "notes": [],
    }
    _save(reg)
    return gid


def record_primary(group_id: str, order: Dict[str, Any]) -> None:
    reg = _load()
    rec = reg.setdefault(group_id, {
        "tradingsymbol": "",
        "created_at": _now(),
        "closed": False,
        "closed_reason": None,
        "closed_at": None,
        "primary": None,
        "stop": None,
        "target": None,
        "notes": [],
    })
    rec["primary"] = order
    rec.setdefault("updated_at", _now())
    rec["updated_at"] = _now()
    _save(reg)


def record_stop(group_id: str, order_id: str, order: Dict[str, Any]) -> None:
    reg = _load()
    rec = reg.setdefault(group_id, {})
    rec["stop"] = {"order_id": str(order_id), "order": order}
    rec.setdefault("updated_at", _now())
    rec["updated_at"] = _now()
    _save(reg)


def record_target(group_id: str, order_id: str, order: Dict[str, Any]) -> None:
    reg = _load()
    rec = reg.setdefault(group_id, {})
    rec["target"] = {"order_id": str(order_id), "order": order}
    rec.setdefault("updated_at", _now())
    rec["updated_at"] = _now()
    _save(reg)


def mark_closed(group_id: str, reason: str | None = None) -> None:
    """
    Mark OCO group as closed (e.g., 'exit_by_stop', 'exit_by_target', 'manual').
    """
    reg = _load()
    rec = reg.get(group_id)
    if not rec:
        logger.warning(f"OCO mark_closed: group {group_id} not found")
        return
    rec["closed"] = True
    rec["closed_reason"] = reason or rec.get("closed_reason") or "closed"
    rec["closed_at"] = _now()
    rec.setdefault("updated_at", _now())
    rec["updated_at"] = _now()
    _save(reg)


# -------------------- convenience / queries --------------------

def all_groups() -> Dict[str, Any]:
    """
    Return the entire registry dict (id -> record).
    """
    return _load()


def get_group(group_id: str) -> Optional[Dict[str, Any]]:
    return _load().get(group_id)


def list_open_groups(symbol: str | None = None) -> Dict[str, Any]:
    """
    Return only groups not marked closed. Optionally filter by symbol.
    """
    reg = _load()
    out: Dict[str, Any] = {}
    for gid, rec in reg.items():
        if rec.get("closed"):
            continue
        if symbol:
            sym = str(rec.get("tradingsymbol") or "").upper()
            if sym != str(symbol).upper():
                continue
        out[gid] = rec
    return out


def append_note(group_id: str, note: str) -> None:
    reg = _load()
    rec = reg.get(group_id)
    if not rec:
        logger.warning(f"OCO append_note: group {group_id} not found")
        return
    rec.setdefault("notes", [])
    rec["notes"].append({"t": _now(), "text": str(note)})
    rec.setdefault("updated_at", _now())
    rec["updated_at"] = _now()
    _save(reg)


def remove_group(group_id: str) -> None:
    """
    Hard-delete a group from the registry.
    Useful for cleanup after archival/log export.
    """
    reg = _load()
    if group_id in reg:
        del reg[group_id]
        _save(reg)
    else:
        logger.warning(f"OCO remove_group: group {group_id} not found")


def clear_registry(keep_closed: bool = True) -> int:
    """
    Remove entries from the registry.
    - keep_closed=True: drop only closed groups
    - keep_closed=False: wipe everything
    Returns number of entries removed.
    """
    reg = _load()
    if not reg:
        return 0
    if keep_closed:
        ids = [gid for gid, rec in reg.items() if rec.get("closed")]
        for gid in ids:
            del reg[gid]
    else:
        ids = list(reg.keys())
        reg = {}
    _save(reg)
    return len(ids)
