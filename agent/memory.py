# agent/memory.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Deque, Iterable
from collections import deque
from threading import RLock
import time, os, json

class Memory:
    """
    Ultra-light in-memory store with recency. Optionally persists to a JSONL file.
    API (backward-compatible):
      - write(kind: str, payload: Dict[str, Any]) -> None
      - recent(kind: Optional[str] = None, n: int = 20) -> List[Dict[str, Any]]

    Extras:
      - last(kind: Optional[str] = None) -> Optional[Dict[str, Any]]
      - clear(kind: Optional[str] = None) -> None
      - stats() -> Dict[str, int]
      - find(kind: Optional[str], where: callable(event_dict) -> bool, limit: int = 50)
    """

    def __init__(self, maxlen: int = 200, storage_path: Optional[str] = None, autosave: bool = True):
        self._lock = RLock()
        self._events: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._storage_path = storage_path
        self._autosave = autosave
        if self._storage_path:
            self._ensure_dir(self._storage_path)
            self._load_from_disk_safely()

    # ---------------- public API ----------------

    def write(self, kind: str, payload: Dict[str, Any]) -> None:
        evt = {
            "t": time.time(),              # seconds since epoch (float)
            "kind": str(kind or "").strip(),
            "payload": payload if isinstance(payload, dict) else {"value": payload},
        }
        with self._lock:
            self._events.append(evt)
            if self._storage_path and self._autosave:
                self._append_jsonl(evt)

    def recent(self, kind: Optional[str] = None, n: int = 20) -> List[Dict[str, Any]]:
        n = 0 if n is None else max(0, int(n))
        with self._lock:
            items = list(self._events)[-n:] if n > 0 else list(self._events)
            if kind:
                k = str(kind)
                items = [e for e in items if e.get("kind") == k]
            return items

    # ---------------- handy extras (optional) ----------------

    def last(self, kind: Optional[str] = None) -> Optional[Dict[str, Any]]:
        with self._lock:
            if kind is None:
                return self._events[-1] if self._events else None
            for e in reversed(self._events):
                if e.get("kind") == kind:
                    return e
            return None

    def clear(self, kind: Optional[str] = None) -> None:
        with self._lock:
            if kind is None:
                self._events.clear()
            else:
                kept = [e for e in self._events if e.get("kind") != kind]
                self._events.clear()
                for e in kept:
                    self._events.append(e)
            # note: does not wipe disk file (safer). Call wipe_disk() if you need that.

    def stats(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for e in self._events:
                k = e.get("kind", "")
                out[k] = out.get(k, 0) + 1
            out["_total"] = len(self._events)
            return out

    def find(self, kind: Optional[str], where, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Find events matching a predicate: where(event_dict) -> bool
        """
        limit = max(0, int(limit))
        out: List[Dict[str, Any]] = []
        with self._lock:
            it: Iterable[Dict[str, Any]] = reversed(self._events)
            for e in it:
                if kind and e.get("kind") != kind:
                    continue
                try:
                    if where(e):
                        out.append(e)
                        if len(out) >= limit:
                            break
                except Exception:
                    # ignore bad predicates
                    continue
        return list(reversed(out))

    # ---------------- persistence helpers ----------------

    def wipe_disk(self) -> None:
        """Delete the storage file if configured. In-memory events are kept."""
        if not self._storage_path:
            return
        try:
            os.remove(self._storage_path)
        except FileNotFoundError:
            pass
        except Exception:
            # best-effort
            pass

    def _ensure_dir(self, path: str) -> None:
        d = os.path.dirname(path) or "."
        os.makedirs(d, exist_ok=True)

    def _append_jsonl(self, evt: Dict[str, Any]) -> None:
        try:
            with open(self._storage_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(evt, ensure_ascii=False) + "\n")
        except Exception:
            # best-effort persistence; do not crash
            pass

    def _load_from_disk_safely(self) -> None:
        try:
            if not os.path.exists(self._storage_path):
                return
            # load up to maxlen most recent lines
            lines: List[str] = []
            with open(self._storage_path, "r", encoding="utf-8") as f:
                for line in f:
                    lines.append(line)
            # only keep the tail according to current deque capacity
            for line in lines[-self._events.maxlen:]:
                try:
                    evt = json.loads(line)
                    if isinstance(evt, dict) and "kind" in evt and "t" in evt:
                        self._events.append(evt)
                except Exception:
                    continue
        except Exception:
            # ignore any file issues; keep memory usable
            pass
