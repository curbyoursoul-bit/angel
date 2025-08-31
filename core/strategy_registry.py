# core/strategy_registry.py
from __future__ import annotations
from typing import Callable, Dict, Optional
from types import ModuleType
from pathlib import Path
import importlib
import importlib.util
import re
from loguru import logger

# Public: used by engine
REGISTRY: Dict[str, Callable] = {}
ALIASES: Dict[str, str] = {}

# Where to look
STRAT_DIR = Path(__file__).resolve().parent.parent / "strategies"

# -------------------------------
# Helpers
# -------------------------------

def _canon(name: str) -> str:
    """Normalize a filename/module name to a canonical registry key."""
    s = name.strip().lower()
    s = re.sub(r"[^\w]+", "_", s)     # non-word -> _
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _alias_set(canon: str) -> set[str]:
    """
    Build useful aliases from a canonical name.
    Examples:
      atm_straddle -> {"atm_straddle","atm-straddle","atm","straddle","atmstraddle"}
      atm_iron_fly -> {"atm_iron_fly","atm-iron-fly","ironfly","ifly","atm","iron","fly","atmironfly"}
    """
    aliases: set[str] = set()
    aliases.add(canon)
    aliases.add(canon.replace("_", "-"))
    toks = canon.split("_")

    # token-based
    aliases.update(toks)                      # atm, iron, fly
    aliases.add("".join(toks))                # atmironfly
    if len(toks) >= 2:
        aliases.add("_".join(toks[:2]))       # atm_iron
        aliases.add(toks[0] + toks[-1])       # atmfly

    # opinionated shorthands
    if canon == "atm_straddle":
        aliases.update({"atm", "straddle", "atmstraddle"})
    if canon in {"atm_iron_fly", "iron_fly"}:
        aliases.update({"ironfly", "ifly", "iron_fly", "iron"})
    if canon.endswith("_breakout"):
        aliases.add("breakout")
    if canon.endswith("_mean_reversion"):
        aliases.update({"mr", "meanreversion"})

    # remove empties/dupes
    return {a for a in aliases if a}

def _load_module_from_path(mod_name: str, path: Path) -> Optional[ModuleType]:
    spec = importlib.util.spec_from_file_location(mod_name, str(path))
    if not spec or not spec.loader:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module
    except Exception as e:
        logger.error(f"Failed to load strategy '{path.name}': {e}")
        return None

def _discover_files() -> list[Path]:
    if not STRAT_DIR.exists():
        logger.warning(f"Strategies dir not found: {STRAT_DIR}")
        return []
    files: list[Path] = []
    for p in STRAT_DIR.glob("*.py"):
        name = p.name
        if name.startswith("_") or name == "__init__.py":
            continue
        files.append(p)
    return files

def _register_module(mod: ModuleType, key_hint: Optional[str] = None) -> None:
    fn = getattr(mod, "run", None)
    if not callable(fn):
        return
    # Determine canonical name
    if key_hint:
        canon = _canon(key_hint)
    else:
        mod_name = getattr(mod, "__name__", "")
        canon = _canon(mod_name.rsplit(".", 1)[-1] or mod_name)

    # Register
    if canon in REGISTRY:
        logger.warning(f"Overriding existing strategy '{canon}'")
    REGISTRY[canon] = fn

    # Aliases
    for a in _alias_set(canon):
        # don't overwrite an explicit mapping if already present
        ALIASES.setdefault(a, canon)

# -------------------------------
# Public API (manual registration)
# -------------------------------

def register(name: str, fn: Callable) -> None:
    key = _canon(name)
    if not key:
        raise ValueError("Strategy name cannot be empty")
    if not callable(fn):
        raise TypeError("Strategy must be callable and accept a SmartAPI client param")
    if key in REGISTRY:
        logger.warning(f"Overriding existing strategy '{key}'")
    REGISTRY[key] = fn
    # refresh aliases for this key
    for a in _alias_set(key):
        ALIASES[a] = key
    logger.info(f"Registered strategy: {key}")

def get_strategy_names() -> list[str]:
    return sorted(REGISTRY.keys())

def get_strategy_callable(name: str) -> Callable:
    if not name:
        raise KeyError("No strategy name provided")
    key = ALIASES.get(_canon(name), _canon(name))
    fn = REGISTRY.get(key)
    if not fn:
        available = ", ".join(get_strategy_names())
        raise KeyError(f"Unknown strategy '{name}'. Available: {available}")
    return fn

# -------------------------------
# Discovery bootstrap
# -------------------------------

# 1) Import packages in 'strategies' normally if present (so relative imports work)
try:
    import strategies  # noqa: F401
except Exception:
    # Not fatal; we'll still load by file path
    pass

# 2) Discover and load every strategies/*.py with a run() function
for py in _discover_files():
    mod_name = f"strategies.{py.stem}"
    mod = None
    # Try importlib.import_module first (honors package __init__ if present)
    try:
        mod = importlib.import_module(mod_name)
    except Exception:
        # Fallback to path-based loader
        mod = _load_module_from_path(mod_name, py)
    if mod is not None:
        _register_module(mod, key_hint=py.stem)

# 3) Opinionated extra aliases (explicit mapping wins)
ALIASES.update({
    "ema": "ema_crossover",
    "bb": "bollinger_breakout",
    "orb": "orb_breakout",
    "vwap": "vwap_mean_reversion",
    "vol": "volume_breakout",
    "volbreak": "volume_breakout",
    "momo": "equity_momentum",
    "pairs": "pairs_trading",
    "theta": "theta_short",
    "vix": "vix_regime",
    "news": "news_reactor",
})

# 4) Dynamically load generated strategies if available
try:
    from core.generated_registry import extend_registry
    before = set(REGISTRY.keys())
    extend_registry(REGISTRY)
    after = set(REGISTRY.keys())
    new = sorted(after - before)
    if new:
        logger.info(f"Generated strategies registered: {', '.join(new)}")
    else:
        logger.info("No new generated strategies found.")
except Exception as e:
    logger.debug(f"No generated strategies extension applied: {e}")

# Final summary
logger.info(f"Strategy registry ready — {len(REGISTRY)} strategy(ies) loaded.")
