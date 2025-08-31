# core/generated_registry.py
from __future__ import annotations
import importlib.util, sys
from pathlib import Path
from typing import Dict, Callable
from loguru import logger

def _load_py_module(mod_path: Path):
    spec = importlib.util.spec_from_file_location(mod_path.stem, mod_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_path.stem] = mod
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def extend_registry(registry: Dict[str, Callable]) -> None:
    root = Path(__file__).resolve().parents[1]
    gen_dir = root / "strategies" / "generated"
    if not gen_dir.exists():
        logger.info("No generated strategies directory found.")
        return

    for p in gen_dir.glob("*.py"):
        try:
            mod = _load_py_module(p)
            run = getattr(mod, "run", None)
            if run is None or not callable(run):
                logger.warning(f"Skip {p.name}: no callable run(df) found.")
                continue
            name = getattr(mod, "NAME", None) or f"gen_{p.stem}"
            if name in registry:
                logger.warning(f"Registry already has '{name}', skipping.")
                continue
            registry[name] = run
            logger.info(f"Registered generated strategy: {name}")
        except Exception as e:
            logger.exception(f"Failed loading generated strategy {p.name}: {e}")
