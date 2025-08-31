# scripts/print_registry.py
from __future__ import annotations
from core.strategy_registry import REGISTRY

if __name__ == "__main__":
    print("\nRegistered strategies:")
    for k in sorted(REGISTRY.keys()):
        print(" -", k)
