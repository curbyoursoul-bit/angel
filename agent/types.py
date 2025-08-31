# agent/types.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Literal

Mode = Literal["DRY_RUN", "LIVE"]

@dataclass
class Goal:
    text: str
    params: Dict[str, Any] = field(default_factory=dict)  # e.g. {"underlying": "BANKNIFTY", "qty": 1}

    def __repr__(self) -> str:
        return f"Goal({self.text}, params={{{', '.join(f'{k}={v}' for k,v in self.params.items())}}})"

@dataclass
class Action:
    tool: str
    name: str
    args: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Observation:
    ok: bool
    data: Any | None = None
    error: Optional[str] = None

@dataclass
class Step:
    goal: Goal
    action: Action
    observation: Observation

    @property
    def status(self) -> str:
        return "ok" if self.observation and self.observation.ok else "err"

    def __repr__(self) -> str:
        return f"Step(goal={self.goal.text}, action={self.action.tool}.{self.action.name}, status={self.status})"

@dataclass
class AgentState:
    mode: Mode = "DRY_RUN"
    steps: List[Step] = field(default_factory=list)
    scratch: Dict[str, Any] = field(default_factory=dict)  # ephemeral, per-run scratchpad
