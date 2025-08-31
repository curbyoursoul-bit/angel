# agent/agent.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from loguru import logger
import os

# ---- guarded imports (no-crash fallbacks) -----------------------------------
try:
    from agent.types import AgentState, Goal, Step, Observation  # type: ignore
except Exception:
    @dataclass
    class Goal:  # minimal stub
        text: str
        params: Dict[str, Any] | None = None
    @dataclass
    class Observation:
        ok: bool
        data: Any = None
        error: Optional[str] = None
    @dataclass
    class Step:
        goal: Goal
        action: Any
        observation: Observation
    @dataclass
    class AgentState:
        mode: str = "DRY_RUN"
        steps: List[Step] = None
        def __post_init__(self):
            if self.steps is None:
                self.steps = []

try:
    from agent.planner import Planner  # type: ignore
except Exception:
    class Planner:
        def plan(self, goal: Goal, context: Dict[str, Any] | None = None):
            # Minimal action stub: call strategy tool with goal.text
            class _Action:
                tool = "strategy"
                name = goal.text
                args = goal.params or {}
            logger.warning("[planner-fallback] Using trivial plan for goal %s", goal.text)
            return _Action()

try:
    from agent.executor import Executor  # type: ignore
except Exception:
    class Executor:
        def run(self, tool: str, name: str, **kwargs):
            logger.warning("[executor-fallback] Would run tool=%s name=%s kwargs=%s", tool, name, kwargs)
            # emulate a successful dry-run
            return {"ok": True, "data": {"orders": kwargs.get("orders", []), "note": "fallback-exec"}, "error": None}

try:
    from agent.memory import Memory  # type: ignore
except Exception:
    class Memory:
        _store: Dict[str, List[Dict[str, Any]]] = {}
        def write(self, key: str, value: Dict[str, Any]):
            self._store.setdefault(key, []).append(value)
        def recent(self, key: str, n: int = 5):
            return (self._store.get(key) or [])[-n:]

# policies (risk + market hours)
try:
    from agent.policies import enforce_risk_caps, market_is_open  # type: ignore
except Exception:
    def enforce_risk_caps(order: Dict[str, Any], caps: Dict[str, Any]) -> Dict[str, Any]:
        max_qty = int(caps.get("MAX_QTY", 999999))
        q = int(order.get("quantity") or order.get("qty") or 0)
        if q > max_qty:
            order = {**order, "quantity": max_qty}
        return order
    def market_is_open(_offset_minutes: int = 0) -> bool:
        # default to open if policy missing
        return True

# ---- Agent ------------------------------------------------------------------
class Agent:
    def __init__(self, mode: str = "DRY_RUN", caps: Dict[str, Any] | None = None, rollback_on_failure: bool = False):
        self.state = AgentState(mode=mode)
        self.memory = Memory()
        self.planner = Planner()
        self.exec = Executor()
        self.caps = caps or {"MAX_QTY": 2}
        self.rollback_on_failure = bool(rollback_on_failure)

    def _normalize_obs(self, raw: Any) -> Observation:
        if isinstance(raw, dict):
            ok = bool(raw.get("ok", False))
            return Observation(ok=ok, data=raw.get("data"), error=raw.get("error"))
        # tolerate weird returns
        return Observation(ok=bool(raw), data=raw, error=None if raw else "no data")

    def run_once(self, goal: Goal) -> Step:
        # 0) market-hours quick gate (can be bypassed)
        if os.getenv("AGENT_BYPASS_MKT_HOURS", "").lower() in {"1", "true", "yes", "y"}:
            market_open = True
        else:
            try:
                market_open = market_is_open(0)
            except Exception as e:
                logger.warning("market_is_open check failed: %s (continuing)", e)
                market_open = True

        if not market_open:
            logger.warning("Market closed; skipping goal: %s", getattr(goal, "text", str(goal)))
            # still log a no-op step
            action = type("Action", (), {"tool": "noop", "name": "market_closed", "args": {}})()
            obs = Observation(ok=True, data={"skipped": True}, error=None)
            step = Step(goal=goal, action=action, observation=obs)
            self.state.steps.append(step)
            try:
                self.memory.write("step", {"goal": goal.text, "action": "noop.market_closed", "ok": True, "summary": "skipped"})
            except Exception:
                pass
            try:
                self.exec.run("report", "log_step", goal=goal.text, action="noop.market_closed", ok=True, summary="skipped")
            except Exception:
                pass
            return step

        # 1) plan
        try:
            action = self.planner.plan(goal, context={"mode": self.state.mode})
        except Exception as e:
            logger.exception("Planner failed for goal %s: %s", goal.text, e)
            action = type("Action", (), {"tool": "noop", "name": "planner_error", "args": {"error": str(e)}})()

        # 2) act
        try:
            tool = getattr(action, "tool", "noop")
            name = getattr(action, "name", "noop")
            args = getattr(action, "args", {}) or {}
            obs_raw = self.exec.run(tool, name, **args)
        except Exception as e:
            logger.exception("Executor failed for %s.%s: %s", getattr(action, "tool", "?"), getattr(action, "name", "?"), e)
            obs_raw = {"ok": False, "data": None, "error": str(e)}
        obs = self._normalize_obs(obs_raw)

        # 3) reflect & place (batch) if strategy produced orders
        summary = ""
        if getattr(action, "tool", "") == "strategy" and obs.ok:
            out = obs.data or {}
            orders = out.get("orders", []) if isinstance(out, dict) else []
            safe_orders: List[Dict[str, Any]] = []

            for od in orders or []:
                try:
                    od = dict(od)  # shallow copy
                    # sane defaults (your broker/order_exec will re-normalize too)
                    od.setdefault("producttype", "INTRADAY")
                    od.setdefault("exchange", "NFO")
                    # clamp qty via policy caps
                    od = enforce_risk_caps(od, self.caps)
                    safe_orders.append(od)
                except Exception as e:
                    safe_orders.append({"_error": f"risk normalize error: {e}"})

            try:
                batch_res = self.exec.run(
                    "angel",
                    "place_orders",
                    mode=("LIVE" if self.state.mode.upper() == "LIVE" else "DRY_RUN"),
                    orders=safe_orders,
                    rollback_on_failure=self.rollback_on_failure,
                )
            except Exception as e:
                logger.exception("Batch place failed: %s", e)
                batch_res = {"ok": False, "data": None, "error": str(e)}

            obs = self._normalize_obs(batch_res)
            # keep notes from strategy output if any
            notes = out.get("notes", "") if isinstance(out, dict) else ""
            if isinstance(obs.data, dict):
                obs.data.setdefault("notes", notes)
                obs.data.setdefault("dry_run", self.state.mode.upper() != "LIVE")
            summary = f"strategy batch placed n={len(safe_orders)} ok={obs.ok} live={self.state.mode.upper()=='LIVE'}"

        # 4) log to memory & report
        try:
            self.memory.write("step", {
                "goal": getattr(goal, "text", str(goal)),
                "action": f"{getattr(action, 'tool', '?')}.{getattr(action, 'name', '?')}",
                "ok": bool(getattr(obs, "ok", False)),
                "summary": summary
            })
        except Exception:
            pass

        try:
            self.exec.run("report", "log_step",
                          goal=getattr(goal, "text", str(goal)),
                          action=f"{getattr(action, 'tool', '?')}.{getattr(action, 'name', '?')}",
                          ok=bool(getattr(obs, "ok", False)),
                          summary=summary)
        except Exception:
            pass

        step = Step(goal=goal, action=action, observation=obs)
        self.state.steps.append(step)
        return step

    def loop(self, goals: List[Goal]):
        # single pass over provided goals (cron externally)
        for g in goals:
            try:
                self.run_once(g)
            except Exception as e:
                logger.exception("Agent loop error for goal %s: %s", getattr(g, "text", str(g)), e)


if __name__ == "__main__":
    # EXAMPLES:
    goals = [
        Goal(text="run_atm_straddle", params={"underlying": "BANKNIFTY", "lots": 1, "strike_step": 100}),
        Goal(text="run_orb_breakout", params={"symbols": ["HDFCBANK", "ICICIBANK"], "interval": "FIFTEEN_MINUTE", "bars": 300, "qty": 1}),
    ]
    agent = Agent(mode="DRY_RUN", caps={"MAX_QTY": 2}, rollback_on_failure=False)
    agent.loop(goals)
    try:
        print("recent memory:", agent.memory.recent("step", n=5))
    except Exception:
        pass
