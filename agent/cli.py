# agent/cli.py
from __future__ import annotations
import argparse, json, sys, os
from typing import List, Dict, Any
from agent.agent import Agent
from agent.types import Goal

def _load_params(params_str: str | None, params_file: str | None) -> Dict[str, Any]:
    if params_file:
        try:
            with open(params_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to read --params-file: {e}", file=sys.stderr)
            sys.exit(2)
    if params_str:
        try:
            return json.loads(params_str)
        except Exception as e:
            print(f"Invalid --params JSON: {e}", file=sys.stderr)
            sys.exit(2)
    return {}

def _parse_goals(goal: str | None, goals_csv: str | None) -> List[str]:
    names: List[str] = []
    if goal:
        names.append(goal.strip())
    if goals_csv:
        names.extend([g.strip() for g in goals_csv.split(",") if g.strip()])
    return names

def main():
    p = argparse.ArgumentParser(description="Run agent goals from CLI")
    p.add_argument("--mode", choices=["DRY_RUN", "LIVE"], default="DRY_RUN", help="Execution mode")

    # Single or multiple goals
    p.add_argument("--goal", help="Single goal, e.g. run_atm_straddle")
    p.add_argument("--goals", help="CSV of goals, e.g. run_atm_straddle,run_orb_breakout")

    # Params
    p.add_argument("--params", default=None, help="JSON dict of params for the goal(s)")
    p.add_argument("--params-file", default=None, help="Path to JSON file of params")
    p.add_argument("--maxqty", type=int, default=2, help="Risk cap: MAX_QTY")

    # Behavior
    p.add_argument("--rollback", action="store_true", help="Rollback already placed legs if any leg fails")
    p.add_argument("--bypass-market-hours", action="store_true", help="Skip market-hours check (env override)")
    p.add_argument("--json", action="store_true", help="Print JSON summary of executed steps")

    args = p.parse_args()

    # Resolve goals
    goal_names = _parse_goals(args.goal, args.goals)
    if not goal_names:
        print("Please provide --goal or --goals", file=sys.stderr)
        sys.exit(2)

    # Params (shared across goals by default)
    params = _load_params(args.params, args.params_file)

    # Optional env override for market-hours
    if args.bypass_market_hours:
        os.environ["AGENT_BYPASS_MKT_HOURS"] = "1"

    agent = Agent(mode=args.mode, caps={"MAX_QTY": args.maxqty}, rollback_on_failure=args.rollback)

    # Build Goal objects
    goals = [Goal(text=g, params=params) for g in goal_names]
    agent.loop(goals)

    # Summarize & exit code
    try:
        steps = agent.memory.recent("step", n=len(goal_names))
    except Exception:
        steps = []

    if args.json:
        print(json.dumps({"mode": args.mode, "steps": steps}, indent=2, ensure_ascii=False))
    else:
        for st in steps:
            print(f"[{st.get('ok') and 'OK' or 'ERR'}] {st.get('action')} — {st.get('summary','')}")

    # Exit non-zero if any step failed
    failed = any(not s.get("ok", False) for s in steps)
    sys.exit(1 if failed else 0)

if __name__ == "__main__":
    main()
