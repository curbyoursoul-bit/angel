# tools/report_tool.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from pathlib import Path
import csv
from datetime import datetime, date

from tools.base import Tool
from utils.market_hours import IST


class ReportTool(Tool):
    name = "report"

    def __init__(self, path: str = "logs/agent_reports.csv"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(["ts", "goal", "action", "ok", "summary"])

    # Tool.run will auto-route to these _<fn> handlers

    def _log_step(self, *, goal: str = "", action: str = "", ok: bool = False, summary: str = "") -> Dict[str, Any]:
        """Append a single agent step log row to logs/agent_reports.csv."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([
                datetime.now().isoformat(timespec="seconds"),
                goal,
                action,
                bool(ok),
                summary,
            ])
        return {"ok": True, "data": str(self.path)}

    def _eod_report(self, *, day: Optional[str] = None, trades_csv: Optional[str] = None) -> Dict[str, Any]:
        """
        Build an End-Of-Day markdown report (FIFO realized P&L by symbol and ordertag).

        Params:
          - day: "YYYY-MM-DD" (defaults to today IST)
          - trades_csv: override path to TRADE_LOG_CSV (defaults to config.TRADE_LOG_CSV)

        Returns:
          { ok, data: { day, total_realized, by_symbol, by_tag, path } }
        """
        # Resolve date
        d = date.fromisoformat(day) if day else datetime.now(IST).date()

        # Resolve trades CSV path
        if trades_csv is None:
            try:
                from config import TRADE_LOG_CSV as _TRADES
                trades_csv = _TRADES
            except Exception as e:
                return {"ok": False, "error": f"TRADE_LOG_CSV not set in config: {e}"}
        tpath = Path(trades_csv)

        # Load + compute
        try:
            from utils.pnl import load_trades, realized_fifo_pnl
        except Exception as e:
            return {"ok": False, "error": f"pnl utils unavailable: {e}"}

        try:
            trades = load_trades(tpath, d)
        except Exception as e:
            return {"ok": False, "error": f"Failed to load trades from {tpath}: {e}"}

        try:
            total, by_sym, by_tag = realized_fifo_pnl(trades)
        except Exception as e:
            return {"ok": False, "error": f"Failed to compute FIFO P&L: {e}"}

        # Compose markdown
        out_dir = Path("logs/eod")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{d.isoformat()}.md"

        def _fmt_money(x: float) -> str:
            return f"{x:,.2f}"

        lines: List[str] = []
        lines.append(f"# EOD Report — {d.isoformat()}\n")
        lines.append(f"- Trades loaded: **{len(trades)}** from `{tpath}`")
        lines.append(f"- Realized P&L: **₹ {_fmt_money(total)}**\n")

        if by_sym:
            lines.append("## P&L by Symbol")
            lines.append("| Symbol | Realized P&L (₹) |")
            lines.append("|---|---:|")
            for sym, pnl in sorted(by_sym.items(), key=lambda kv: kv[0]):
                lines.append(f"| {sym} | {_fmt_money(pnl)} |")
            lines.append("")

        if by_tag:
            lines.append("## P&L by OrderTag")
            lines.append("| OrderTag | Realized P&L (₹) |")
            lines.append("|---|---:|")
            for tag, pnl in sorted(by_tag.items(), key=lambda kv: kv[0]):
                t = tag if tag else "UNSPECIFIED"
                lines.append(f"| {t} | {_fmt_money(pnl)} |")
            lines.append("")

        lines.append("## Notes")
        lines.append("- Realized P&L is computed using **FIFO** from logged trade prices only.")
        lines.append("- Open positions at EOD are **not** marked-to-market in this report.")
        lines.append("- DRY-RUN trades are included if they had non-zero prices; filter upstream if needed.\n")

        out_path.write_text("\n".join(lines), encoding="utf-8")

        return {
            "ok": True,
            "data": {
                "day": d.isoformat(),
                "total_realized": total,
                "by_symbol": by_sym,
                "by_tag": by_tag,
                "path": str(out_path),
            },
        }
