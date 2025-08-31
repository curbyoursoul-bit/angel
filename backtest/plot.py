from __future__ import annotations
import pandas as pd
import matplotlib.pyplot as plt

def plot_equity_and_drawdown(equity: pd.Series, out_png: str | None = None):
    eq = equity.dropna()
    dd = (eq / eq.cummax()) - 1.0

    fig = plt.figure(figsize=(12, 6))
    ax1 = fig.add_subplot(2, 1, 1)
    ax1.plot(eq.index, eq.values)
    ax1.set_title("Equity Curve")
    ax1.grid(True)

    ax2 = fig.add_subplot(2, 1, 2)
    ax2.plot(dd.index, dd.values)
    ax2.set_title("Drawdown")
    ax2.grid(True)

    fig.tight_layout()
    if out_png:
        fig.savefig(out_png, dpi=160, bbox_inches="tight")
    return fig
