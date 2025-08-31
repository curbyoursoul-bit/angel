# dashboard/web_app.py
from __future__ import annotations
from flask import Flask, jsonify
from pathlib import Path

app = Flask(__name__)
TRADES_CSV = Path("data/trades.csv")

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/stats")
def stats():
    count = 0
    if TRADES_CSV.exists():
        count = max(0, len(TRADES_CSV.read_text(encoding="utf-8", errors="ignore").splitlines()) - 1)
    return jsonify({"trades": count})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8088, debug=True)
