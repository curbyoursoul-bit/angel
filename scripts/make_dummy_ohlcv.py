from __future__ import annotations
import os, math, random
from datetime import datetime, timedelta, time as dtime
import pandas as pd

OUT = os.path.join("data", "BANKNIFTY_5m.csv")
os.makedirs("data", exist_ok=True)

# 10 trading days of 5-minute bars, 09:15–15:30 IST
start_date = datetime(2025, 8, 11)  # pick a recent Monday-ish
days = 10
bars = []

def is_weekday(dt):
    return dt.weekday() < 5

def daterange(d0, d1):
    for i in range((d1 - d0).days + 1):
        yield d0 + timedelta(days=i)

spot = 49000.0
rng = random.Random(42)

for day in daterange(start_date, start_date + timedelta(days=days-1)):
    if not is_weekday(day):
        continue
    # trading window
    t = datetime.combine(day.date(), dtime(9, 15))
    end = datetime.combine(day.date(), dtime(15, 30))
    while t <= end:
        # simple random walk
        drift = 0.0
        shock = rng.normalvariate(0, 10)  # ~10 pts std
        spot = max(100.0, spot + drift + shock)
        o = spot + rng.normalvariate(0, 3)
        h = o + abs(rng.normalvariate(0, 12))
        l = o - abs(rng.normalvariate(0, 12))
        c = max(min(o + rng.normalvariate(0, 5), h), l)
        v = max(100, int(abs(rng.normalvariate(1500, 600))))
        # timestamp in IST; backtester can parse any tz
        ts = pd.Timestamp(t, tz="Asia/Kolkata").isoformat()
        bars.append([ts, o, h, l, c, v])
        t += timedelta(minutes=5)

df = pd.DataFrame(bars, columns=["timestamp","open","high","low","close","volume"])
df.to_csv(OUT, index=False)
print(f"Wrote {OUT} with {len(df)} rows")
