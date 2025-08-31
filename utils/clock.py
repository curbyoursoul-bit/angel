# utils/clock.py
from __future__ import annotations
import socket
import struct
import time
from typing import Tuple, Optional

# Minimal SNTP. We hit a few servers and take the median offset.
_NTP_SERVERS = [
    "time.google.com",
    "time.cloudflare.com",
    "pool.ntp.org",
]
_NTP_DELTA = 2208988800  # NTP epoch (1900) → Unix epoch (1970)

def _query_ntp(host: str, timeout: float = 2.0) -> Optional[float]:
    """
    Returns server_time - local_time (seconds) or None on failure.
    Positive => local clock is behind. Negative => local clock is ahead.
    """
    try:
        addr = (host, 123)
        packet = b"\x1b" + 47 * b"\0"  # LI=0, VN=3/4, Mode=3 (client)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(timeout)
            t0 = time.time()
            s.sendto(packet, addr)
            data, _ = s.recvfrom(512)
            t3 = time.time()
        if len(data) < 48:
            return None
        secs, frac = struct.unpack("!II", data[40:48])  # transmit timestamp
        server_tx = secs - _NTP_DELTA + (frac / 2**32)
        t_local = (t0 + t3) / 2.0  # crude delay compensation
        return float(server_tx - t_local)
    except Exception:
        return None

def measure_clock_skew(samples: int = 3) -> Tuple[float, int]:
    """Return (median_offset_seconds, successes)."""
    offsets = []
    for host in _NTP_SERVERS:
        off = _query_ntp(host)
        if off is not None:
            offsets.append(off)
        if len(offsets) >= samples:
            break
    if not offsets:
        return 0.0, 0
    offsets.sort()
    mid = offsets[len(offsets)//2]
    return float(mid), len(offsets)

def check_clock_drift(max_skew_seconds: float = 2.0) -> Tuple[bool, float, int]:
    """
    Returns (ok, skew_seconds, successes).
    ok=True if |skew| <= max_skew_seconds or we couldn't measure (successes==0).
    """
    skew, succ = measure_clock_skew()
    if succ == 0:
        return True, 0.0, 0
    return (abs(skew) <= float(max_skew_seconds)), skew, succ
