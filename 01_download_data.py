"""
Step 1: Download real MBTA LAMP data.

Fact source:  subway-on-time-performance-v1 (one Parquet file per service date)
Dimensions:   LAMP static routes + stops (Parquet)
Source docs:  https://performancedata.mbta.com/

Usage:  python 01_download_data.py [N_DAYS]   (default 7 most recent service dates)
"""
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE = "https://performancedata.mbta.com/lamp"
OTP_DIR = f"{BASE}/subway-on-time-performance-v1"
STATIC_FILES = ["LAMP_static_routes.parquet", "LAMP_static_stops.parquet"]
N_DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7

RAW = Path("data/raw")
(RAW / "otp").mkdir(parents=True, exist_ok=True)
(RAW / "static").mkdir(parents=True, exist_ok=True)


def fetch(url: str, dest: Path) -> bool:
    if dest.exists():
        print(f"  skip (exists) {dest}")
        return True
    try:
        urllib.request.urlretrieve(url, dest)
        print(f"  ok   {dest}  ({dest.stat().st_size / 1e6:.1f} MB)")
        return True
    except urllib.error.HTTPError as e:
        print(f"  miss {url}  (HTTP {e.code})")
        return False


# 1. Index of all published service dates
index_text = urllib.request.urlopen(f"{OTP_DIR}/index.csv").read().decode()
(RAW / "otp_index.csv").write_text(index_text)
dates = sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}", index_text)), reverse=True)
print(f"index.csv lists {len(dates)} distinct dates ({dates[-1]} -> {dates[0]})")

# 2. Most recent N service dates that actually download
got = []
for d in dates:
    if len(got) == N_DAYS:
        break
    url = f"{OTP_DIR}/{d}-subway-on-time-performance-v1.parquet"
    if fetch(url, RAW / "otp" / f"{d}.parquet"):
        got.append(d)
print(f"\nDownloaded {len(got)} service days: {sorted(got)}")

# 3. Static dimension tables
for f in STATIC_FILES:
    fetch(f"{BASE}/tableau/rail/{f}", RAW / "static" / f)
