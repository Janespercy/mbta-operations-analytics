"""Download MBTA LAMP subway performance files and static route/stop tables.

Data source: https://performancedata.mbta.com/
"""
import argparse
import re
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

LAMP_URL = "https://performancedata.mbta.com/lamp"
OTP_URL = f"{LAMP_URL}/subway-on-time-performance-v1"
STATIC_TABLES = ["LAMP_static_routes.parquet", "LAMP_static_stops.parquet"]


def download(url, dest):
    if dest.exists():
        print(f"already have {dest.name}")
        return True
    try:
        urllib.request.urlretrieve(url, dest)
    except urllib.error.HTTPError as err:
        print(f"failed {url} ({err.code})")
        return False
    print(f"saved {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    return True


def main(n_days, out_dir):
    otp_dir = out_dir / "otp"
    static_dir = out_dir / "static"
    otp_dir.mkdir(parents=True, exist_ok=True)
    static_dir.mkdir(parents=True, exist_ok=True)

    index = urllib.request.urlopen(f"{OTP_URL}/index.csv").read().decode()
    (out_dir / "otp_index.csv").write_text(index)

    # grab dates from the raw text so this doesn't depend on the index's column names
    dates = sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}", index)), reverse=True)
    # today's file is a partial service day, so only take completed days
    dates = [d for d in dates if d < date.today().isoformat()]
    print(f"{len(dates)} dates in index, latest is {dates[0]}")

    downloaded = []
    for d in dates:
        if len(downloaded) == n_days:
            break
        # skip any date whose file isn't there and keep going back
        url = f"{OTP_URL}/{d}-subway-on-time-performance-v1.parquet"
        if download(url, otp_dir / f"{d}.parquet"):
            downloaded.append(d)

    if downloaded:
        print(f"got {len(downloaded)} days: {min(downloaded)} to {max(downloaded)}")
    else:
        print("no daily files downloaded, check the URLs")

    for table in STATIC_TABLES:
        download(f"{LAMP_URL}/tableau/rail/{table}", static_dir / table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--out", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    main(args.days, args.out)