"""Download one month of NYC TLC Yellow Taxi trip data.

Usage:
    python scripts/download.py 2025-01

Saves to data/raw/yellow_tripdata_2025-01.parquet. Skips the download if the file already exists.
"""
from __future__ import annotations

import re
import sys
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def download(month: str, dest_dir: Path = Path("data/raw")) -> Path:
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        raise SystemExit(f"Month must look like YYYY-MM, got {month!r}")

    filename = f"yellow_tripdata_{month}.parquet"
    dest = dest_dir / filename
    if dest.exists():
        print(f"Already have {dest}, skipping.")
        return dest

    dest_dir.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")  # write to a temp name so a failed download never looks complete
    print(f"Downloading {BASE_URL}/{filename} ...")
    urllib.request.urlretrieve(f"{BASE_URL}/{filename}", tmp)
    tmp.rename(dest)
    print(f"Saved {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    download(sys.argv[1])
