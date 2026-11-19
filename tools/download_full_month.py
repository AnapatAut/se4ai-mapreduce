"""Download the full July 1995 NASA log (stretch goal), or cut out one day.

    python3 tools/download_full_month.py              # -> data/full/nasa_1995-07.log
    python3 tools/download_full_month.py --day 01     # also writes data/nasa_1995-07-DD.log

Source: the Internet Traffic Archive, NASA-HTTP traces
https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html ("The traces may be freely
redistributed"). The archive asks that the logs are used only for general
traffic patterns. The file is 20.7 MB compressed (about 200 MB unpacked)
and the server can be slow: the download may take several minutes.

This is also how data/nasa_1995-07-01.log was produced (``--day 01``).
"""

import argparse
import gzip
import shutil
import urllib.request
from pathlib import Path

URL = "https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
ROOT = Path(__file__).resolve().parent.parent
FULL = ROOT / "data" / "full"


def download() -> Path:
    FULL.mkdir(parents=True, exist_ok=True)
    gz = FULL / "NASA_access_log_Jul95.gz"
    if not gz.exists():
        print(f"downloading {URL}")
        print("20.7 MB; the archive's server is slow, so this can take 10 minutes or more.")
        tmp = gz.with_suffix(".part")
        done = 0
        with urllib.request.urlopen(URL, timeout=60) as r, open(tmp, "wb") as f:
            while chunk := r.read(256 * 1024):
                f.write(chunk)
                done += len(chunk)
                print(f"\r  {done / 1e6:5.1f} MB", end="", flush=True)
        print()
        tmp.rename(gz)
    log = FULL / "nasa_1995-07.log"
    if not log.exists():
        print("unpacking ...")
        with gzip.open(gz, "rb") as src, open(log, "wb") as dst:
            shutil.copyfileobj(src, dst)
    print(f"full month: {log}")
    return log


def cut_day(log: Path, day: str) -> Path:
    out = ROOT / "data" / f"nasa_1995-07-{day}.log"
    marker = f"[{day}/Jul/1995:".encode()
    n = 0
    with open(log, "rb") as src, open(out, "wb") as dst:
        for line in src:
            if marker in line:
                dst.write(line)
                n += 1
    print(f"{n:,} lines -> {out}")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", help="two-digit day of July to cut out, e.g. 01")
    args = ap.parse_args()
    log = download()
    if args.day:
        cut_day(log, args.day)
