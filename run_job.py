"""Run a MapReduce job from the jobs/ folder and print its results.

Examples:
    python3 run_job.py views_per_image
    python3 run_job.py views_per_image --workers 4 --top 5
    python3 run_job.py megabytes_per_hour --input data/sample.log
"""

import argparse
import sys
from pathlib import Path

from mapreduce.engine import default_input, run_job

JOBS = sorted(p.stem for p in (Path(__file__).parent / "jobs").glob("*.py") if p.stem != "__init__")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("job", choices=JOBS, help="name of a file in jobs/ (without .py)")
    ap.add_argument("--input", default=str(default_input()), help="log file to process")
    ap.add_argument("--partitions", type=int, default=4, help="how many pieces to split the input into (default 4)")
    ap.add_argument("--workers", type=int, default=1, help="how many processes run in parallel (default 1)")
    ap.add_argument("--top", type=int, default=10, help="how many results to print (default 10, 0 = all)")
    args = ap.parse_args()

    try:
        results, stats = run_job("jobs." + args.job, args.input, args.partitions, args.workers)
    except NotImplementedError as e:
        sys.exit(f"Not done yet: {e}")

    print(f"input        {stats.lines:>10,} lines in {stats.partitions} partitions, {args.workers} worker(s)")
    print(f"map          {stats.map_pairs:>10,} pairs emitted")
    print(f"shuffle      {stats.shuffled_pairs:>10,} pairs sent, {stats.keys:,} distinct keys")
    print(f"time         {stats.seconds:>10.2f} s "
          f"(map {stats.timings['map']:.2f}, shuffle {stats.timings['shuffle']:.2f}, reduce {stats.timings['reduce']:.2f})")
    print()

    job = __import__("jobs." + args.job, fromlist=["_"])
    if getattr(job, "SORT_BY", "value") == "key":
        rows = sorted(results.items())
    else:
        rows = sorted(results.items(), key=lambda kv: (-kv[1], str(kv[0])))
    if args.top:
        rows = rows[: args.top] if getattr(job, "SORT_BY", "value") == "value" else rows
    width = max([len(str(k)) for k, _ in rows] + [3])
    for k, v in rows:
        print(f"{str(k):<{width}}  {v:>12,}" if isinstance(v, int) else f"{str(k):<{width}}  {v:>12}")


if __name__ == "__main__":
    main()
