"""Stretch goal: run the "views per image" job on Dask.

Dask is a Python library that splits data into partitions and runs tasks on
many processes, on one laptop or on a cluster of machines, like Spark does.

    python3 -m pip install -r requirements-stretch.txt
    python3 stretch/dask_views.py

The script runs the same job three ways and checks that all agree:

  A. the course engine (mapreduce/engine.py) with YOUR Task 1 functions;
  B. Dask, calling YOUR Task 1 mapper and reducer unchanged;
  C. Dask written the "Dask way", which YOU complete in ``views_dask_style``.

While it runs, open the dashboard (link printed below) to watch the tasks.
"""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import dask.bag as db                      # noqa: E402
from dask.distributed import Client, LocalCluster  # noqa: E402

from jobs import views_per_image as job    # noqa: E402
from mapreduce import parse_line           # noqa: E402
from mapreduce.engine import default_input, run_job  # noqa: E402


def views_with_your_functions(lines):
    """B: Dask runs your mapper and reducer, unchanged."""
    pairs = lines.map(lambda line: list(job.mapper(line))).flatten()   # map
    groups = pairs.groupby(lambda kv: kv[0])                           # shuffle
    return groups.map(lambda g: (g[0], job.reducer(g[0], [v for _, v in g[1]])))  # reduce


def views_dask_style(lines):
    """C: the same job using Dask's own building blocks.

    ``lines`` is a Dask bag of log lines. Return a bag of (path, count) pairs.

    Useful bag methods (each runs per partition, in parallel):
      lines.map(f)          apply f to every element
      bag.filter(pred)      keep elements where pred(element) is True
      bag.pluck("path")     take one field from every dict
      bag.frequencies()     count how often each element occurs:
                            a map, a combine AND a reduce in one call
    Hint: parse, drop None, keep image views (same rules as Task 1, including
    upper-case extensions), take the path, count.
    """
    # TODO (stretch): replace the next line
    raise NotImplementedError("write views_dask_style")


def main():
    path = str(default_input())

    t = time.perf_counter()
    expected, _ = run_job("jobs.views_per_image", path, partitions=4, workers=1)
    print(f"A. course engine        {len(expected):>6,} images  {time.perf_counter() - t:5.2f} s")

    with LocalCluster(n_workers=4, threads_per_worker=1) as cluster, Client(cluster) as client:
        print(f"   Dask dashboard: {client.dashboard_link}")
        lines = db.read_text(path, blocksize="1MB", encoding="latin-1").map(str.rstrip)
        print(f"   input split into {lines.npartitions} partitions\n")

        t = time.perf_counter()
        b = dict(views_with_your_functions(lines).compute())
        print(f"B. Dask, your functions {len(b):>6,} images  {time.perf_counter() - t:5.2f} s  "
              f"{'same as A' if b == expected else 'DIFFERENT from A'}")

        try:
            t = time.perf_counter()
            c = dict(views_dask_style(lines).compute())
            print(f"C. Dask style           {len(c):>6,} images  {time.perf_counter() - t:5.2f} s  "
                  f"{'same as A' if c == expected else 'DIFFERENT from A'}")
        except NotImplementedError:
            print("C. Dask style           not written yet: complete views_dask_style()")

        input("\nPress Enter to stop Dask (look at the dashboard first) ... ")

    print("\nTop 5 images:")
    for p, n in sorted(expected.items(), key=lambda kv: -kv[1])[:5]:
        print(f"  {n:>6,}  {p}")


if __name__ == "__main__":
    main()
