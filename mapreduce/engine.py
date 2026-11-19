"""A tiny MapReduce engine that runs on one computer.

You do not need to edit this file. It does what Hadoop or Spark would do for
you on a cluster, on a much smaller scale:

1. **Split** the input into partitions (on a cluster: blocks stored on
   different machines).
2. **Map** every partition, line by line. With ``workers > 1``, partitions are
   mapped in parallel in separate processes (on a cluster: separate machines).
3. **Combine** (optional): if the job defines ``combiner``, it runs on each
   partition's output before the shuffle, to shrink what has to be sent.
4. **Shuffle**: group all intermediate (key, value) pairs by key.
5. **Reduce** every key. With ``workers > 1``, keys are reduced in parallel.

A job is a Python module in ``jobs/`` that defines two functions:

    def mapper(line):           # one input line -> zero or more (key, value)
        yield key, value

    def reducer(key, values):   # one key and ALL its values -> one result
        return result

and, optionally, ``combiner(key, values)`` with the same shape as ``reducer``.
"""

from __future__ import annotations

import importlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
import multiprocessing
from pathlib import Path


# Always start worker processes fresh ("spawn"), as on macOS and Windows.
# Linux would otherwise copy the parent process ("fork"), including any
# leftover module state, so buggy jobs would behave differently per OS.
_ctx = multiprocessing.get_context("spawn")


@dataclass
class JobStats:
    lines: int = 0          # input lines read
    partitions: int = 0     # number of input partitions
    map_pairs: int = 0      # (key, value) pairs produced by all mappers
    shuffled_pairs: int = 0 # pairs sent through the shuffle (after combiner)
    keys: int = 0           # distinct keys after the shuffle
    seconds: float = 0.0    # wall-clock time
    timings: dict = field(default_factory=dict)


def split(lines: list[str], partitions: int) -> list[list[str]]:
    """Split the input into ``partitions`` contiguous chunks of similar size."""
    partitions = max(1, min(partitions, len(lines) or 1))
    size, extra = divmod(len(lines), partitions)
    chunks, start = [], 0
    for i in range(partitions):
        end = start + size + (1 if i < extra else 0)
        chunks.append(lines[start:end])
        start = end
    return chunks


def _load(job_name: str):
    return importlib.import_module(job_name)


def _map_partition(args):
    """Run the job's mapper (and combiner) over one partition."""
    job_name, chunk = args
    job = _load(job_name)
    pairs = []
    for line in chunk:
        out = job.mapper(line)
        if out is None:
            continue
        for kv in out:
            pairs.append(kv)
    produced = len(pairs)
    combiner = getattr(job, "combiner", None)
    if combiner is not None:
        groups = defaultdict(list)
        for k, v in pairs:
            groups[k].append(v)
        pairs = [(k, combiner(k, vs)) for k, vs in groups.items()]
    return produced, pairs


def _reduce_key(args):
    job_name, key, values = args
    return key, _load(job_name).reducer(key, values)


def run_job(job_name: str, input_path, partitions: int = 4, workers: int = 1):
    """Run a job and return ``(results, stats)``.

    ``job_name`` is an importable module name such as ``"jobs.views_per_image"``.
    ``results`` is a dict from key to the reducer's result.
    """
    t0 = time.perf_counter()
    stats = JobStats()
    # Split on newlines only: str.splitlines() would also split on rare
    # control characters that appear inside a few real log lines.
    with open(input_path, encoding="latin-1", newline="") as f:
        lines = [line.rstrip("\r\n") for line in f]
    stats.lines = len(lines)

    chunks = split(lines, partitions)
    stats.partitions = len(chunks)

    # --- map (+ combine) --------------------------------------------------
    t = time.perf_counter()
    tasks = [(job_name, c) for c in chunks]
    if workers > 1:
        with _ctx.Pool(workers) as pool:
            mapped = pool.map(_map_partition, tasks)
    else:
        mapped = [_map_partition(task) for task in tasks]
    stats.timings["map"] = time.perf_counter() - t

    # --- shuffle: group by key ---------------------------------------------
    t = time.perf_counter()
    groups = defaultdict(list)
    for produced, pairs in mapped:
        stats.map_pairs += produced
        stats.shuffled_pairs += len(pairs)
        for k, v in pairs:
            groups[k].append(v)
    stats.keys = len(groups)
    stats.timings["shuffle"] = time.perf_counter() - t

    # --- reduce ------------------------------------------------------------
    t = time.perf_counter()
    rtasks = [(job_name, k, vs) for k, vs in groups.items()]
    if workers > 1:
        with _ctx.Pool(workers) as pool:
            reduced = pool.map(_reduce_key, rtasks, chunksize=256)
    else:
        reduced = [_reduce_key(task) for task in rtasks]
    stats.timings["reduce"] = time.perf_counter() - t

    stats.seconds = time.perf_counter() - t0
    return dict(reduced), stats


def default_input() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "nasa_1995-07-01.log"
