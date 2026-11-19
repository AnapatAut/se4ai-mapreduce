"""Task 2 tests. Run: python3 -m pytest tests/test_megabytes_per_hour.py

All test lines are real lines from data/sample.log.
"""

from pathlib import Path

import pytest

from jobs import megabytes_per_hour as job
from mapreduce.engine import run_job

SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample.log"
LINES = SAMPLE.read_text(encoding="latin-1").splitlines()


def line(text):
    """The first line of the sample that contains ``text``."""
    return next(l for l in LINES if text in l)


def pairs(l):
    return list(job.mapper(l) or [])


def test_mapper_emits_hour_and_bytes():
    assert pairs(line("00:00:01")) == [(0, 6245)]             # ... 200 6245
    assert pairs(line("mission-sts-71.html")) == [(2, 12040)]  # 02:01:07 ... 200 12040


def test_mapper_counts_every_request_whatever_its_status_or_method():
    assert pairs(line('" 404 -')) == [(0, 0)]                  # 404, "-" bytes
    assert pairs(line('"POST ')) == [(0, 703)]                 # POST


def test_mapper_skips_a_malformed_line_without_crashing():
    assert pairs(line("images.html   HTTP/1.0")) == []


def test_reducer_returns_megabytes_rounded_to_one_decimal():
    assert job.reducer(15, [1_200_000, 2_040_000]) == pytest.approx(3.2)
    assert job.reducer(15, [786]) == pytest.approx(0.0)


@pytest.mark.parametrize("partitions,workers", [(1, 1), (4, 2)])
def test_whole_job_on_the_sample(partitions, workers):
    results, stats = run_job("jobs.megabytes_per_hour", SAMPLE, partitions, workers)
    assert stats.map_pairs == len(LINES) - 1     # every line but the malformed one
    assert results == pytest.approx({0: 0.2, 1: 0.1, 2: 0.0})
