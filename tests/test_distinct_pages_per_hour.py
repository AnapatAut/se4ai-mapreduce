"""Task 3 tests. Run: python3 -m pytest tests/test_distinct_pages_per_hour.py

In the sample, these .html pages are requested:
  hour 0: mission-sts-73.html, countdown.html, winvn.html  -> 3 distinct
  hour 1: countdown.html (twice)                            -> 1 distinct
  hour 2: mission-sts-71.html                               -> 1 distinct
"""

from pathlib import Path

import pytest

from mapreduce.engine import run_job

SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample.log"
EXPECTED = {0: 3, 1: 1, 2: 1}


@pytest.mark.parametrize("partitions,workers", [(1, 1), (2, 1), (4, 1), (2, 2), (4, 4)])
def test_same_correct_answer_however_the_work_is_split(partitions, workers):
    results, _ = run_job("jobs.distinct_pages_per_hour", SAMPLE, partitions, workers)
    assert results == EXPECTED


def test_same_answer_when_run_twice():
    first, _ = run_job("jobs.distinct_pages_per_hour", SAMPLE, 2, 1)
    second, _ = run_job("jobs.distinct_pages_per_hour", SAMPLE, 2, 1)
    assert first == second == EXPECTED
