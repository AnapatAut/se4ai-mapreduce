"""Task 1 tests. Run: python3 -m pytest tests/test_views_per_image.py

All test lines are real lines from data/sample.log.
"""

from pathlib import Path

import pytest

from jobs import views_per_image as job
from mapreduce.engine import run_job

SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample.log"
LINES = SAMPLE.read_text(encoding="latin-1").splitlines()


def line(text):
    """The first line of the sample that contains ``text``."""
    return next(l for l in LINES if text in l)


VIEW = line('NASA-logosmall.gif HTTP/1.0" 200')
JPEG = line('livevideo.jpeg HTTP/1.0" 200')
UPPER_CASE = line('70HC314.GIF HTTP/1.0" 200')
CACHED = line('NASA-logosmall.gif HTTP/1.0" 304')
NOT_FOUND = line('pad-abort-test-1-patch-small.gif HTTP/1.0" 404')
PAGE = line('mission-sts-73.html HTTP/1.0" 200')
POST = line('"POST ')
BROKEN = line("images.html   HTTP/1.0")   # extra spaces: parse_line returns None


def pairs(l):
    return list(job.mapper(l) or [])


def test_mapper_emits_one_pair_for_an_image_view():
    assert pairs(VIEW) == [("/images/NASA-logosmall.gif", 1)]


def test_mapper_counts_jpeg_too():
    assert pairs(JPEG) == [("/shuttle/countdown/video/livevideo.jpeg", 1)]


def test_mapper_counts_upper_case_extensions():
    assert pairs(UPPER_CASE) == [("/history/apollo/apollo-13/images/70HC314.GIF", 1)]


@pytest.mark.parametrize("l", [CACHED, NOT_FOUND, PAGE, POST],
                         ids=["304 cached", "404 not found", "html page", "POST"])
def test_mapper_ignores_requests_that_are_not_image_views(l):
    assert pairs(l) == []


def test_mapper_skips_a_malformed_line_without_crashing():
    assert pairs(BROKEN) == []


def test_reducer_adds_up_the_views():
    assert job.reducer("/images/NASA-logosmall.gif", [1, 1, 1]) == 3


EXPECTED = {
    "/images/NASA-logosmall.gif": 3,
    "/shuttle/countdown/count.gif": 2,
    "/images/KSC-logosmall.gif": 2,
    "/shuttle/countdown/video/livevideo.jpeg": 2,
    "/shuttle/countdown/video/livevideo.gif": 1,
    "/history/apollo/apollo-13/images/70HC314.GIF": 1,
}


@pytest.mark.parametrize("partitions,workers", [(1, 1), (3, 1), (4, 2)])
def test_whole_job_on_the_sample(partitions, workers):
    results, _ = run_job("jobs.views_per_image", SAMPLE, partitions, workers)
    assert results == EXPECTED
