"""Solution, Task 2: megabytes per hour.

Try the task yourself first. See solutions/README.md.
"""

from mapreduce import parse_line

SORT_BY = "key"


def mapper(line):
    request = parse_line(line)
    if request is not None:
        # Emit raw bytes. Converting or rounding here would round every
        # request separately and give the wrong total.
        yield request["hour"], request["bytes"]


def reducer(key, values):
    # Add up all bytes for the hour first, then convert to MB once.
    return round(sum(values) / 1_000_000, 1)
