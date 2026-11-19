"""Solution, Task 3: distinct pages per hour, without side effects.

Try the task yourself first. See solutions/README.md.
"""

from mapreduce import parse_line

SORT_BY = "key"


def mapper(line):
    # No global set: the mapper looks at this one line only, so the answer
    # cannot depend on which lines share a process or how often it runs.
    request = parse_line(line)
    if request is not None and request["path"].lower().endswith(".html"):
        yield request["hour"], request["path"]


def reducer(key, values):
    # The reducer sees ALL pages requested in this hour, from every
    # partition, so this is the place to count each page only once.
    return len(set(values))
