"""Solution, Task 1: views per image.

Try the task yourself first. See solutions/README.md.
"""

from mapreduce import parse_line

IMAGE_EXTENSIONS = (".gif", ".jpg", ".jpeg")


def mapper(line):
    request = parse_line(line)
    # A malformed line gives None: skip it instead of crashing.
    if request is None:
        return
    # A view is a successful GET (200). 304 means the browser used its cache.
    # .lower() also catches upper-case names such as 70HC314.GIF.
    if (request["method"] == "GET" and request["status"] == 200
            and request["path"].lower().endswith(IMAGE_EXTENSIONS)):
        yield request["path"], 1          # one view of this image


def reducer(key, values):
    # values holds a 1 for every view of this image, from every partition.
    return sum(values)
