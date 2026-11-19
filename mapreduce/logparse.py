"""Parse one line of the NASA web-server log. You do not need to edit this file.

A line looks like this (Common Log Format):

    199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245

``parse_line`` returns a dict, or ``None`` if the line is malformed. Real logs
always contain a few broken lines, so your mapper must handle ``None``.

The first field (the requesting host) is deliberately not returned: the data
providers ask that the logs are used only for general traffic patterns, not to
study who visited the site.
"""

import re

_LINE = re.compile(
    r'^\S+ \S+ \S+ '                                   # host, ident, user
    r'\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) [-+]\d{4}\] '
    r'"(\S+) (\S+)(?: \S+)?" '                         # method, path, protocol
    r'(\d{3}) (\d+|-)$'                                # status, bytes
)


def parse_line(line: str):
    """Return a dict with the fields of one request, or ``None``.

    Keys:
        day     int, day of the month (1-31)
        hour    int, hour of the day (0-23), server time
        method  str, e.g. "GET"
        path    str, e.g. "/images/NASA-logosmall.gif"
        status  int, HTTP status code, e.g. 200 or 404
        bytes   int, size of the reply in bytes ("-" in the log becomes 0)
    """
    m = _LINE.match(line.strip())
    if m is None:
        return None
    day, _mon, _year, hour, _mi, _s, method, path, status, size = m.groups()
    return {
        "day": int(day),
        "hour": int(hour),
        "method": method,
        "path": path,
        "status": int(status),
        "bytes": 0 if size == "-" else int(size),
    }
