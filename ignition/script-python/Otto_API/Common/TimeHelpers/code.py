import calendar
import re


def parseIsoTimestampToEpochMillis(timestampText):
    """
    Parse a basic ISO-8601 timestamp into epoch milliseconds without Java APIs.
    """
    raw = str(timestampText).strip()
    match = re.match(
        (
            r"^(\d{4})-(\d{2})-(\d{2})"
            r"T(\d{2}):(\d{2}):(\d{2})"
            r"(?:\.(\d{1,6}))?"
            r"(Z|[+-]\d{2}:\d{2})$"
        ),
        raw
    )
    if not match:
        raise ValueError("Unsupported ISO timestamp: {}".format(raw))

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = int(match.group(6))
    fractional = match.group(7) or ""
    timezonePart = match.group(8)

    milliseconds = 0
    if fractional:
        milliseconds = int((fractional + "000")[:3])

    utcSeconds = calendar.timegm((
        year,
        month,
        day,
        hour,
        minute,
        second,
    ))

    if timezonePart != "Z":
        sign = 1 if timezonePart[0] == "+" else -1
        offsetHours = int(timezonePart[1:3])
        offsetMinutes = int(timezonePart[4:6])
        offsetSeconds = sign * ((offsetHours * 60 * 60) + (offsetMinutes * 60))
        utcSeconds -= offsetSeconds

    return (utcSeconds * 1000) + milliseconds
