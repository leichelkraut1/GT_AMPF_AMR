import calendar
import json
import re

from Otto_API.TagHelpers import writeTagValues


def parseServerStatus(responseText):
    """
    Parse a Fleet Manager server-state response and return a status string.
    """
    if not responseText:
        raise ValueError("Empty server status response")

    payload = json.loads(responseText)
    return payload.get("state", "Unknown")


def buildMissionsUrl(baseUrl, missionStatus, limit=None):
    """
    Build the OTTO missions URL for a specific mission status filter.
    """
    url = (
        baseUrl
        + "/missions/?fields=%2A"
        + "&mission_status=" + str(missionStatus)
    )
    if limit is not None:
        url += "&limit=" + str(limit)
    return url


def parseMissionResults(responseText):
    """
    Parse a missions response and return the list payload.
    """
    if not responseText:
        return []

    payload = json.loads(responseText)
    results = payload.get("results", [])
    if not isinstance(results, list):
        return []
    return results


def parseJsonResponse(responseText):
    """
    Parse a JSON response body and return the decoded payload.
    """
    if not responseText:
        raise ValueError("Empty JSON response")
    return json.loads(responseText)


def parseListPayload(responseText):
    """
    Parse a JSON response that may be either a list or a dict with results.
    """
    payload = parseJsonResponse(responseText)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        results = payload.get("results", [])
        if isinstance(results, list):
            return results
    return []


def _createdSortValue(created):
    if created is None:
        return 0

    try:
        return parseIsoTimestampToEpochMillis(created)
    except Exception:
        digits = "".join([
            ch for ch in str(created)
            if ch.isdigit()
        ])
        if not digits:
            return 0
        try:
            return int(digits)
        except Exception:
            return 0


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


def selectDominantSystemState(entries):
    """
    Select the dominant OTTO system-state entry.
    """
    bestEntry = None
    bestPriority = None
    bestCreated = None

    for entry in list(entries or []):
        priority = entry.get("priority", 9999)
        try:
            priority = int(priority)
        except Exception:
            priority = 9999

        createdValue = _createdSortValue(entry.get("created"))

        if bestEntry is None:
            bestEntry = entry
            bestPriority = priority
            bestCreated = createdValue
            continue

        if priority < bestPriority:
            bestEntry = entry
            bestPriority = priority
            bestCreated = createdValue
            continue

        if priority == bestPriority and createdValue > bestCreated:
            bestEntry = entry
            bestPriority = priority
            bestCreated = createdValue

    return bestEntry


def buildRobotIdToPathMap(robotRows, basePath, readTagValue):
    """
    Build a robot UUID -> robot tag path mapping from browsed UDT rows.
    """
    robotIdToPath = {}
    invalidRobotRows = []

    for row in list(robotRows or []):
        if str(row.get("tagType")) != "UdtInstance":
            continue

        robotName = str(row.get("name"))
        robotPath = basePath + "/" + robotName
        try:
            robotId = readTagValue(robotPath + "/ID")
        except Exception as exc:
            invalidRobotRows.append({
                "robot_name": robotName,
                "robot_path": robotPath,
                "reason": str(exc),
            })
            continue

        if robotId is None or not str(robotId).strip():
            invalidRobotRows.append({
                "robot_name": robotName,
                "robot_path": robotPath,
                "reason": "Robot ID returned no value",
            })
            continue

        robotIdToPath[str(robotId).strip()] = robotPath

    return robotIdToPath, invalidRobotRows


def invalidateRobotSyncState(robotPath):
    """
    Clear derived sync fields for a robot instance whose ID is invalid.
    """
    paths = [
        robotPath + "/SystemState",
        robotPath + "/SubSystemState",
        robotPath + "/SystemStatePriority",
        robotPath + "/SystemStateUpdatedTs",
        robotPath + "/ActivityState",
        robotPath + "/ChargeLevel",
        robotPath + "/AvailableForWork",
    ]
    values = [
        None,
        None,
        None,
        None,
        None,
        None,
        False,
    ]
    writeTagValues(paths, values)
    return zip(paths, values)


def buildRobotMetricWrites(robotIdToPath, metricRecords, robotKey, valueKey, targetSuffix):
    """
    Match OTTO robot metric records to robot tag paths and build tag writes.
    """
    writes = []
    unmatchedRobotIds = []

    for record in list(metricRecords or []):
        robotId = record.get(robotKey)
        if robotId is None:
            continue

        robotId = str(robotId).strip()
        if robotId not in robotIdToPath:
            unmatchedRobotIds.append(robotId)
            continue

        writes.append((
            robotIdToPath[robotId] + "/" + targetSuffix,
            record.get(valueKey)
        ))

    return writes, unmatchedRobotIds


def normalizeChargePercentage(rawValue):
    """
    Normalize OTTO battery values to 0-100 percent units.
    """
    if rawValue is None:
        return None

    try:
        numericValue = float(rawValue)
    except Exception:
        return rawValue

    if 0 <= numericValue <= 1:
        return numericValue * 100
    return numericValue


def groupRecordsByRobot(records, robotKey="robot"):
    """
    Group records by robot identifier.
    """
    grouped = {}
    for record in list(records or []):
        robotId = record.get(robotKey)
        if robotId is None:
            continue
        robotId = str(robotId).strip()
        grouped.setdefault(robotId, []).append(record)
    return grouped
