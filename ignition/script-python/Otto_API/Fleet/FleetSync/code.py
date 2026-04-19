import calendar
import json
import re

from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import writeTagValues


def parseServerStatus(responseText):
    """
    Parse a Fleet Manager server-state response and return a status string.
    """
    if not responseText:
        raise ValueError("Empty server status response")

    payload = json.loads(responseText)
    return payload.get("state", "Unknown")


def _normalizeMissionStatusList(missionStatus):
    if missionStatus is None:
        return []

    if isinstance(missionStatus, (list, tuple)):
        values = missionStatus
    else:
        values = [missionStatus]

    normalized = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized.append(text)
    return normalized


def buildMissionsUrl(baseUrl, missionStatus, limit=None, offset=None):
    """
    Build the OTTO missions URL for one or more mission status filters.
    """
    statuses = _normalizeMissionStatusList(missionStatus)
    if not statuses:
        raise ValueError("At least one mission status is required")

    url = baseUrl + "/missions/?fields=%2A"
    if offset is not None:
        url += "&offset=" + str(offset)

    for status in statuses:
        url += "&mission_status=" + status

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


def buildRobotIdReadPlan(robotRows, basePath):
    """
    Build a read plan for robot ID tags from browsed robot UDT rows.
    """
    plan = []
    for row in list(robotRows or []):
        if str(row.get("tagType")) != "UdtInstance":
            continue

        robotName = str(row.get("name"))
        robotPath = basePath + "/" + robotName
        plan.append({
            "robot_name": robotName,
            "robot_path": robotPath,
            "id_path": robotPath + "/ID",
        })

    return plan


def buildRobotIdToPathMapFromReads(readPlan, qualifiedValues):
    """
    Build a robot UUID -> tag path mapping from a bulk ID read.
    """
    robotIdToPath = {}
    invalidRobotRows = []

    for planRow, qualifiedValue in zip(list(readPlan or []), list(qualifiedValues or [])):
        quality = getattr(qualifiedValue, "quality", None)
        if quality is None or not quality.isGood():
            invalidRobotRows.append({
                "robot_name": planRow["robot_name"],
                "robot_path": planRow["robot_path"],
                "reason": "Robot ID tag is not readable",
            })
            continue

        robotId = qualifiedValue.value
        if robotId is None or not str(robotId).strip():
            invalidRobotRows.append({
                "robot_name": planRow["robot_name"],
                "robot_path": planRow["robot_path"],
                "reason": "Robot ID returned no value",
            })
            continue

        robotIdToPath[str(robotId).strip()] = planRow["robot_path"]

    return robotIdToPath, invalidRobotRows


def readRobotIdToPathMap(robotRows, basePath):
    """
    Browse robot UDT rows, bulk-read IDs, and return UUID -> tag path mappings.
    """
    readPlan = buildRobotIdReadPlan(robotRows, basePath)
    if not readPlan:
        return {}, [], []

    qualifiedValues = readTagValues([
        row["id_path"] for row in readPlan
    ])
    robotIdToPath, invalidRobotRows = buildRobotIdToPathMapFromReads(
        readPlan,
        qualifiedValues
    )
    return robotIdToPath, invalidRobotRows, readPlan


def readRobotInventoryMetadata(basePath):
    """
    Read shared robot inventory metadata from the local robot tag tree.
    Returns browse rows, ID/path mappings, invalid rows, and name lookups that
    are useful across both fleet sync and mission sorting.
    """
    browseResults = browseTagResults(basePath)
    readPlan = buildRobotIdReadPlan(browseResults, basePath)
    qualifiedValues = readTagValues([
        row["id_path"] for row in readPlan
    ])
    robotIdToPath, invalidRobotRows = buildRobotIdToPathMapFromReads(
        readPlan,
        qualifiedValues
    )

    robotNameByLower = {}
    robotNameById = {}

    for row in list(browseResults or []):
        if str(row.get("tagType")) != "UdtInstance":
            continue
        robotName = str(row.get("name"))
        robotNameByLower[robotName.strip().lower()] = robotName

    for planRow, qualifiedValue in zip(
        list(readPlan or []),
        list(qualifiedValues or [])
    ):
        quality = getattr(qualifiedValue, "quality", None)
        if quality is None or not quality.isGood():
            continue

        robotId = qualifiedValue.value
        if robotId is None:
            continue

        normalizedId = str(robotId).strip().lower()
        if not normalizedId:
            continue

        robotNameById[normalizedId] = planRow["robot_name"]

    return {
        "browse_results": browseResults,
        "robot_path_by_id": robotIdToPath,
        "invalid_robot_rows": invalidRobotRows,
        "read_plan": readPlan,
        "robot_name_by_lower": robotNameByLower,
        "robot_name_by_id": robotNameById,
    }


def invalidateRobotSyncState(robotPath):
    """
    Clear derived sync fields for a robot instance whose ID is invalid.
    """
    writes = buildInvalidRobotSyncWrites(robotPath)
    if writes:
        writeTagValues(
            [path for path, _ in writes],
            [value for _, value in writes]
        )
    return writes


def buildInvalidRobotSyncWrites(robotPath):
    """
    Build derived sync field clears for a robot instance whose ID is invalid.
    """
    paths = [
        robotPath + "/SystemState",
        robotPath + "/SubSystemState",
        robotPath + "/SystemStatePriority",
        robotPath + "/SystemStateUpdatedTs",
        robotPath + "/ActivityState",
        robotPath + "/ChargeLevel",
        robotPath + "/ActiveMissionCount",
        robotPath + "/FailedMissionCount",
        robotPath + "/AvailableForWork",
        robotPath + "/NotReadyReason",
    ]
    values = [
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        0,
        False,
        "invalid_robot_id",
    ]
    return list(zip(paths, values))


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
