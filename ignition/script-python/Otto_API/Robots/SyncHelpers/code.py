from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import readTagValues

from Otto_API.Robots.Records import RobotSystemStateEntry


def selectDominantSystemState(entries, logger=None):
    """
    Select the dominant OTTO system-state entry.
    """
    bestEntry = None
    bestPriority = None
    bestCreated = None

    for entry in list(entries or []):
        if not isinstance(entry, RobotSystemStateEntry):
            entry = RobotSystemStateEntry.fromDict(entry)
        priority = entry.priority
        createdValue = entry.createdSortValue(logger=logger)

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
        robotPath + "/PlaceId",
        robotPath + "/PlaceName",
        robotPath + "/ContainerPresent",
        robotPath + "/ContainerId",
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
        None,
        None,
        False,
        "",
        False,
        "invalid_robot_id",
    ]
    return list(zip(paths, values))


def buildRobotMetricWrites(robotIdToPath, metricRecords, robotKey, valueKey, targetSuffix):
    """
    Match raw OTTO robot metric records to robot tag paths and build tag writes.
    """
    writes = []
    unmatchedRobotIds = []

    for record in list(metricRecords or []):
        record = dict(record or {})
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

    Accepts typed RobotSystemStateEntry objects or raw OTTO payload dicts.
    """
    grouped = {}
    for record in list(records or []):
        if isinstance(record, RobotSystemStateEntry):
            robotId = record.robot
        else:
            robotId = dict(record or {}).get(robotKey)
        if robotId is None:
            continue
        robotId = str(robotId).strip()
        grouped.setdefault(robotId, []).append(record)
    return grouped
