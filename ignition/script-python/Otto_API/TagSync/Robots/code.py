from Otto_API.Common.RuntimeHistory import appendRobotStateHistoryRow
from Otto_API.Common.RuntimeHistory import buildRobotStateLogSignature
from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import writeObservedTagValues
from Otto_API.Models.Results import OperationalResult


ROBOT_STATE_LOG_SIGNATURE_MEMBER = "LastRobotStateLogSignature"
_MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS = set()


def buildRobotTagValues(basePath, robotRecord):
    """
    Build the tag value map for a robot record.
    """
    instanceName = robotRecord.get("name")
    if not instanceName:
        return None, {}

    instancePath = basePath + "/" + instanceName
    return instanceName, {
        instancePath + "/Hostname": robotRecord.get("hostname"),
        instancePath + "/ID": robotRecord.get("id"),
        instancePath + "/SerialNum": robotRecord.get("serial_number"),
    }


def buildRobotSyncResult(ok, level, message, records=None, writes=None, data=None, issues=None):
    records = list(records or [])
    writes = list(writes or [])
    issues = list(issues or [])
    payload = OperationalResult(
        ok,
        level,
        message,
        dataFields={
            "records": records,
            "writes": writes,
            "value": data,
            "issues": issues,
        },
    ).toDict()
    payload["records"] = records
    payload["writes"] = writes
    payload["issues"] = issues
    return payload


def writeObservedPairs(writes, label, logger):
    writePairs = list(writes or [])
    if not writePairs:
        return
    writeObservedTagValues(
        [path for path, _ in writePairs],
        [value for _, value in writePairs],
        labels=[label] * len(writePairs),
        logger=logger
    )


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
    Read robot inventory metadata from the local robot tag tree.
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


def readRobotInventory(robotsBasePath):
    inventory = readRobotInventoryMetadata(robotsBasePath)
    return (
        inventory["browse_results"],
        inventory["robot_path_by_id"],
        inventory["invalid_robot_rows"],
        inventory["read_plan"],
    )


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


def collectInvalidRobotWrites(invalidRobotRows, logger):
    invalidated = []
    for invalidRow in list(invalidRobotRows or []):
        robotPath = invalidRow["robot_path"]
        reason = invalidRow["reason"]
        logger.warn("Invalid robot ID for {} - {}".format(robotPath, reason))
        try:
            invalidated.extend(list(buildInvalidRobotSyncWrites(robotPath)))
        except Exception as exc:
            logger.warn(
                "Failed to invalidate sync state for {} - {}".format(
                    robotPath,
                    str(exc)
                )
            )
    return invalidated


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


def warnMissingRobotStateLogSignaturePath(logger, tagPath):
    tagPath = str(tagPath or "")
    if not tagPath or tagPath in _MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS:
        return
    _MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS.add(tagPath)
    logger.warn(
        "Robot state history dedupe is disabled until [{}] is added to api_Robot".format(
            tagPath
        )
    )


def buildRobotStateHistoryUpdate(
    robotName,
    previousSystemState,
    effectiveSystemState,
    previousSubSystemState,
    effectiveSubSystemState,
    previousActivityState,
    effectiveActivity,
    previousRobotStateLogSignature,
    robotStateLogSignaturePath,
    hasSignatureTag,
    logger,
):
    if not (
        previousSystemState != effectiveSystemState or
        previousSubSystemState != effectiveSubSystemState or
        previousActivityState != effectiveActivity
    ):
        return None

    if not hasSignatureTag:
        warnMissingRobotStateLogSignaturePath(logger, robotStateLogSignaturePath)
        return None

    signature = buildRobotStateLogSignature(
        robotName,
        previousSystemState,
        effectiveSystemState,
        previousSubSystemState,
        effectiveSubSystemState,
        previousActivityState,
        effectiveActivity
    )
    if previousRobotStateLogSignature == signature:
        return None

    return {
        "pending_row": {
            "robot_name": robotName,
            "old_system_state": previousSystemState,
            "new_system_state": effectiveSystemState,
            "old_sub_system_state": previousSubSystemState,
            "new_sub_system_state": effectiveSubSystemState,
            "old_activity_state": previousActivityState,
            "new_activity_state": effectiveActivity,
        },
        "signature_write": (robotStateLogSignaturePath, signature),
    }


def appendPendingRobotStateHistoryRows(nowTimestamp, pendingRows):
    for row in list(pendingRows or []):
        appendRobotStateHistoryRow(
            nowTimestamp,
            row["robot_name"],
            row["old_system_state"],
            row["new_system_state"],
            row["old_sub_system_state"],
            row["new_sub_system_state"],
            row["old_activity_state"],
            row["new_activity_state"],
        )
