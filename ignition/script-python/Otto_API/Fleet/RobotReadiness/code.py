from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getMissionLastUpdateSuccessPath
from Otto_API.Common.TagHelpers import getMissionLastUpdateTsPath
from Otto_API.Common.TagHelpers import getMissionMinChargePath
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import writeObservedTagValues
from Otto_API.Common.TagHelpers import writeTagValues
from Otto_API.Common.TagHelpers import writeTagValue


DEFAULT_ALLOWED_ACTIVITY_STATES = set([
    "PARKING",
    "IDLE",
    "WAITING"
])


def _log():
    return system.util.getLogger("Otto_API.Fleet.RobotReadiness")


def _robotStatusReadPaths(robotPath):
    """Return the readiness-related tag paths read for one robot."""
    return [
        robotPath + "/SystemState",
        robotPath + "/ActivityState",
        robotPath + "/ChargeLevel",
        robotPath + "/ActiveMissionCount",
        robotPath + "/FailedMissionCount",
    ]


def _normalizeStateValue(value):
    if value is None:
        return None
    return str(value).strip().upper()


def _buildRobotReadinessResult(
    robotName,
    available,
    reason,
    systemState=None,
    activityState=None,
    chargeLevel=None,
    minCharge=None,
    activeMissionCount=None,
    failedMissionCount=None,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None
):
    return {
        "robot_name": robotName,
        "available": available,
        "reason": reason,
        "system_state": systemState,
        "activity_state": activityState,
        "charge_level": chargeLevel,
        "min_charge": minCharge,
        "active_mission_count": activeMissionCount,
        "failed_mission_count": failedMissionCount,
        "mission_last_update_ts": missionLastUpdateTs,
        "mission_last_update_success": missionLastUpdateSuccess,
    }


def evaluateRobotReadiness(
    robotName,
    systemState,
    activityState,
    chargeLevel,
    minCharge,
    activeMissionCount=None,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None,
    failedMissionCount=None,
    allowedActivityStates=None
):
    """
    Evaluates a single robot snapshot from explicit inputs and returns
    a structured result describing mission readiness.
    """
    if allowedActivityStates is None:
        allowedActivityStates = DEFAULT_ALLOWED_ACTIVITY_STATES

    normalizedSystemState = _normalizeStateValue(systemState)
    normalizedActivityState = _normalizeStateValue(activityState)
    normalizedAllowedStates = set([
        _normalizeStateValue(value) for value in allowedActivityStates
        if value is not None
    ])

    if minCharge is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "min_charge_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if normalizedSystemState is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "system_state_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if normalizedActivityState is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "activity_state_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if chargeLevel is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "charge_level_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if not missionLastUpdateSuccess:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "mission_data_not_successful",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if missionLastUpdateTs is None or not str(missionLastUpdateTs).strip():
        return _buildRobotReadinessResult(
            robotName,
            False,
            "mission_data_missing_timestamp",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if normalizedSystemState != "RUN":
        return _buildRobotReadinessResult(
            robotName,
            False,
            "system_state_not_run",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if normalizedActivityState not in normalizedAllowedStates:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "activity_state_not_allowed",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if chargeLevel < minCharge:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "charge_below_minimum",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if activeMissionCount is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "active_mission_count_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if activeMissionCount != 0:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "active_missions_present",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if failedMissionCount is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "failed_mission_count_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    if failedMissionCount != 0:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "failed_missions_present",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge,
            activeMissionCount,
            failedMissionCount,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )

    return _buildRobotReadinessResult(
        robotName,
        True,
        "available",
        normalizedSystemState,
        normalizedActivityState,
        chargeLevel,
        minCharge,
        activeMissionCount,
        failedMissionCount,
        missionLastUpdateTs,
        missionLastUpdateSuccess
    )


def isRobotAvailable(
    systemState,
    activityState,
    chargeLevel,
    minCharge,
    activeMissionCount=None,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None,
    failedMissionCount=None,
    allowedActivityStates=None
):
    """
    Convenience helper for callers that only need the final availability flag.
    """
    return evaluateRobotReadiness(
        None,
        systemState,
        activityState,
        chargeLevel,
        minCharge,
        activeMissionCount,
        missionLastUpdateTs,
        missionLastUpdateSuccess,
        failedMissionCount,
        allowedActivityStates
    )["available"]


def _buildUpdateResult(ok, level, message, minCharge=None, robotResults=None):
    robotResults = list(robotResults or [])
    robotsAvailable = len([
        result for result in robotResults if result.get("available")
    ])
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "min_charge": minCharge,
            "robots_evaluated": len(robotResults),
            "robots_available": robotsAvailable,
            "robot_results": robotResults,
        },
        min_charge=minCharge,
        robots_evaluated=len(robotResults),
        robots_available=robotsAvailable,
        robot_results=robotResults,
    )


def _reasonToTagValue(readiness):
    if readiness.get("available"):
        return ""
    return str(readiness.get("reason") or "")


def buildReadinessResultsAndWrites(
    robotSnapshots,
    minCharge,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None
):
    """
    Evaluate readiness for explicit robot snapshots and build the tag writes.
    Each snapshot should include:
    - robot_name
    - robot_path
    - system_state
    - activity_state
    - charge_level
    - active_mission_count
    - failed_mission_count
    """
    robotResults = []
    writePaths = []
    writeValues = []

    for snapshot in list(robotSnapshots or []):
        readiness = evaluateRobotReadiness(
            snapshot.get("robot_name"),
            snapshot.get("system_state"),
            snapshot.get("activity_state"),
            snapshot.get("charge_level"),
            minCharge,
            snapshot.get("active_mission_count"),
            missionLastUpdateTs,
            missionLastUpdateSuccess,
            snapshot.get("failed_mission_count"),
        )
        robotResults.append(readiness)

        robotPath = snapshot.get("robot_path")
        if robotPath:
            writePaths.extend([
                robotPath + "/AvailableForWork",
                robotPath + "/NotReadyReason",
            ])
            writeValues.extend([
                readiness["available"],
                _reasonToTagValue(readiness),
            ])

    return {
        "robot_results": robotResults,
        "write_paths": writePaths,
        "write_values": writeValues,
    }


def updateAvailableForWork():
    """
    Evaluates SystemState, ActivityState, and ChargeLevel for each robot
    and sets /AvailableForWork based on mission eligibility rules.

    This is kept as a standalone manual/fallback/debugging entrypoint.
    At this time it is not the intended main runtime path; the normal
    runtime flow should prefer Otto_API.Fleet.Get.updateRobotOperationalState()
    so OTTO sync and readiness evaluation happen in one pass.
    """
    ottoLogger = _log()

    robotsBasePath = getFleetRobotsPath()
    minChargePath = getMissionMinChargePath()
    missionLastUpdateTsPath = getMissionLastUpdateTsPath()
    missionLastUpdateSuccessPath = getMissionLastUpdateSuccessPath()

    try:
        try:
            minCharge = readRequiredTagValue(
                minChargePath,
                "Minimum charge threshold"
            )
            missionLastUpdateTs = readOptionalTagValue(
                missionLastUpdateTsPath,
                None,
                allowEmptyString=False
            )
            missionLastUpdateSuccess = bool(readOptionalTagValue(
                missionLastUpdateSuccessPath,
                False
            ))
        except ValueError as e:
            message = str(e)
            ottoLogger.warn(message)
            return _buildUpdateResult(False, "warn", message)

        browseResults = browseTagResults(robotsBasePath)
        robotRows = []
        readPaths = []

        for tag in browseResults:
            if str(tag["tagType"]) != "UdtInstance":
                continue

            robotName = str(tag["name"])
            robotPath = robotsBasePath + "/" + robotName
            robotRows.append({
                "robot_name": robotName,
                "robot_path": robotPath,
            })
            readPaths.extend(_robotStatusReadPaths(robotPath))

        readResults = []
        if readPaths:
            readResults = readTagValues(readPaths)

        expectedReadCount = len(robotRows) * 5
        if len(readResults) < expectedReadCount:
            message = (
                "AvailableForWork evaluation failed - expected {} readiness tag values but received {}"
            ).format(expectedReadCount, len(readResults))
            ottoLogger.warn(message)
            return _buildUpdateResult(False, "warn", message, minCharge)

        robotSnapshots = []
        for index, robotRow in enumerate(robotRows):
            offset = index * 5
            robotSnapshots.append({
                "robot_name": robotRow["robot_name"],
                "robot_path": robotRow["robot_path"],
                "system_state": readResults[offset].value if readResults[offset].quality.isGood() else None,
                "activity_state": readResults[offset + 1].value if readResults[offset + 1].quality.isGood() else None,
                "charge_level": readResults[offset + 2].value if readResults[offset + 2].quality.isGood() else None,
                "active_mission_count": (
                    readResults[offset + 3].value
                    if readResults[offset + 3].quality.isGood()
                    else None
                ),
                "failed_mission_count": (
                    readResults[offset + 4].value
                    if readResults[offset + 4].quality.isGood()
                    else None
                ),
            })

        readinessBatch = buildReadinessResultsAndWrites(
            robotSnapshots,
            minCharge,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )
        robotResults = readinessBatch["robot_results"]
        writePaths = readinessBatch["write_paths"]
        writeValues = readinessBatch["write_values"]

        if writePaths:
            writeObservedTagValues(
                writePaths,
                writeValues,
                labels=["Otto_API.RobotReadiness write"] * len(writePaths),
                logger=ottoLogger
            )

        message = "AvailableForWork updated for {} robot(s)".format(
            len(robotResults)
        )
        return _buildUpdateResult(True, "info", message, minCharge, robotResults)

    except Exception as e:
        message = "AvailableForWork evaluation failed - " + str(e)
        ottoLogger.error(message)
        return _buildUpdateResult(False, "error", message)
