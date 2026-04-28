import json
import time

from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.SyncHelpers import listUdtInstanceNames
from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import deleteTagPath
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import readOptionalTagValues
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.TagPaths import getMissionLastUpdateSuccessPath
from Otto_API.Common.TagPaths import getMissionLastUpdateTsPath
from Otto_API.Common.TagPaths import getMissionMinChargePath
from Otto_API.Common.TagPaths import getRobotChargingDelayMsPath
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Fleet.RobotReadiness import buildReadinessResultsAndWrites
from Otto_API.Robots.Inventory import collectInvalidRobotWrites
from Otto_API.Robots.Inventory import readRobotInventory
from Otto_API.Robots.Normalize import buildRobotTagValues
from Otto_API.Robots.ObservedWrites import buildRobotSyncResult
from Otto_API.Robots.ObservedWrites import writeObservedPairs
from Otto_API.Models.Robots import RobotPlace
from Otto_API.Models.Robots import RobotSnapshot
from Otto_API.Models.Robots import RobotSystemStateEntry
from Otto_API.Robots.StateHistory import appendPendingRobotStateHistoryRows
from Otto_API.Robots.StateHistory import buildRobotStateHistoryUpdate
from Otto_API.Robots.StateHistory import ROBOT_STATE_LOG_SIGNATURE_MEMBER
from Otto_API.Robots.SyncHelpers import groupRecordsByRobot
from Otto_API.Robots.SyncHelpers import normalizeChargePercentage
from Otto_API.Robots.SyncHelpers import selectDominantSystemState


def _log():
    return system.util.getLogger("Otto_API.Robots.Get")


def _fetch_json_results(url, failureMessage, logger):
    issueSuffix = str(url or "").split("/")[-1].split("?")[0] or "unknown"
    response = httpGet(url=url, headerValues=jsonHeaders())
    if not response:
        logger.error("Otto API - {}".format(failureMessage))
        return None, buildRobotSyncResult(
            False,
            "error",
            failureMessage,
            issues=[
                buildRuntimeIssue(
                    "robot_state.http_failed.{}".format(issueSuffix),
                    "Otto_API.Robots.Get",
                    "error",
                    failureMessage,
                )
            ],
        )

    try:
        data = json.loads(response)
    except Exception as exc:
        message = "JSON decode error - {}".format(exc)
        logger.error("Otto API - {}".format(message))
        return None, buildRobotSyncResult(
            False,
            "error",
            message,
            issues=[
                buildRuntimeIssue(
                    "robot_state.json_decode_failed.{}".format(issueSuffix),
                    "Otto_API.Robots.Get",
                    "error",
                    message,
                )
            ],
        )

    return data.get("results", []), None


def _robot_keyed_values(records, robotField, valueField, valueTransform=None):
    valuesByRobot = {}
    for record in list(records or []):
        robotId = record.get(robotField)
        if robotId is None:
            continue
        value = record.get(valueField)
        if valueTransform is not None:
            value = valueTransform(value)
        valuesByRobot[str(robotId).strip()] = value
    return valuesByRobot


def _robot_places(records):
    placesByRobot = {}
    for record in list(records or []):
        robotId = str(record.get("robot") or "").strip()
        if not robotId:
            continue
        placesByRobot[robotId] = RobotPlace.fromDict(record)
    return placesByRobot


def _read_current_robot_values(readPlan):
    robotPaths = [row["robot_path"] for row in list(readPlan or [])]
    currentValuePaths = []
    for robotPath in robotPaths:
        currentValuePaths.extend([
            robotPath + "/SystemState",
            robotPath + "/SubSystemState",
            robotPath + "/SystemStatePriority",
            robotPath + "/SystemStateUpdatedTs",
            robotPath + "/ActivityState",
            robotPath + "/ChargeLevel",
            robotPath + "/ActiveMissionCount",
            robotPath + "/FailedMissionCount",
            robotPath + "/ChargingTOF",
            robotPath + "/Charging_TS",
            robotPath + "/" + ROBOT_STATE_LOG_SIGNATURE_MEMBER,
        ])

    currentValues = {}
    if currentValuePaths:
        readResults = readTagValues(currentValuePaths)
        for path, qualifiedValue in zip(currentValuePaths, readResults):
            currentValues[path] = qualifiedValue.value if qualifiedValue.quality.isGood() else None
    return currentValues


def _chargingStateUpdate(activityState, chargingTof, chargingTs, chargingDelayMs, nowEpochMs):
    normalizedActivity = str(activityState or "").strip().upper()
    currentChargingTof = bool(chargingTof)
    currentChargingTs = int(chargingTs or 0)
    delayMs = max(0, int(chargingDelayMs or 0))
    nowEpochMs = int(nowEpochMs or 0)

    if normalizedActivity == "CHARGING":
        return True, nowEpochMs

    if currentChargingTof and currentChargingTs > 0 and (nowEpochMs - currentChargingTs) < delayMs:
        return True, currentChargingTs

    return False, currentChargingTs


def _epochMillis(dateValue):
    if hasattr(dateValue, "getTime"):
        return int(dateValue.getTime())
    if hasattr(dateValue, "to_datetime"):
        nativeDate = dateValue.to_datetime()
        return int(
            time.mktime(nativeDate.timetuple()) * 1000
            + (nativeDate.microsecond // 1000)
        )
    return int(time.time() * 1000)


def updateRobots():
    """
    Get vehicle information from OTTO and sync Fleet/Robots inventory tags.
    """
    url = getApiBaseUrl() + "/robots/?fields=id,hostname,name,serial_number"
    ottoLogger = _log()
    ottoLogger.info("Otto API - Updating /Robots/ Tags")

    try:
        robotResults, errorResult = _fetch_json_results(
            url,
            "HTTP GET failed for /Robots/",
            ottoLogger,
        )
        if errorResult is not None:
            return errorResult

        basePath = getFleetRobotsPath()
        apiRobots = []
        writes = []

        for robot in robotResults:
            instanceName, tagValues = buildRobotTagValues(basePath, robot)
            if not instanceName:
                continue

            apiRobots.append(instanceName)
            instancePath = basePath + "/" + instanceName

            if not tagExists(instancePath):
                ensureUdtInstancePath(instancePath, "api_Robot")
                ottoLogger.info("Otto API - Created new robot tag instance: " + instanceName)

            writeObservedPairs(tagValues.items(), "Otto_API.Robots.Get robot sync", ottoLogger)
            writes.extend(tagValues.items())

        try:
            existingRobots = listUdtInstanceNames(browseTagResults(basePath))

            for robotName in existingRobots:
                if robotName not in apiRobots:
                    deleteTagPath(basePath + "/" + robotName)
                    ottoLogger.info("Otto API - Removed stale robot tag instance: " + robotName)

        except Exception as e:
            ottoLogger.warn("Otto API - Cleanup skipped due to error: " + str(e))

        return buildRobotSyncResult(
            True,
            "info",
            "Robots updated for {} instance(s)".format(len(apiRobots)),
            records=robotResults,
            writes=writes
        )

    except Exception as e:
        ottoLogger.error("Otto API - /Robots/ Tag Update Failed - " + str(e))
        return buildRobotSyncResult(False, "error", "Robot tag update failed - " + str(e))


def updateRobotOperationalState():
    """
    Sync robot operational state in one pass:
    system state, activity state, charge level, and AvailableForWork.
    """
    ottoLogger = _log()
    robotsBasePath = getFleetRobotsPath()

    try:
        minCharge = readRequiredTagValue(
            getMissionMinChargePath(),
            "Minimum charge threshold"
        )
        chargingDelayMs, missionLastUpdateTs, missionLastUpdateSuccess = readOptionalTagValues(
            [
                getRobotChargingDelayMsPath(),
                getMissionLastUpdateTsPath(),
                getMissionLastUpdateSuccessPath(),
            ],
            [0, None, False],
            allowEmptyString=False
        )
        chargingDelayMs = int(chargingDelayMs or 0)
        missionLastUpdateSuccess = bool(missionLastUpdateSuccess)
    except ValueError as e:
        message = str(e)
        ottoLogger.warn(message)
        return buildRobotSyncResult(
            False,
            "warn",
            message,
            issues=[
                buildRuntimeIssue(
                    "robot_state.required_config_missing",
                    "Otto_API.Robots.Get",
                    "warn",
                    message,
                )
            ],
        )

    try:
        _, robotTags, invalidRobotRows, readPlan = readRobotInventory(robotsBasePath)
        invalidated = collectInvalidRobotWrites(invalidRobotRows, ottoLogger)
        currentValues = _read_current_robot_values(readPlan)

        baseUrl = getApiBaseUrl()
        systemStateResults, errorResult = _fetch_json_results(
            baseUrl + "/robots/states/?fields=%2A",
            "HTTP GET failed for /robots/system_states/",
            ottoLogger
        )
        if errorResult is not None:
            return errorResult

        activityResults, errorResult = _fetch_json_results(
            baseUrl + "/robots/activities/?fields=activity,robot&offset=0&limit=100",
            "HTTP GET failed for /robots/activities/",
            ottoLogger
        )
        if errorResult is not None:
            return errorResult

        batteryResults, errorResult = _fetch_json_results(
            baseUrl + "/robots/batteries/?fields=percentage,robot",
            "HTTP GET failed for /robots/batteries/",
            ottoLogger
        )
        if errorResult is not None:
            return errorResult

        placeResults, errorResult = _fetch_json_results(
            baseUrl + "/robots/places/?fields=%2A&offset=0&limit=100",
            "HTTP GET failed for /robots/places/",
            ottoLogger
        )
        if errorResult is not None:
            return errorResult

        statesByRobot = groupRecordsByRobot(
            [RobotSystemStateEntry.fromDict(record) for record in list(systemStateResults or [])],
            "robot"
        )
        activityByRobot = _robot_keyed_values(activityResults, "robot", "activity")
        chargeByRobot = _robot_keyed_values(
            batteryResults,
            "robot",
            "percentage",
            valueTransform=normalizeChargePercentage
        )
        placesByRobot = _robot_places(placeResults)

        writesByPath = {}
        nowDate = system.date.now()
        nowTimestamp = timestampString()
        pendingRobotStateHistoryRows = []

        for invalidPath, invalidValue in invalidated:
            writesByPath[invalidPath] = invalidValue

        robotSnapshots = []
        nowEpochMs = _epochMillis(nowDate)
        for robotId, robotPath in robotTags.items():
            robotName = str(robotPath).rsplit("/", 1)[1]
            dominant = selectDominantSystemState(statesByRobot.get(robotId, []), logger=ottoLogger)

            systemStatePath = robotPath + "/SystemState"
            subSystemPath = robotPath + "/SubSystemState"
            priorityPath = robotPath + "/SystemStatePriority"
            updatedTsPath = robotPath + "/SystemStateUpdatedTs"
            activityPath = robotPath + "/ActivityState"
            chargePath = robotPath + "/ChargeLevel"
            activeMissionCountPath = robotPath + "/ActiveMissionCount"
            failedMissionCountPath = robotPath + "/FailedMissionCount"
            placeIdPath = robotPath + "/PlaceId"
            placeNamePath = robotPath + "/PlaceName"
            chargingTofPath = robotPath + "/ChargingTOF"
            chargingTsPath = robotPath + "/Charging_TS"
            robotStateLogSignaturePath = robotPath + "/" + ROBOT_STATE_LOG_SIGNATURE_MEMBER

            previousSystemState = currentValues.get(systemStatePath)
            previousSubSystemState = currentValues.get(subSystemPath)
            previousActivityState = currentValues.get(activityPath)
            effectivePriority = currentValues.get(priorityPath)
            effectiveUpdatedTs = currentValues.get(updatedTsPath)
            previousRobotStateLogSignature = currentValues.get(robotStateLogSignaturePath)
            snapshot = RobotSnapshot(
                robotName,
                robotPath,
                currentValues.get(systemStatePath),
                currentValues.get(activityPath),
                currentValues.get(chargePath),
                currentValues.get(activeMissionCountPath),
                currentValues.get(failedMissionCountPath),
                currentValues.get(placeIdPath),
                currentValues.get(placeNamePath),
                currentValues.get(chargingTofPath),
                currentValues.get(chargingTsPath),
            )
            effectiveSubSystemState = currentValues.get(subSystemPath)

            if dominant is not None:
                snapshot = snapshot.withUpdatedOperationalState(
                    systemState=dominant.system_state,
                )
                effectiveSubSystemState = dominant.sub_system_state
                effectivePriority = dominant.priority
                effectiveUpdatedTs = nowDate
                writesByPath[systemStatePath] = snapshot.system_state
                writesByPath[subSystemPath] = effectiveSubSystemState
                writesByPath[priorityPath] = effectivePriority
                writesByPath[updatedTsPath] = effectiveUpdatedTs

            if robotId in activityByRobot:
                snapshot = snapshot.withUpdatedOperationalState(
                    activityState=activityByRobot.get(robotId),
                )
                writesByPath[activityPath] = snapshot.activity_state

            if robotId in chargeByRobot:
                snapshot = snapshot.withUpdatedOperationalState(
                    chargeLevel=chargeByRobot.get(robotId),
                )
                writesByPath[chargePath] = snapshot.charge_level

            snapshot = snapshot.withPlace(placesByRobot.get(robotId, RobotPlace.empty()))
            writesByPath[placeIdPath] = snapshot.place_id
            writesByPath[placeNamePath] = snapshot.place_name

            chargingTof, chargingTs = _chargingStateUpdate(
                snapshot.activity_state,
                snapshot.charging_tof,
                snapshot.charging_ts,
                chargingDelayMs,
                nowEpochMs
            )
            snapshot = snapshot.withChargingState(chargingTof, chargingTs)
            writesByPath[chargingTofPath] = snapshot.charging_tof
            writesByPath[chargingTsPath] = snapshot.charging_ts

            historyUpdate = buildRobotStateHistoryUpdate(
                robotName,
                previousSystemState,
                snapshot.system_state,
                previousSubSystemState,
                effectiveSubSystemState,
                previousActivityState,
                snapshot.activity_state,
                previousRobotStateLogSignature,
                robotStateLogSignaturePath,
                tagExists(robotStateLogSignaturePath),
                ottoLogger,
            )
            if historyUpdate is not None:
                pendingRobotStateHistoryRows.append(historyUpdate["pending_row"])
                signaturePath, signatureValue = historyUpdate["signature_write"]
                writesByPath[signaturePath] = signatureValue

            robotSnapshots.append(snapshot)

        readinessBatch = buildReadinessResultsAndWrites(
            robotSnapshots,
            minCharge,
            chargingDelayMs,
            missionLastUpdateTs,
            missionLastUpdateSuccess
        )
        for path, value in zip(
            readinessBatch["write_paths"],
            readinessBatch["write_values"]
        ):
            writesByPath[path] = value

        if writesByPath:
            writeObservedPairs(writesByPath.items(), "Otto_API.Robots.Get operational state sync", ottoLogger)

        appendPendingRobotStateHistoryRows(nowTimestamp, pendingRobotStateHistoryRows)

        allRecords = list(systemStateResults) + list(activityResults) + list(batteryResults) + list(placeResults)
        writes = list(writesByPath.items())
        return buildRobotSyncResult(
            True,
            "info",
            "Robot operational state updated for {} robot(s)".format(len(robotTags)),
            records=list(allRecords or []),
            writes=list(writes or [])
        )

    except Exception as e:
        message = "Failed to update robot operational state - " + str(e)
        ottoLogger.error("Otto API - " + message)
        return buildRobotSyncResult(
            False,
            "error",
            message,
            issues=[
                buildRuntimeIssue(
                    "robot_state.update_failed",
                    "Otto_API.Robots.Get",
                    "error",
                    message,
                )
            ],
        )
