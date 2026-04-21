import json

from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.SyncHelpers import listUdtInstanceNames
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import deleteTagPath
from Otto_API.Common.TagHelpers import ensureUdtInstancePath
from Otto_API.Common.TagHelpers import getApiBaseUrl
from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getMissionLastUpdateSuccessPath
from Otto_API.Common.TagHelpers import getMissionLastUpdateTsPath
from Otto_API.Common.TagHelpers import getMissionMinChargePath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.TagHelpers import writeLastSystemResponse
from Otto_API.Fleet.RobotReadiness import buildReadinessResultsAndWrites
from Otto_API.Robots.Inventory import collectInvalidRobotWrites
from Otto_API.Robots.Inventory import readRobotInventory
from Otto_API.Robots.Normalize import buildRobotTagValues
from Otto_API.Robots.ObservedWrites import buildRobotSyncResult
from Otto_API.Robots.ObservedWrites import writeObservedPairs
from Otto_API.Robots.StateHistory import appendPendingRobotStateHistoryRows
from Otto_API.Robots.StateHistory import buildRobotStateHistoryUpdate
from Otto_API.Robots.StateHistory import ROBOT_STATE_LOG_SIGNATURE_MEMBER
from Otto_API.Robots.SyncHelpers import buildRobotMetricWrites
from Otto_API.Robots.SyncHelpers import groupRecordsByRobot
from Otto_API.Robots.SyncHelpers import normalizeChargePercentage
from Otto_API.Robots.SyncHelpers import selectDominantSystemState


def _log():
    return system.util.getLogger("Otto_API.Robots.Get")


def updateRobots():
    """
    Get vehicle information from OTTO and sync Fleet/Robots inventory tags.
    """
    url = getApiBaseUrl() + "/robots/?fields=id,hostname,name,serial_number"
    ottoLogger = _log()
    ottoLogger.info("Otto API - Updating /Robots/ Tags")

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())

        if not response:
            ottoLogger.error("Otto API - HTTPGet Failed for /Robots/")
            return buildRobotSyncResult(False, "error", "HTTP GET failed for /Robots/")

        writeLastSystemResponse(response)

        try:
            data = json.loads(response)
        except Exception as jsonErr:
            ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
            return buildRobotSyncResult(False, "error", "Robot JSON decode error - {}".format(jsonErr))

        basePath = getFleetRobotsPath()
        robotResults = data.get("results", [])
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


def updateSystemStates():
    """
    Sync dominant OTTO system-state data into Fleet/Robots.
    """
    ottoLogger = _log()
    baseUrl = getApiBaseUrl()
    url = baseUrl + "/robots/states/?fields=%2A"
    robotsBasePath = getFleetRobotsPath()

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())

        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/system_states/")
            return buildRobotSyncResult(False, "error", "HTTP GET failed for /robots/system_states/")

        data = json.loads(response)
        results = data.get("results", [])

        statesByRobot = groupRecordsByRobot(results, "robot")
        _, robotTags, invalidRobotRows, _ = readRobotInventory(robotsBasePath)

        writes = []
        invalidated = collectInvalidRobotWrites(invalidRobotRows, ottoLogger)
        nowDate = system.date.now()

        for robotId, stateList in statesByRobot.items():
            if robotId not in robotTags:
                ottoLogger.warn("SystemState received for unknown robot ID " + robotId)
                continue

            dominant = selectDominantSystemState(stateList, logger=ottoLogger)
            if dominant is None:
                continue

            robotPath = robotTags[robotId]
            try:
                paths = [
                    robotPath + "/SystemState",
                    robotPath + "/SubSystemState",
                    robotPath + "/SystemStatePriority",
                    robotPath + "/SystemStateUpdatedTs",
                ]
                values = [
                    dominant.get("system_state"),
                    dominant.get("sub_system_state"),
                    dominant.get("priority"),
                    nowDate,
                ]
                writes.extend(zip(paths, values))
            except Exception as exc:
                ottoLogger.warn(
                    "Failed to write SystemState for robot {} - {}".format(
                        robotId,
                        str(exc)
                    )
                )

        finalWrites = writes + invalidated
        if finalWrites:
            writeObservedPairs(finalWrites, "Otto_API.Robots.Get system state sync", ottoLogger)

        return buildRobotSyncResult(
            True,
            "info",
            "System states updated for {} robot(s)".format(len(writes) // 4),
            records=results,
            writes=finalWrites
        )

    except Exception as e:
        ottoLogger.error("Otto API - Failed to update system states - " + str(e))
        return buildRobotSyncResult(False, "error", "Failed to update system states - " + str(e))


def updateChargeLevels():
    """
    Sync OTTO battery percentages into Fleet/Robots.
    """
    baseUrl = getApiBaseUrl()
    url = baseUrl + "/robots/batteries/?fields=percentage,robot"
    ottoLogger = _log()

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/batteries/")
            return buildRobotSyncResult(False, "error", "HTTP GET failed for /robots/batteries/")

        batteryData = json.loads(response)
        basePath = getFleetRobotsPath()
        batteryResults = batteryData.get("results", [])

        _, robotTags, invalidRobotRows, _ = readRobotInventory(basePath)
        invalidated = collectInvalidRobotWrites(invalidRobotRows, ottoLogger)
        writes, unmatchedRobotIds = buildRobotMetricWrites(
            robotTags,
            batteryResults,
            "robot",
            "percentage",
            "ChargeLevel"
        )
        writes = [
            (path, normalizeChargePercentage(value))
            for path, value in writes
        ]

        for robotId in unmatchedRobotIds:
            ottoLogger.warn("No matching robot tag found for robot ID " + robotId)

        finalWrites = writes + invalidated
        if finalWrites:
            writeObservedPairs(finalWrites, "Otto_API.Robots.Get charge sync", ottoLogger)

        return buildRobotSyncResult(
            True,
            "info",
            "Charge levels updated for {} robot(s)".format(len(writes)),
            records=batteryResults,
            writes=finalWrites
        )

    except Exception as e:
        ottoLogger.error("Otto API - Failed to update charge levels - " + str(e))
        return buildRobotSyncResult(False, "error", "Failed to update charge levels - " + str(e))


def updateActivityStates():
    """
    Sync OTTO activity-state data into Fleet/Robots.
    """
    baseUrl = getApiBaseUrl()
    url = baseUrl + "/robots/activities/?fields=activity,robot&offset=0&limit=100"
    ottoLogger = _log()

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/activities/")
            return buildRobotSyncResult(False, "error", "HTTP GET failed for /robots/activities/")

        activityData = json.loads(response)
        basePath = getFleetRobotsPath()
        activityResults = activityData.get("results", [])

        _, robotTags, invalidRobotRows, _ = readRobotInventory(basePath)
        invalidated = collectInvalidRobotWrites(invalidRobotRows, ottoLogger)
        writes, unmatchedRobotIds = buildRobotMetricWrites(
            robotTags,
            activityResults,
            "robot",
            "activity",
            "ActivityState"
        )

        for robotId in unmatchedRobotIds:
            ottoLogger.warn("No matching robot tag found for robot ID " + robotId)

        finalWrites = writes + invalidated
        if finalWrites:
            writeObservedPairs(finalWrites, "Otto_API.Robots.Get activity sync", ottoLogger)

        return buildRobotSyncResult(
            True,
            "info",
            "Activity states updated for {} robot(s)".format(len(writes)),
            records=activityResults,
            writes=finalWrites
        )

    except Exception as e:
        ottoLogger.error("Otto API - Failed to update activity states - " + str(e))
        return buildRobotSyncResult(False, "error", "Failed to update activity states - " + str(e))


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
        missionLastUpdateTs = readOptionalTagValue(
            getMissionLastUpdateTsPath(),
            None,
            allowEmptyString=False
        )
        missionLastUpdateSuccess = bool(readOptionalTagValue(
            getMissionLastUpdateSuccessPath(),
            False
        ))
    except ValueError as e:
        message = str(e)
        ottoLogger.warn(message)
        return buildRobotSyncResult(False, "warn", message)

    try:
        browseResults, robotTags, invalidRobotRows, readPlan = readRobotInventory(robotsBasePath)
        invalidated = collectInvalidRobotWrites(invalidRobotRows, ottoLogger)

        robotPaths = [row["robot_path"] for row in readPlan]
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
                robotPath + "/" + ROBOT_STATE_LOG_SIGNATURE_MEMBER,
            ])

        currentValues = {}
        if currentValuePaths:
            readResults = readTagValues(currentValuePaths)
            for path, qualifiedValue in zip(currentValuePaths, readResults):
                currentValues[path] = qualifiedValue.value if qualifiedValue.quality.isGood() else None

        baseUrl = getApiBaseUrl()
        systemStateResponse = httpGet(
            url=baseUrl + "/robots/states/?fields=%2A",
            headerValues=jsonHeaders()
        )
        activityResponse = httpGet(
            url=baseUrl + "/robots/activities/?fields=activity,robot&offset=0&limit=100",
            headerValues=jsonHeaders()
        )
        batteryResponse = httpGet(
            url=baseUrl + "/robots/batteries/?fields=percentage,robot",
            headerValues=jsonHeaders()
        )

        if not systemStateResponse:
            return buildRobotSyncResult(False, "error", "HTTP GET failed for /robots/system_states/")
        if not activityResponse:
            return buildRobotSyncResult(False, "error", "HTTP GET failed for /robots/activities/")
        if not batteryResponse:
            return buildRobotSyncResult(False, "error", "HTTP GET failed for /robots/batteries/")

        systemStateResults = json.loads(systemStateResponse).get("results", [])
        activityResults = json.loads(activityResponse).get("results", [])
        batteryResults = json.loads(batteryResponse).get("results", [])

        statesByRobot = groupRecordsByRobot(systemStateResults, "robot")
        activityByRobot = {}
        for record in list(activityResults or []):
            robotId = record.get("robot")
            if robotId is None:
                continue
            activityByRobot[str(robotId).strip()] = record.get("activity")

        chargeByRobot = {}
        for record in list(batteryResults or []):
            robotId = record.get("robot")
            if robotId is None:
                continue
            chargeByRobot[str(robotId).strip()] = normalizeChargePercentage(record.get("percentage"))

        writesByPath = {}
        nowDate = system.date.now()
        nowTimestamp = timestampString()
        pendingRobotStateHistoryRows = []

        for invalidPath, invalidValue in invalidated:
            writesByPath[invalidPath] = invalidValue

        robotSnapshots = []
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
            robotStateLogSignaturePath = robotPath + "/" + ROBOT_STATE_LOG_SIGNATURE_MEMBER

            previousSystemState = currentValues.get(systemStatePath)
            previousSubSystemState = currentValues.get(subSystemPath)
            previousActivityState = currentValues.get(activityPath)
            effectiveSystemState = currentValues.get(systemStatePath)
            effectiveSubSystemState = currentValues.get(subSystemPath)
            effectivePriority = currentValues.get(priorityPath)
            effectiveUpdatedTs = currentValues.get(updatedTsPath)
            effectiveActivity = currentValues.get(activityPath)
            effectiveCharge = currentValues.get(chargePath)
            effectiveActiveMissionCount = currentValues.get(activeMissionCountPath)
            effectiveFailedMissionCount = currentValues.get(failedMissionCountPath)
            previousRobotStateLogSignature = currentValues.get(robotStateLogSignaturePath)

            if dominant is not None:
                effectiveSystemState = dominant.get("system_state")
                effectiveSubSystemState = dominant.get("sub_system_state")
                effectivePriority = dominant.get("priority")
                effectiveUpdatedTs = nowDate
                writesByPath[systemStatePath] = effectiveSystemState
                writesByPath[subSystemPath] = effectiveSubSystemState
                writesByPath[priorityPath] = effectivePriority
                writesByPath[updatedTsPath] = effectiveUpdatedTs

            if robotId in activityByRobot:
                effectiveActivity = activityByRobot.get(robotId)
                writesByPath[activityPath] = effectiveActivity

            if robotId in chargeByRobot:
                effectiveCharge = chargeByRobot.get(robotId)
                writesByPath[chargePath] = effectiveCharge

            historyUpdate = buildRobotStateHistoryUpdate(
                robotName,
                previousSystemState,
                effectiveSystemState,
                previousSubSystemState,
                effectiveSubSystemState,
                previousActivityState,
                effectiveActivity,
                previousRobotStateLogSignature,
                robotStateLogSignaturePath,
                tagExists(robotStateLogSignaturePath),
                ottoLogger,
            )
            if historyUpdate is not None:
                pendingRobotStateHistoryRows.append(historyUpdate["pending_row"])
                signaturePath, signatureValue = historyUpdate["signature_write"]
                writesByPath[signaturePath] = signatureValue

            robotSnapshots.append({
                "robot_name": robotName,
                "robot_path": robotPath,
                "system_state": effectiveSystemState,
                "activity_state": effectiveActivity,
                "charge_level": effectiveCharge,
                "active_mission_count": effectiveActiveMissionCount,
                "failed_mission_count": effectiveFailedMissionCount,
            })

        readinessBatch = buildReadinessResultsAndWrites(
            robotSnapshots,
            minCharge,
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

        allRecords = list(systemStateResults) + list(activityResults) + list(batteryResults)
        writes = list(writesByPath.items())
        return buildRobotSyncResult(
            True,
            "info",
            "Robot operational state updated for {} robot(s)".format(len(robotTags)),
            records=allRecords,
            writes=writes
        )

    except Exception as e:
        ottoLogger.error(
            "Otto API - Failed to update robot operational state - " + str(e)
        )
        return buildRobotSyncResult(False, "error", "Failed to update robot operational state - " + str(e))
