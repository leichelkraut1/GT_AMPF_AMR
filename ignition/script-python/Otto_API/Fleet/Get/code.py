import json
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import getApiBaseUrl
from Otto_API.Common.TagHelpers import getFleetMapsPath
from Otto_API.Common.TagHelpers import getFleetPlacesPath
from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import getFleetSystemPath
from Otto_API.Common.TagHelpers import getFleetWorkflowsPath
from Otto_API.Common.TagHelpers import getMissionLastUpdateSuccessPath
from Otto_API.Common.TagHelpers import getMissionLastUpdateTsPath
from Otto_API.Common.TagHelpers import getMissionMinChargePath
from Otto_API.Common.TagHelpers import getSystemLastResponsePath
from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import deleteTagPath
from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureUdtInstancePath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.TagHelpers import writeLastSystemResponse
from Otto_API.Common.TagHelpers import writeObservedTagValues
from Otto_API.Common.TagHelpers import writeTagValue
from Otto_API.Common.TagHelpers import writeTagValueAsync
from Otto_API.Common.TagHelpers import writeTagValues
from Otto_API.Common.TagHelpers import writeTagValuesAsync
from Otto_API.Fleet.ContentSync import buildMapInstanceName
from Otto_API.Fleet.ContentSync import buildMapTagValues
from Otto_API.Fleet.ContentSync import buildPlaceRecipeWrites
from Otto_API.Fleet.ContentSync import buildRobotTagValues
from Otto_API.Fleet.ContentSync import buildUdtInstanceDef
from Otto_API.Fleet.ContentSync import buildWorkflowTagValues
from Otto_API.Fleet.ContentSync import listUdtInstanceNames
from Otto_API.Fleet.ContentSync import normalizeContainerRecord
from Otto_API.Fleet.ContentSync import normalizePlaceRecord
from Otto_API.Fleet.ContentSync import sanitizeTagName
from Otto_API.Fleet.ContentSync import selectMostRecentMap
from Otto_API.Fleet.FleetSync import buildMissionsUrl
from Otto_API.Fleet.FleetSync import buildInvalidRobotSyncWrites
from Otto_API.Fleet.FleetSync import buildRobotMetricWrites
from Otto_API.Fleet.FleetSync import groupRecordsByRobot
from Otto_API.Fleet.FleetSync import invalidateRobotSyncState
from Otto_API.Fleet.FleetSync import normalizeChargePercentage
from Otto_API.Fleet.FleetSync import parseIsoTimestampToEpochMillis
from Otto_API.Fleet.FleetSync import parseJsonResponse
from Otto_API.Fleet.FleetSync import parseListPayload
from Otto_API.Fleet.FleetSync import parseMissionResults
from Otto_API.Fleet.FleetSync import parseServerStatus
from Otto_API.Fleet.FleetSync import readRobotInventoryMetadata
from Otto_API.Fleet.FleetSync import selectDominantSystemState
from Otto_API.Fleet.RobotReadiness import buildReadinessResultsAndWrites
from MainController.CommandHelpers import appendRobotStateHistoryRow
from MainController.CommandHelpers import buildRobotStateLogSignature
from MainController.CommandHelpers import timestampString

SYSTEM_BASE_PATH = getFleetSystemPath()
ROBOTS_BASE_PATH = getFleetRobotsPath()
PLACES_BASE_PATH = getFleetPlacesPath()
MAPS_BASE_PATH = getFleetMapsPath()
WORKFLOWS_BASE_PATH = getFleetWorkflowsPath()
CONTAINERS_BASE_PATH = getFleetContainersPath()
ROBOT_STATE_LOG_SIGNATURE_MEMBER = "LastRobotStateLogSignature"
_MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS = set()


def _log():
    return system.util.getLogger("Otto_API.Fleet.Get")

def _buildSyncResult(ok, level, message, records=None, writes=None, data=None):
    records = list(records or [])
    writes = list(writes or [])
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "records": records,
            "writes": writes,
            "value": data,
        },
        records=records,
        writes=writes,
    )


def _warnMissingRobotStateLogSignaturePath(logger, tagPath):
    """Warn once when the robot state log signature member is missing from api_Robot."""
    tagPath = str(tagPath or "")
    if not tagPath or tagPath in _MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS:
        return
    _MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS.add(tagPath)
    logger.warn(
        "Robot state history dedupe is disabled until [{}] is added to api_Robot".format(
            tagPath
        )
    )


def _writeObservedPairs(writes, label, logger):
    """Write UDT-backed tag pairs and warn on any bad result."""
    writePairs = list(writes or [])
    if not writePairs:
        return
    writeObservedTagValues(
        [path for path, _ in writePairs],
        [value for _, value in writePairs],
        labels=[label] * len(writePairs),
        logger=logger
    )


def _readRobotInventory(robotsBasePath):
    inventory = readRobotInventoryMetadata(robotsBasePath)
    return (
        inventory["browse_results"],
        inventory["robot_path_by_id"],
        inventory["invalid_robot_rows"],
        inventory["read_plan"],
    )


def _collectInvalidRobotWrites(invalidRobotRows, ottoLogger):
    invalidated = []
    for invalidRow in list(invalidRobotRows or []):
        robotPath = invalidRow["robot_path"]
        reason = invalidRow["reason"]
        ottoLogger.warn(
            "Invalid robot ID for {} - {}".format(robotPath, reason)
        )
        try:
            invalidated.extend(list(buildInvalidRobotSyncWrites(robotPath)))
        except Exception as exc:
            ottoLogger.warn(
                "Failed to invalidate sync state for {} - {}".format(
                    robotPath,
                    str(exc)
                )
            )
    return invalidated


def getServerStatus():
    """
    Gets Fleet Manager server states.
    """
    url = getApiBaseUrl() + "/system/state/"
    ottoLogger = _log()

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        if response:
            status = parseServerStatus(response)
            writeTagValueAsync(SYSTEM_BASE_PATH + "/ServerStatus", status)
            return _buildSyncResult(True, "info", "Server status updated", data=status)

        ottoLogger.warn("Otto Fleet Manager Did Not Respond to Status Update Request")
        writeTagValueAsync(SYSTEM_BASE_PATH + "/ServerStatus", "ResponseError")
        return _buildSyncResult(False, "warn", "Otto Fleet Manager did not respond")
    except Exception as e:
        ottoLogger.error("Otto API - Status Update Failed - " + str(e))
        return _buildSyncResult(False, "error", "Status update failed - " + str(e))


def readCachedServerStatus():
    """
    Read the most recent server status value written by the slower status timer.

    The fast MainController loop uses this cached value so it does not need to
    make a separate HTTP request every pass.
    """
    status = readOptionalTagValue(
        SYSTEM_BASE_PATH + "/ServerStatus",
        None,
        allowEmptyString=False
    )
    if status in [None, "", "ResponseError"]:
        return _buildSyncResult(
            False,
            "warn",
            "Cached server status is unavailable",
            data=status
        )

    return _buildSyncResult(
        True,
        "info",
        "Cached server status read",
        data=status
    )


def getMissions(logger, debug, mission_status=None, limit=None):
    """
    Gets mission status info from OTTO for one or more mission statuses.
    If mission_status is None, returns an empty list (intentional safety).
    """
    try:
        if not mission_status:
            if debug:
                logger.warn("getMissions called with no mission_status")
            return []

        base = getApiBaseUrl()
        url = buildMissionsUrl(base, mission_status, limit)
        if isinstance(mission_status, (list, tuple)):
            statusLabel = ",".join([str(x) for x in mission_status])
        else:
            statusLabel = str(mission_status)

        if debug:
            logger.debug(
                "Otto API - Requesting missions status={} url={}".format(
                    statusLabel, url
                )
            )

        response = httpGet(url=url, headerValues=jsonHeaders())

        results = parseMissionResults(response)

        if debug:
            logger.debug(
                "Otto API - Received {} missions for status {}".format(
                    len(results), statusLabel
                )
            )

        return results

    except Exception as e:
        logger.error(
            "Otto API - Error fetching missions (status={}): {}".format(
                mission_status, e
            )
        )
        return []


def updateRobots():
    """
    Gets vehicle information from Otto and creates tags for each vehicle in [Otto_FleetManager]Fleet/Robots.
    Also removes UDT instances that no longer exist in the API response.
    Intended to be run only when a vehicle is added or removed from the Fleet.
    """
    url = getApiBaseUrl() + "/robots/?fields=id,hostname,name,serial_number"
    ottoLogger = _log()
    ottoLogger.info("Otto API - Updating /Robots/ Tags")

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())

        if response:
            ottoLogger.info("Otto API - Updating /Robots/ - Response Received")
            writeLastSystemResponse(response)

            try:
                data = parseJsonResponse(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Robot JSON decode error - {}".format(jsonErr))

            basePath = ROBOTS_BASE_PATH
            robotResults = data.get("results", [])
            apiRobots = []
            writes = []

            for robot in robotResults:
                instanceName, tagValues = buildRobotTagValues(basePath, robot)
                if not instanceName:
                    continue

                apiRobots.append(instanceName)
                instancePath = basePath + "/" + instanceName
                exists = tagExists(instancePath)

                if not exists:
                    ensureUdtInstancePath(instancePath, "api_Robot")
                    ottoLogger.info("Otto API - Created new robot tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing robot tag instance: " + instanceName)

                writeObservedTagValues(
                    list(tagValues.keys()),
                    list(tagValues.values()),
                    labels=["Otto_API.Get robot sync"] * len(tagValues),
                    logger=ottoLogger
                )
                writes.extend(tagValues.items())

            try:
                existingRobots = listUdtInstanceNames(browseTagResults(basePath))

                for robotName in existingRobots:
                    if robotName not in apiRobots:
                        instancePath = basePath + "/" + robotName
                        deleteTagPath(instancePath)
                        ottoLogger.info("Otto API - Removed stale robot tag instance: " + robotName)

            except Exception as e:
                ottoLogger.warn("Otto API - Cleanup skipped due to error: " + str(e))

            return _buildSyncResult(
                True,
                "info",
                "Robots updated for {} instance(s)".format(len(apiRobots)),
                records=robotResults,
                writes=writes
            )

        else:
            ottoLogger.error("Otto API - HTTPGet Failed for /Robots/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /Robots/")

    except Exception as e:
        ottoLogger.error("Otto API - /Robots/ Tag Update Failed - " + str(e))
        return _buildSyncResult(False, "error", "Robot tag update failed - " + str(e))


def updateSystemStates():
    """
    Retrieves system_state entries from OTTO, resolves the dominant
    system state per robot using priority arbitration, and writes
    the results into robot UDTs.

    OTTO priority rule:
        LOWER numeric priority = HIGHER authority
        If priorities tie, newest 'created' timestamp wins
    """
    ottoLogger = _log()

    baseUrl = getApiBaseUrl()
    url = baseUrl + "/robots/states/?fields=%2A"
    robotsBasePath = ROBOTS_BASE_PATH

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())

        if not response:
            ottoLogger.error(
                "Otto API - HTTP GET failed for /robots/system_states/"
            )
            return

        data = json.loads(response)
        results = data.get("results", [])

        statesByRobot = groupRecordsByRobot(results, "robot")
        _, robotTags, invalidRobotRows, _ = _readRobotInventory(robotsBasePath)

        writes = []
        invalidated = _collectInvalidRobotWrites(invalidRobotRows, ottoLogger)
        nowDate = system.date.now()

        for robotId, stateList in statesByRobot.items():
            if robotId not in robotTags:
                ottoLogger.warn(
                    "SystemState received for unknown robot ID " +
                    robotId
                )
                continue

            dominant = selectDominantSystemState(stateList)
            if dominant is None:
                continue
            robotPath = robotTags[robotId]

            try:
                paths = [
                    robotPath + "/SystemState",
                    robotPath + "/SubSystemState",
                    robotPath + "/SystemStatePriority",
                    robotPath + "/SystemStateUpdatedTs"
                ]
                values = [
                    dominant.get("system_state"),
                    dominant.get("sub_system_state"),
                    dominant.get("priority"),
                    nowDate
                ]
                writes.extend(zip(paths, values))
            except Exception as e:
                ottoLogger.warn(
                    "Failed to write SystemState for robot " +
                    robotId + " - " + str(e)
                )

        finalWrites = writes + invalidated
        if finalWrites:
            _writeObservedPairs(finalWrites, "Otto_API.Get system state sync", ottoLogger)

        return _buildSyncResult(
            True,
            "info",
            "System states updated for {} robot(s)".format(len(writes) // 4),
            records=results,
            writes=finalWrites
        )

    except Exception as e:
        ottoLogger.error(
            "Otto API - Failed to update system states - " + str(e)
        )
        return _buildSyncResult(False, "error", "Failed to update system states - " + str(e))


def updateChargeLevels():
    """
    Updates the .ChargeLevel tag for all vehicles in [Otto_FleetManager]Fleet/Robots
    by retrieving battery percentages from the API and matching by robot ID.
    """
    baseUrl = getApiBaseUrl()
    url = baseUrl + "/robots/batteries/?fields=percentage,robot"
    ottoLogger = _log()

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/batteries/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /robots/batteries/")

        batteryData = json.loads(response)
        basePath = ROBOTS_BASE_PATH
        batteryResults = batteryData.get("results", [])

        _, robotTags, invalidRobotRows, _ = _readRobotInventory(basePath)
        invalidated = _collectInvalidRobotWrites(invalidRobotRows, ottoLogger)
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
            _writeObservedPairs(finalWrites, "Otto_API.Get charge sync", ottoLogger)

        return _buildSyncResult(
            True,
            "info",
            "Charge levels updated for {} robot(s)".format(len(writes)),
            records=batteryResults,
            writes=finalWrites
        )

    except Exception as e:
        ottoLogger.error("Otto API - Failed to update charge levels - " + str(e))
        return _buildSyncResult(False, "error", "Failed to update charge levels - " + str(e))


def updateActivityStates():
    """
    Updates the .ActivityState tag for all vehicles in [Otto_FleetManager]Fleet/Robots
    by retrieving activity states from the API and matching by robot ID.
    """
    baseUrl = getApiBaseUrl()
    url = baseUrl + "/robots/activities/?fields=activity,robot&offset=0&limit=100"
    ottoLogger = _log()

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/activities/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /robots/activities/")

        activityData = json.loads(response)
        basePath = ROBOTS_BASE_PATH
        activityResults = activityData.get("results", [])

        _, robotTags, invalidRobotRows, _ = _readRobotInventory(basePath)
        invalidated = _collectInvalidRobotWrites(invalidRobotRows, ottoLogger)
        writes, unmatchedRobotIds = buildRobotMetricWrites(
            robotTags,
            activityResults,
            "robot",
            "activity",
            "ActivityState"
        )

        for robotId in unmatchedRobotIds:
            ottoLogger.warn(
                "No matching robot tag found for robot ID " +
                robotId
            )

        finalWrites = writes + invalidated
        if finalWrites:
            _writeObservedPairs(finalWrites, "Otto_API.Get activity sync", ottoLogger)

        return _buildSyncResult(
            True,
            "info",
            "Activity states updated for {} robot(s)".format(len(writes)),
            records=activityResults,
            writes=finalWrites
        )

    except Exception as e:
        ottoLogger.error(
            "Otto API - Failed to update activity states - " + str(e)
        )
        return _buildSyncResult(False, "error", "Failed to update activity states - " + str(e))


def updateRobotOperationalState():
    """
    Sync robot operational state in one pass:
    system state, activity state, charge level, and AvailableForWork.
    """
    ottoLogger = _log()
    robotsBasePath = ROBOTS_BASE_PATH

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
        return _buildSyncResult(False, "warn", message)

    try:
        browseResults, robotTags, invalidRobotRows, readPlan = _readRobotInventory(robotsBasePath)
        invalidated = _collectInvalidRobotWrites(invalidRobotRows, ottoLogger)

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
            return _buildSyncResult(False, "error", "HTTP GET failed for /robots/system_states/")
        if not activityResponse:
            return _buildSyncResult(False, "error", "HTTP GET failed for /robots/activities/")
        if not batteryResponse:
            return _buildSyncResult(False, "error", "HTTP GET failed for /robots/batteries/")

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
            dominant = selectDominantSystemState(statesByRobot.get(robotId, []))

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

            if (
                previousSystemState != effectiveSystemState or
                previousSubSystemState != effectiveSubSystemState or
                previousActivityState != effectiveActivity
            ):
                if not tagExists(robotStateLogSignaturePath):
                    _warnMissingRobotStateLogSignaturePath(
                        ottoLogger,
                        robotStateLogSignaturePath
                    )
                    robotSnapshots.append({
                        "robot_name": robotName,
                        "robot_path": robotPath,
                        "system_state": effectiveSystemState,
                        "activity_state": effectiveActivity,
                        "charge_level": effectiveCharge,
                        "active_mission_count": effectiveActiveMissionCount,
                        "failed_mission_count": effectiveFailedMissionCount,
                    })
                    continue
                signature = buildRobotStateLogSignature(
                    robotName,
                    previousSystemState,
                    effectiveSystemState,
                    previousSubSystemState,
                    effectiveSubSystemState,
                    previousActivityState,
                    effectiveActivity
                )
                if previousRobotStateLogSignature != signature:
                    pendingRobotStateHistoryRows.append({
                        "robot_name": robotName,
                        "signature": signature,
                        "old_system_state": previousSystemState,
                        "new_system_state": effectiveSystemState,
                        "old_sub_system_state": previousSubSystemState,
                        "new_sub_system_state": effectiveSubSystemState,
                        "old_activity_state": previousActivityState,
                        "new_activity_state": effectiveActivity,
                    })
                    writesByPath[robotStateLogSignaturePath] = signature

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
            writeObservedTagValues(
                list(writesByPath.keys()),
                list(writesByPath.values()),
                labels=["Otto_API.Get operational state sync"] * len(writesByPath),
                logger=ottoLogger
            )

        for row in list(pendingRobotStateHistoryRows or []):
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

        allRecords = list(systemStateResults) + list(activityResults) + list(batteryResults)
        writes = list(writesByPath.items())
        return _buildSyncResult(
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
        return _buildSyncResult(False, "error", "Failed to update robot operational state - " + str(e))


def updatePlaces():
    """
    Gets endpoint information from Otto and creates tags for each endpoint in [Otto_FleetManager]Fleet/Places.
    Also removes UDT instances that no longer exist in the API response.
    Ignores TEMPLATE place types entirely.
    """
    url = getApiBaseUrl() + "/places/"
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Places/")

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        writeLastSystemResponse(response)

        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Places JSON decode error - {}".format(jsonErr))

            writeTagValueAsync(PLACES_BASE_PATH + "/jsonString", response)

            basePath = PLACES_BASE_PATH
            apiPlaces = []
            writes = []

            for place in data:
                normalizedPlace = normalizePlaceRecord(place)
                if normalizedPlace is None:
                    continue

                instanceName = normalizedPlace["instance_name"]
                apiPlaces.append(instanceName)
                instancePath = basePath + "/" + instanceName

                exists = tagExists(instancePath)

                if not exists:
                    ensureUdtInstancePath(instancePath, "api_Place")
                    ottoLogger.info("Otto API - Created new place tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing place tag instance: " + instanceName)

                tagDict = {}
                for suffix, value in normalizedPlace["tag_values"].items():
                    tagDict[instancePath + suffix] = value

                writeObservedTagValues(
                    list(tagDict.keys()),
                    list(tagDict.values()),
                    labels=["Otto_API.Get place sync"] * len(tagDict),
                    logger=ottoLogger
                )
                writes.extend(tagDict.items())

                recipeValueWrites, recipeBoolWrites = buildPlaceRecipeWrites(
                    instancePath,
                    normalizedPlace["recipes"]
                )
                if recipeBoolWrites:
                    writeObservedTagValues(
                        list(recipeBoolWrites.keys()),
                        list(recipeBoolWrites.values()),
                        labels=["Otto_API.Get place recipe bool sync"] * len(recipeBoolWrites),
                        logger=ottoLogger
                    )
                    writes.extend(recipeBoolWrites.items())

                if recipeValueWrites:
                    writeObservedTagValues(
                        list(recipeValueWrites.keys()),
                        list(recipeValueWrites.values()),
                        labels=["Otto_API.Get place recipe value sync"] * len(recipeValueWrites),
                        logger=ottoLogger
                    )
                    writes.extend(recipeValueWrites.items())

            try:
                existingPlaces = listUdtInstanceNames(browseTagResults(basePath))

                for placeName in existingPlaces:
                    if placeName not in apiPlaces:
                        instancePath = basePath + "/" + placeName
                        deleteTagPath(instancePath)
                        ottoLogger.info("Otto API - Removed stale place tag instance: " + placeName)

            except Exception as e:
                ottoLogger.warn("Otto API - Cleanup skipped due to error: " + str(e))

            return _buildSyncResult(
                True,
                "info",
                "Places updated for {} instance(s)".format(len(apiPlaces)),
                records=data,
                writes=writes
            )

        else:
            ottoLogger.error("Otto API - HTTPGet Failed for /Places/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /Places/")

    except Exception as e:
        ottoLogger.error("Otto API - /Places/ Tag Update Failed - " + str(e))
        return _buildSyncResult(False, "error", "Places tag update failed - " + str(e))


def updateMaps():
    """
    Gets Map data from Otto and creates tags in [Otto_FleetManager]Fleet/Maps/ for each map instance.
    Also determines the most recently modified map and stores its ID in ActiveMapID.
    Cleanup removes old map UDT instances but ignores the ActiveMapID memory tag.
    """
    url = getApiBaseUrl() + "/maps/?offset=0&tagged=false"
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Maps/")

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        writeTagValue(MAPS_BASE_PATH + "/updateResponse", response)

        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Maps JSON decode error - {}".format(jsonErr))

            writeTagValueAsync(MAPS_BASE_PATH + "/jsonString", response)

            basePath = MAPS_BASE_PATH
            activeMapTag = basePath + "/ActiveMapID"
            apiMaps = []
            writes = []
            activeMapId = None

            try:
                mostRecent = selectMostRecentMap(data)
                if mostRecent is not None:
                    activeMapId = mostRecent.get("id")
                    writeTagValue(activeMapTag, activeMapId)
                    writes.append((activeMapTag, activeMapId))
                    ottoLogger.info("Otto API - ActiveMapID updated to: " + str(activeMapId))
            except Exception as sortErr:
                ottoLogger.warn("Otto API - Failed to determine most recent map: " + str(sortErr))

            for mapItem in data:
                instanceName, tagDict = buildMapTagValues(basePath, mapItem)
                apiMaps.append(instanceName)
                instancePath = basePath + "/" + instanceName

                exists = tagExists(instancePath)

                if not exists:
                    ensureUdtInstancePath(instancePath, "api_Map")
                    ottoLogger.info("Otto API - Created new map tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing map tag instance: " + instanceName)

                writeObservedTagValues(
                    list(tagDict.keys()),
                    list(tagDict.values()),
                    labels=["Otto_API.Get map sync"] * len(tagDict),
                    logger=ottoLogger
                )
                writes.extend(tagDict.items())

            try:
                existingMaps = listUdtInstanceNames(browseTagResults(basePath))

                for mapName in existingMaps:
                    if mapName not in apiMaps:
                        if mapName == "ActiveMapID":
                            continue

                        instancePath = basePath + "/" + mapName
                        deleteTagPath(instancePath)
                        ottoLogger.info("Otto API - Removed stale map tag instance: " + mapName)

            except Exception as e:
                ottoLogger.warn("Otto API - Cleanup skipped due to error: " + str(e))

            return _buildSyncResult(
                True,
                "info",
                "Maps updated for {} instance(s)".format(len(apiMaps)),
                records=data,
                writes=writes,
                data=activeMapId
            )

        else:
            ottoLogger.error("Otto API - HTTPGet Failed for /Maps/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /Maps/")

    except Exception as e:
        ottoLogger.error("Otto API - /Maps/ Tag Update Failed - " + str(e))
        return _buildSyncResult(False, "error", "Maps tag update failed - " + str(e))


def updateWorkflows():
    """
    Gets Workflows (called Mission Templates in the API documentation) from Otto
    and creates tags in /Workflows/ for each one.
    The full mission JSON (including tasks) is stored in jsonString for later reconstruction.
    """
    baseUrl = getApiBaseUrl() + "/maps/mission_templates/?offset=0&map="
    mapUuid = readRequiredTagValue(MAPS_BASE_PATH + "/ActiveMapID", "Active map ID")
    url = baseUrl + str(mapUuid)
    responseTag = getSystemLastResponsePath()
    basePath    = WORKFLOWS_BASE_PATH
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Workflows/")
    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        writeTagValueAsync(responseTag, response)
        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - Mission templates JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Workflow JSON decode error - {}".format(jsonErr))

            apiTemplates = [tmpl.get("name") for tmpl in data]
            writes = []

            for tmpl in data:
                instanceName, missionDict = buildWorkflowTagValues(basePath, tmpl)
                if not instanceName:
                    continue
                instancePath = basePath + "/" + instanceName

                exists = tagExists(instancePath)

                if not exists:
                    ensureUdtInstancePath(instancePath, "api_Mission")
                    ottoLogger.info("Otto API - Created Workflow: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating Workflow: " + instanceName)

                writeObservedTagValues(
                    list(missionDict.keys()),
                    list(missionDict.values()),
                    labels=["Otto_API.Get workflow sync"] * len(missionDict),
                    logger=ottoLogger
                )
                writes.extend(missionDict.items())

            try:
                existingTemplates = listUdtInstanceNames(browseTagResults(basePath))

                for tmplName in existingTemplates:
                    if tmplName not in apiTemplates:
                        deleteTagPath(basePath + "/" + tmplName)
                        ottoLogger.info("Otto API - Removed stale workflow: " + tmplName)

            except Exception as e:
                ottoLogger.warn("Otto API - Workflow cleanup skipped: {}".format(str(e)))

            return _buildSyncResult(
                True,
                "info",
                "Workflows updated for {} instance(s)".format(len(apiTemplates)),
                records=data,
                writes=writes
            )

        else:
            ottoLogger.error("Otto API - HTTP GET failed for /Workflows/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /Workflows/")

    except Exception as e:
        ottoLogger.error("Otto API - Workflows tag update failed: {}".format(str(e)))
        return _buildSyncResult(False, "error", "Workflow tag update failed: {}".format(str(e)))


def updateContainers():
    """
    Gets container data from Otto and creates tags in [Otto_FleetManager]Fleet/Containers.
    Container instances are named by container ID and stale instances are removed.
    """
    url = getApiBaseUrl() + "/containers/?fields=%2A"
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Containers/")

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        writeLastSystemResponse(response)

        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - Containers JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Containers JSON decode error - {}".format(jsonErr))

            ensureFolder(CONTAINERS_BASE_PATH)
            basePath = CONTAINERS_BASE_PATH
            apiContainers = []
            writes = []

            for containerRecord in data:
                normalizedContainer = normalizeContainerRecord(containerRecord)
                if normalizedContainer is None:
                    continue

                instanceName = normalizedContainer["instance_name"]
                apiContainers.append(instanceName)
                instancePath = basePath + "/" + instanceName

                if not tagExists(instancePath):
                    ensureUdtInstancePath(instancePath, "api_Container")
                    ottoLogger.info("Otto API - Created new container tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing container tag instance: " + instanceName)

                tagDict = {}
                for suffix, value in normalizedContainer["tag_values"].items():
                    tagDict[instancePath + suffix] = value

                writeObservedTagValues(
                    list(tagDict.keys()),
                    list(tagDict.values()),
                    labels=["Otto_API.Get container sync"] * len(tagDict),
                    logger=ottoLogger
                )
                writes.extend(tagDict.items())

            try:
                existingContainers = listUdtInstanceNames(browseTagResults(basePath))

                for containerName in existingContainers:
                    if containerName not in apiContainers:
                        deleteTagPath(basePath + "/" + containerName)
                        ottoLogger.info("Otto API - Removed stale container tag instance: " + containerName)
            except Exception as e:
                ottoLogger.warn("Otto API - Container cleanup skipped due to error: " + str(e))

            return _buildSyncResult(
                True,
                "info",
                "Containers updated for {} instance(s)".format(len(apiContainers)),
                records=data,
                writes=writes
            )

        ottoLogger.error("Otto API - HTTP GET failed for /Containers/")
        return _buildSyncResult(False, "error", "HTTP GET failed for /Containers/")
    except Exception as e:
        ottoLogger.error("Otto API - Containers tag update failed: {}".format(str(e)))
        return _buildSyncResult(False, "error", "Containers tag update failed: {}".format(str(e)))
