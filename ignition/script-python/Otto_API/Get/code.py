import json
from Otto_API.ContentSyncHelpers import buildMapInstanceName
from Otto_API.ContentSyncHelpers import buildMapTagValues
from Otto_API.ContentSyncHelpers import buildPlaceRecipeWrites
from Otto_API.ContentSyncHelpers import buildRobotTagValues
from Otto_API.ContentSyncHelpers import buildUdtInstanceDef
from Otto_API.ContentSyncHelpers import buildWorkflowTagValues
from Otto_API.ContentSyncHelpers import listUdtInstanceNames
from Otto_API.ContentSyncHelpers import normalizePlaceRecord
from Otto_API.ContentSyncHelpers import sanitizeTagName
from Otto_API.ContentSyncHelpers import selectMostRecentMap
from Otto_API.FleetSyncHelpers import buildMissionsUrl
from Otto_API.FleetSyncHelpers import buildRobotIdToPathMap
from Otto_API.FleetSyncHelpers import buildRobotMetricWrites
from Otto_API.FleetSyncHelpers import groupRecordsByRobot
from Otto_API.HttpHelpers import httpGet
from Otto_API.HttpHelpers import jsonHeaders
from Otto_API.FleetSyncHelpers import invalidateRobotSyncState
from Otto_API.FleetSyncHelpers import normalizeChargePercentage
from Otto_API.FleetSyncHelpers import parseIsoTimestampToEpochMillis
from Otto_API.FleetSyncHelpers import parseJsonResponse
from Otto_API.FleetSyncHelpers import parseListPayload
from Otto_API.FleetSyncHelpers import parseMissionResults
from Otto_API.FleetSyncHelpers import parseServerStatus
from Otto_API.FleetSyncHelpers import selectDominantSystemState
from Otto_API.ResultHelpers import buildOperationResult
from Otto_API.TagHelpers import readOptionalTagValue
from Otto_API.TagHelpers import readRequiredTagValue
from Otto_API.TagHelpers import writeTagValue
from Otto_API.TagHelpers import writeTagValueAsync
from Otto_API.TagHelpers import writeTagValues
from Otto_API.TagHelpers import writeTagValuesAsync


def _jsonHeaders():
    return jsonHeaders()


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


def getServerStatus():
    """
    Gets Fleet Manager server states.
    """
    url = readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL") + "/system/state/"
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())
        if response:
            status = parseServerStatus(response)
            writeTagValueAsync("[Otto_FleetManager]System/ServerStatus", status)
            return _buildSyncResult(True, "info", "Server status updated", data=status)

        ottoLogger.warn("Otto Fleet Manager Did Not Respond to Status Update Request")
        writeTagValueAsync("[Otto_FleetManager]System/ServerStatus", "ReponseError")
        return _buildSyncResult(False, "warn", "Otto Fleet Manager did not respond")
    except Exception as e:
        ottoLogger.error("Otto API - Status Update Failed - " + str(e))
        return _buildSyncResult(False, "error", "Status update failed - " + str(e))


def getMissions(logger, debug, mission_status=None, limit=None):
    """
    Gets Mission status info from Otto for a specific mission_status.
    If mission_status is None, returns empty list (intentional safety).
    """
    try:
        if not mission_status:
            if debug:
                logger.warn("getMissions called with no mission_status")
            return []

        base = readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL")
        url = buildMissionsUrl(base, mission_status, limit)

        if debug:
            logger.debug(
                "Otto API - Requesting missions status={} url={}".format(
                    mission_status, url
                )
            )

        response = httpGet(url=url, headerValues=_jsonHeaders())

        results = parseMissionResults(response)

        if debug:
            logger.debug(
                "Otto API - Received {} missions for status {}".format(
                    len(results), mission_status
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
    Gets vehicle information from Otto and creates tags for each vehicle in [Otto_FleetManager]Robots.
    Also removes UDT instances that no longer exist in the API response.
    Intended to be run only when a vehicle is added or removed from the Fleet.
    """
    url = (
        readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL")
        + "/robots/?fields=id,hostname,name,serial_number"
    )
    ottoLogger = system.util.getLogger("Otto_API_Logger")
    ottoLogger.info("Otto API - Updating /Robots/ Tags")

    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())

        if response:
            ottoLogger.info("Otto API - Updating /Robots/ - Response Received")
            writeTagValue("[Otto_FleetManager]System/lastResponse", response)

            try:
                data = parseJsonResponse(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Robot JSON decode error - {}".format(jsonErr))

            basePath = "[Otto_FleetManager]Robots"
            robotResults = data.get("results", [])
            apiRobots = []
            writes = []

            for robot in robotResults:
                instanceName, tagValues = buildRobotTagValues(basePath, robot)
                if not instanceName:
                    continue

                apiRobots.append(instanceName)
                instancePath = basePath + "/" + instanceName
                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = buildUdtInstanceDef(instanceName, "api_Robot")
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created new robot tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing robot tag instance: " + instanceName)

                writeTagValues(list(tagValues.keys()), list(tagValues.values()))
                writes.extend(tagValues.items())

            try:
                existingRobots = listUdtInstanceNames(system.tag.browse(basePath).getResults())

                for robotName in existingRobots:
                    if robotName not in apiRobots:
                        instancePath = basePath + "/" + robotName
                        system.tag.deleteTags([instancePath])
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
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    baseUrl = readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL")
    url = baseUrl + "/robots/states/?fields=%2A"
    robotsBasePath = "[Otto_FleetManager]Robots"

    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())

        if not response:
            ottoLogger.error(
                "Otto API - HTTP GET failed for /robots/system_states/"
            )
            return

        data = json.loads(response)
        results = data.get("results", [])

        statesByRobot = groupRecordsByRobot(results, "robot")

        def _readRobotId(path):
            return readRequiredTagValue(path)

        robotTags, invalidRobotRows = buildRobotIdToPathMap(
            system.tag.browse(robotsBasePath).getResults(),
            robotsBasePath,
            _readRobotId
        )

        writes = []
        invalidated = []

        for invalidRow in invalidRobotRows:
            robotPath = invalidRow["robot_path"]
            reason = invalidRow["reason"]
            ottoLogger.warn(
                "Invalid robot ID for {} - {}".format(robotPath, reason)
            )
            try:
                invalidated.extend(list(invalidateRobotSyncState(robotPath)))
            except Exception as exc:
                ottoLogger.warn(
                    "Failed to invalidate sync state for {} - {}".format(
                        robotPath,
                        str(exc)
                    )
                )

        for robotUUID, stateList in statesByRobot.items():
            if robotUUID not in robotTags:
                ottoLogger.warn(
                    "SystemState received for unknown robot UUID " +
                    robotUUID
                )
                continue

            dominant = selectDominantSystemState(stateList)
            if dominant is None:
                continue
            robotPath = robotTags[robotUUID]

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
                    system.date.now()
                ]
                writeTagValues(paths, values)
                writes.extend(zip(paths, values))
            except Exception as e:
                ottoLogger.warn(
                    "Failed to write SystemState for robot " +
                    robotUUID + " - " + str(e)
                )

        return _buildSyncResult(
            True,
            "info",
            "System states updated for {} robot(s)".format(len(writes) // 4),
            records=results,
            writes=writes + invalidated
        )

    except Exception as e:
        ottoLogger.error(
            "Otto API - Failed to update system states - " + str(e)
        )
        return _buildSyncResult(False, "error", "Failed to update system states - " + str(e))


def updateChargeLevels():
    """
    Updates the .ChargeLevel tag for all vehicles in [Otto_FleetManager]Robots
    by retrieving battery percentages from the API and matching by robot UUID.
    """
    baseUrl = readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL")
    url = baseUrl + "/robots/batteries/?fields=percentage,robot"
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/batteries/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /robots/batteries/")

        batteryData = json.loads(response)
        basePath = "[Otto_FleetManager]Robots"
        batteryResults = batteryData.get("results", [])

        def _readRobotId(path):
            return readRequiredTagValue(path)

        robotTags, invalidRobotRows = buildRobotIdToPathMap(
            system.tag.browse(basePath).getResults(),
            basePath,
            _readRobotId
        )
        invalidated = []
        for invalidRow in invalidRobotRows:
            robotPath = invalidRow["robot_path"]
            reason = invalidRow["reason"]
            ottoLogger.warn(
                "Invalid robot ID for {} - {}".format(robotPath, reason)
            )
            try:
                invalidated.extend(list(invalidateRobotSyncState(robotPath)))
            except Exception as exc:
                ottoLogger.warn(
                    "Failed to invalidate sync state for {} - {}".format(
                        robotPath,
                        str(exc)
                    )
                )
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

        for path, value in writes:
            try:
                writeTagValue(path, value)
            except Exception as e:
                ottoLogger.warn("Failed to write ChargeLevel for " + path + " - " + str(e))

        return _buildSyncResult(
            True,
            "info",
            "Charge levels updated for {} robot(s)".format(len(writes)),
            records=batteryResults,
            writes=writes + invalidated
        )

    except Exception as e:
        ottoLogger.error("Otto API - Failed to update charge levels - " + str(e))
        return _buildSyncResult(False, "error", "Failed to update charge levels - " + str(e))


def updateActivityStates():
    """
    Updates the .ActivityState tag for all vehicles in [Otto_FleetManager]Robots
    by retrieving activity states from the API and matching by robot UUID.
    """
    baseUrl = readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL")
    url = baseUrl + "/robots/activities/?fields=activity,robot&offset=0&limit=100"
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/activities/")
            return _buildSyncResult(False, "error", "HTTP GET failed for /robots/activities/")

        activityData = json.loads(response)
        basePath = "[Otto_FleetManager]Robots"
        activityResults = activityData.get("results", [])

        def _readRobotId(path):
            return readRequiredTagValue(path)

        robotTags, invalidRobotRows = buildRobotIdToPathMap(
            system.tag.browse(basePath).getResults(),
            basePath,
            _readRobotId
        )
        invalidated = []
        for invalidRow in invalidRobotRows:
            robotPath = invalidRow["robot_path"]
            reason = invalidRow["reason"]
            ottoLogger.warn(
                "Invalid robot ID for {} - {}".format(robotPath, reason)
            )
            try:
                invalidated.extend(list(invalidateRobotSyncState(robotPath)))
            except Exception as exc:
                ottoLogger.warn(
                    "Failed to invalidate sync state for {} - {}".format(
                        robotPath,
                        str(exc)
                    )
                )
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

        for path, value in writes:
            try:
                writeTagValues(
                    [path],
                    [value]
                )
            except Exception as e:
                ottoLogger.warn(
                    "Failed to write ActivityState for " +
                    path + " - " + str(e)
                )

        return _buildSyncResult(
            True,
            "info",
            "Activity states updated for {} robot(s)".format(len(writes)),
            records=activityResults,
            writes=writes + invalidated
        )

    except Exception as e:
        ottoLogger.error(
            "Otto API - Failed to update activity states - " + str(e)
        )
        return _buildSyncResult(False, "error", "Failed to update activity states - " + str(e))


def updatePlaces():
    """
    Gets endpoint information from Otto and creates tags for each endpoint in [Otto_FleetManager]Places.
    Also removes UDT instances that no longer exist in the API response.
    Ignores TEMPLATE place types entirely.
    """
    url = readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL") + "/places/"
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    ottoLogger.info("Otto API - Updating /Places/")

    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())
        writeTagValue("[Otto_FleetManager]System/lastResponse", response)

        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Places JSON decode error - {}".format(jsonErr))

            writeTagValueAsync("[Otto_FleetManager]Places/jsonString", response)

            basePath = "[Otto_FleetManager]Places"
            apiPlaces = []
            writes = []

            for place in data:
                normalizedPlace = normalizePlaceRecord(place)
                if normalizedPlace is None:
                    continue

                instanceName = normalizedPlace["instance_name"]
                apiPlaces.append(instanceName)
                instancePath = basePath + "/" + instanceName

                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = buildUdtInstanceDef(instanceName, "api_Place")
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created new place tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing place tag instance: " + instanceName)

                tagDict = {}
                for suffix, value in normalizedPlace["tag_values"].items():
                    tagDict[instancePath + suffix] = value

                writeTagValues(
                    list(tagDict.keys()),
                    list(tagDict.values())
                )
                writes.extend(tagDict.items())

                recipeValueWrites, recipeBoolWrites = buildPlaceRecipeWrites(
                    instancePath,
                    normalizedPlace["recipes"]
                )
                if recipeBoolWrites:
                    writeTagValuesAsync(
                        list(recipeBoolWrites.keys()),
                        list(recipeBoolWrites.values())
                    )
                    writes.extend(recipeBoolWrites.items())

                if recipeValueWrites:
                    writeTagValues(
                        list(recipeValueWrites.keys()),
                        list(recipeValueWrites.values())
                    )
                    writes.extend(recipeValueWrites.items())

            try:
                existingPlaces = listUdtInstanceNames(system.tag.browse(basePath).getResults())

                for placeName in existingPlaces:
                    if placeName not in apiPlaces:
                        instancePath = basePath + "/" + placeName
                        system.tag.deleteTags([instancePath])
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
            print("HTTP GET failed")
            return _buildSyncResult(False, "error", "HTTP GET failed for /Places/")

    except Exception as e:
        ottoLogger.error("Otto API - /Places/ Tag Update Failed - " + str(e))
        print("Otto API error occurred: {}".format(e))
        return _buildSyncResult(False, "error", "Places tag update failed - " + str(e))


def updateMaps():
    """
    Gets Map data from Otto and creates tags in [Otto_FleetManager]Maps/ for each map instance.
    Also determines the most recently modified map and stores its ID in ActiveMapID.
    Cleanup removes old map UDT instances but ignores the ActiveMapID memory tag.
    """
    url = readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL") + "/maps/?offset=0&tagged=false"
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    ottoLogger.info("Otto API - Updating /Maps/")

    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())
        writeTagValue("[Otto_FleetManager]Maps/updateResponse", response)

        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Maps JSON decode error - {}".format(jsonErr))

            writeTagValueAsync("[Otto_FleetManager]Maps/jsonString", response)

            basePath = "[Otto_FleetManager]Maps"
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

                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = buildUdtInstanceDef(instanceName, "api_Map")
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created new map tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing map tag instance: " + instanceName)

                writeTagValues(list(tagDict.keys()), list(tagDict.values()))
                writes.extend(tagDict.items())

            try:
                existingMaps = listUdtInstanceNames(system.tag.browse(basePath).getResults())

                for mapName in existingMaps:
                    if mapName not in apiMaps:
                        if mapName == "ActiveMapID":
                            continue

                        instancePath = basePath + "/" + mapName
                        system.tag.deleteTags([instancePath])
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
            print("HTTP GET failed")
            return _buildSyncResult(False, "error", "HTTP GET failed for /Maps/")

    except Exception as e:
        ottoLogger.error("Otto API - /Maps/ Tag Update Failed - " + str(e))
        print("Otto API error occurred: {}".format(e))
        return _buildSyncResult(False, "error", "Maps tag update failed - " + str(e))


def updateWorkflows():
    """
    Gets Workflows (called Mission Templates in the API documentation) from Otto
    and creates tags in /Workflows/ for each one.
    The full mission JSON (including tasks) is stored in jsonString for later reconstruction.
    """
    baseUrl = (
        readRequiredTagValue("[Otto_FleetManager]Url_ApiBase", "API base URL")
        + "/maps/mission_templates/?offset=0&map="
    )
    mapUuid = readRequiredTagValue("[Otto_FleetManager]Maps/ActiveMapID", "Active map ID")
    url = baseUrl + str(mapUuid)
    responseTag = "[Otto_FleetManager]System/lastResponse"
    basePath    = "[Otto_FleetManager]Workflows"
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    ottoLogger.info("Otto API - Updating /Workflows/")
    try:
        response = httpGet(url=url, headerValues=_jsonHeaders())
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

                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = buildUdtInstanceDef(instanceName, "api_Mission")
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created Workflow: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating Workflow: " + instanceName)

                writeTagValues(
                    list(missionDict.keys()),
                    list(missionDict.values())
                )
                writes.extend(missionDict.items())

            try:
                existingTemplates = listUdtInstanceNames(system.tag.browse(basePath).getResults())

                for tmplName in existingTemplates:
                    if tmplName not in apiTemplates:
                        system.tag.deleteTags([basePath + "/" + tmplName])
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
