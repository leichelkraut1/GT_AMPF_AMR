import json
import calendar
import re
import time
from Otto_API.HttpHelpers import httpGet
from Otto_API.HttpHelpers import jsonHeaders
from Otto_API.ResultHelpers import buildOperationResult
from Otto_API.TagHelpers import readOptionalTagValue
from Otto_API.TagHelpers import readRequiredTagValue


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

    Supported examples:
    - 2024-01-01T00:00:00Z
    - 2024-01-01T00:00:00.123Z
    - 2024-01-01T00:00:00+00:00
    - 2024-01-01T00:00:00.123-05:00
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

    Rules:
    - lower numeric priority wins
    - newer created timestamp wins on a priority tie
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
    system.tag.writeBlocking(paths, values)
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

    Some OTTO responses use fractional values like 0.76 while others use
    whole percentages like 76 or 88. This helper standardizes both forms.
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


def listUdtInstanceNames(browseResults):
    """
    Return the names of browsed UDT instances only.
    """
    names = []
    for row in list(browseResults or []):
        if str(row.get("tagType")) == "UdtInstance":
            names.append(row.get("name"))
    return names


def buildUdtInstanceDef(instanceName, typeId):
    return {
        "name": instanceName,
        "typeID": typeId,
        "tagType": "UdtInstance"
    }


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


def normalizePlaceRecord(placeRecord):
    """
    Normalize a place record and skip TEMPLATE entries.
    """
    if placeRecord.get("place_type") == "TEMPLATE":
        return None

    instanceName = placeRecord.get("name")
    if not instanceName:
        return None

    recipes = placeRecord.get("recipes", {})
    if not isinstance(recipes, dict):
        recipes = {}

    return {
        "instance_name": instanceName,
        "recipes": recipes,
        "tag_values": {
            "/Container_Types_Supported": placeRecord.get("container_types_supported"),
            "/Created": placeRecord.get("created"),
            "/Description": placeRecord.get("description"),
            "/Enabled": placeRecord.get("enabled"),
            "/Exit_Recipe": placeRecord.get("exit_recipe"),
            "/Feature_Queue": placeRecord.get("feature_queue"),
            "/ID": placeRecord.get("id"),
            "/Metadata": placeRecord.get("metadata"),
            "/Name": placeRecord.get("name"),
            "/Ownership_Queue": placeRecord.get("ownership_queue"),
            "/Place_Groups": placeRecord.get("place_groups"),
            "/Place_Type": placeRecord.get("place_type"),
            "/Primary_Marker_ID": placeRecord.get("primary_marker_id"),
            "/Primary_Marker_Intent": placeRecord.get("primary_marker_intent"),
            "/Source_ID": placeRecord.get("source_id"),
            "/Zone": placeRecord.get("zone"),
        }
    }


def buildPlaceRecipeWrites(instancePath, recipes):
    """
    Build value and enabled writes for place recipes.
    """
    valueWrites = {}
    boolWrites = {}

    for recipeName, recipeValue in dict(recipes or {}).items():
        valueWrites["{}/recipes/{}/Value".format(instancePath, recipeName)] = recipeValue
        boolWrites["{}/recipes/{}/Able".format(instancePath, recipeName)] = (
            1 if recipeValue is not None else 0
        )

    return valueWrites, boolWrites


def buildMapInstanceName(mapItem):
    """
    Build the Ignition instance name for a map record.
    """
    return "{}_{}".format(
        sanitizeTagName(mapItem.get("name")),
        mapItem.get("revision")
    )


def selectMostRecentMap(mapItems):
    """
    Select the map with the newest last_modified timestamp.
    """
    items = list(mapItems or [])
    if not items:
        return None

    return sorted(
        items,
        key=lambda item: str(item.get("last_modified", "1970-01-01T00:00:00Z")),
        reverse=True
    )[0]


def buildMapTagValues(basePath, mapItem):
    """
    Build the tag value map for a map record.
    """
    instanceName = buildMapInstanceName(mapItem)
    instancePath = basePath + "/" + instanceName

    return instanceName, {
        instancePath + "/ID": mapItem.get("id"),
        instancePath + "/Last_Modified": mapItem.get("last_modified"),
        instancePath + "/Created": mapItem.get("created"),
        instancePath + "/Name": mapItem.get("name"),
        instancePath + "/Description": mapItem.get("description"),
        instancePath + "/Project": mapItem.get("project"),
        instancePath + "/Tag": mapItem.get("tag"),
        instancePath + "/Cached": mapItem.get("cached"),
        instancePath + "/Disabled": mapItem.get("disabled"),
        instancePath + "/User_ID": mapItem.get("user_id"),
        instancePath + "/Author": mapItem.get("author"),
        instancePath + "/Revision": mapItem.get("revision"),
        instancePath + "/Tag_Index": mapItem.get("tag_index"),
        instancePath + "/Source_Map": mapItem.get("source_map")
    }


def buildWorkflowTagValues(basePath, templateItem):
    """
    Build the tag value map for a workflow / mission template record.
    """
    instanceName = templateItem.get("name")
    if not instanceName:
        return None, {}

    instancePath = basePath + "/" + instanceName
    return instanceName, {
        instancePath + "/ID": templateItem.get("id"),
        instancePath + "/Description": templateItem.get("description", ""),
        instancePath + "/Priority": templateItem.get("priority", 0),
        instancePath + "/NominalDuration": templateItem.get("nominal_duration"),
        instancePath + "/MaxDuration": templateItem.get("max_duration"),
        instancePath + "/RobotTeam": templateItem.get("robot_team"),
        instancePath + "/OverridePrompts": templateItem.get("override_prompts_json"),
        instancePath + "/jsonString": json.dumps(templateItem)
    }

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
            system.tag.writeAsync("[Otto_FleetManager]System/ServerStatus", status)
            return _buildSyncResult(True, "info", "Server status updated", data=status)

        ottoLogger.warn("Otto Fleet Manager Did Not Respond to Status Update Request")
        system.tag.writeAsync("[Otto_FleetManager]System/ServerStatus", "ReponseError")
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
            system.tag.write("[Otto_FleetManager]System/lastResponse", response)

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

                system.tag.writeBlocking(list(tagValues.keys()), list(tagValues.values()))
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
                system.tag.writeBlocking(paths, values)
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
                system.tag.writeBlocking([path], [value])
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
                system.tag.writeBlocking(
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
        system.tag.write("[Otto_FleetManager]System/lastResponse", response)

        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Places JSON decode error - {}".format(jsonErr))

            system.tag.writeAsync("[Otto_FleetManager]Places/jsonString", response)

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

                system.tag.writeBlocking(
                    list(tagDict.keys()),
                    list(tagDict.values())
                )
                writes.extend(tagDict.items())

                recipeValueWrites, recipeBoolWrites = buildPlaceRecipeWrites(
                    instancePath,
                    normalizedPlace["recipes"]
                )
                if recipeBoolWrites:
                    system.tag.writeAsync(
                        list(recipeBoolWrites.keys()),
                        list(recipeBoolWrites.values())
                    )
                    writes.extend(recipeBoolWrites.items())

                if recipeValueWrites:
                    system.tag.writeBlocking(
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
        system.tag.write("[Otto_FleetManager]Maps/updateResponse", response)

        if response:
            try:
                data = parseListPayload(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return _buildSyncResult(False, "error", "Maps JSON decode error - {}".format(jsonErr))

            system.tag.writeAsync("[Otto_FleetManager]Maps/jsonString", response)

            basePath = "[Otto_FleetManager]Maps"
            activeMapTag = basePath + "/ActiveMapID"
            apiMaps = []
            writes = []
            activeMapId = None

            try:
                mostRecent = selectMostRecentMap(data)
                if mostRecent is not None:
                    activeMapId = mostRecent.get("id")
                    system.tag.write(activeMapTag, activeMapId)
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

                system.tag.writeBlocking(list(tagDict.keys()), list(tagDict.values()))
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
        system.tag.writeAsync(responseTag, response)
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

                system.tag.writeBlocking(
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


def sanitizeTagName(text):
    """Convert mission/tag names into a safe Ignition tag name."""
    if text is None:
        return "None"
    return (
        str(text)
        .replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
        .replace(".", "_")
    )
