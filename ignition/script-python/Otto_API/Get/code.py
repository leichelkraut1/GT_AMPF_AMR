import json
import time
import uuid

def getServerStatus():
	"""
	Gets Fleet Manager server state
	"""
	#--- Config ---
	url = system.tag.read("[Otto_FleetManager]Url_ApiBase").value + "/system/state/"
	headers = {"Accept": "application/json", "Content-Type": "application/json"}
	ottoLogger = system.util.getLogger("Otto_API_Logger")
	
	try:
		response = system.net.httpGet(url=url, bypassCertValidation=True, headerValues=headers)
		if response:
			data = json.loads(response)
			status = data.get("state", "Unknown")
			system.tag.writeAsync("[Otto_FleetManager]System/ServerStatus", status)
		else:
			ottoLogger.warn("Otto Fleet Manager Did Not Respond to Status Update Request")
			system.tag.writeAsync("[Otto_FleetManager]System/ServerStatus", "ReponseError")
	except Exception as e:
		ottoLogger.error("Otto API - Status Update Failed - " + str(e))


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

        base = system.tag.read("[Otto_FleetManager]Url_ApiBase").value

        url = (
            base
            + "/missions/?fields=%2A"
            + "&mission_status=" + mission_status
        )

        if limit is not None:
            url += "&limit=" + str(limit)

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if debug:
            logger.debug(
                "Otto API - Requesting missions status={} url={}".format(
                    mission_status, url
                )
            )

        response = system.net.httpGet(
            url,
            bypassCertValidation=True,
            headerValues=headers
        )

        if not response:
            return []

        data = json.loads(response)
        results = data.get("results", [])

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

    # --- Config ---
    url = system.tag.read("[Otto_FleetManager]Url_ApiBase").value + "/robots/?fields=id,hostname,name,serial_number"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    ottoLogger = system.util.getLogger("Otto_API_Logger")
    ottoLogger.info("Otto API - Updating /Robots/ Tags")

    try:
        response = system.net.httpGet(url=url, bypassCertValidation=True, headerValues=headers)

        if response:
            ottoLogger.info("Otto API - Updating /Robots/ - Response Received")
            system.tag.write("[Otto_FleetManager]System/lastResponse", response)

            try:
                data = json.loads(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return

            # --- Process Results into Tags ---
            basePath = "[Otto_FleetManager]Robots"
            apiRobots = [robot.get("name") for robot in data.get("results", [])]

            for robot in data.get("results", []):
                instanceName = robot["name"]
                instancePath = basePath + "/" + instanceName
                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = {
                        "name": instanceName,
                        "typeID": "api_Robot",
                        "tagType": "UdtInstance"
                    }
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created new robot tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing robot tag instance: " + instanceName)

                # Write values to tags
                tagDict = {
                    instancePath + "/Hostname": robot.get("hostname"),
                    instancePath + "/ID": robot.get("id"),
                    instancePath + "/SerialNum": robot.get("serial_number"),
                }

                system.tag.writeBlocking(list(tagDict.keys()), list(tagDict.values()))

            # --- Cleanup stale tags ---
            try:
                existingTags = system.tag.browse(basePath).getResults()

                existingRobots = []
                for tag in existingTags:
                    if str(tag.get("tagType")) == "UdtInstance":
                        existingRobots.append(tag.get("name"))
                    else:
                        ottoLogger.warn("Found non-UDT tag in Robots folder: {}".format(tag.get("name")))

                for robotName in existingRobots:
                    if robotName not in apiRobots:
                        instancePath = basePath + "/" + robotName
                        system.tag.deleteTags([instancePath])
                        ottoLogger.info("Otto API - Removed stale robot tag instance: " + robotName)

            except Exception as e:
                ottoLogger.warn("Otto API - Cleanup skipped due to error: " + str(e))

        else:
            ottoLogger.error("Otto API - HTTPGet Failed for /Robots/")

    except Exception as e:
        ottoLogger.error("Otto API - /Robots/ Tag Update Failed - " + str(e))


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

    # --- Config ---
    baseUrl = system.tag.read("[Otto_FleetManager]Url_ApiBase").value
    url = baseUrl + "/robots/states/?fields=%2A"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    robotsBasePath = "[Otto_FleetManager]Robots"

    try:
        response = system.net.httpGet(
            url=url,
            bypassCertValidation=True,
            headerValues=headers
        )

        if not response:
            ottoLogger.error(
                "Otto API - HTTP GET failed for /robots/system_states/"
            )
            return

        data = json.loads(response)
        results = data.get("results", [])

        # --- Group system states by robot UUID ---
        statesByRobot = {}

        for entry in results:
            robotUUID = entry.get("robot")
            if robotUUID is None:
                continue

            robotUUID = str(robotUUID).strip()
            statesByRobot.setdefault(robotUUID, []).append(entry)

        # --- Build robot UUID -> UDT path map ---
        robotTags = {}

        browseResults = system.tag.browse(robotsBasePath).getResults()
        for tag in browseResults:
            if str(tag["tagType"]) == "UdtInstance":
                robotPath = robotsBasePath + "/" + tag["name"]
                try:
                    robotID = system.tag.read(robotPath + "/ID").value
                    if robotID is not None:
                        robotTags[str(robotID).strip()] = robotPath
                except Exception as e:
                    ottoLogger.warn(
                        "Failed to read ID for robot " +
                        robotPath + " - " + str(e)
                    )

        # --- Resolve dominant state per robot ---
        for robotUUID, stateList in statesByRobot.items():
            if robotUUID not in robotTags:
                ottoLogger.warn(
                    "SystemState received for unknown robot UUID " +
                    robotUUID
                )
                continue

            def sortKey(entry):
                """
                Sort rules:
                  1) LOWER priority number wins
                  2) Newer 'created' timestamp wins on tie
                """
                priority = entry.get("priority", 9999)
                created = entry.get("created")

                try:
                    ts = OffsetDateTime.parse(
                        created
                    ).toInstant().toEpochMilli()
                except:
                    ts = 0

                return (priority, -ts)

            dominant = sorted(stateList, key=sortKey)[0]

            robotPath = robotTags[robotUUID]

            try:
                system.tag.writeBlocking(
                    [
                        robotPath + "/SystemState",
                        robotPath + "/SubSystemState",
                        robotPath + "/SystemStatePriority",
                        robotPath + "/SystemStateUpdatedTs"
                    ],
                    [
                        dominant.get("system_state"),
                        dominant.get("sub_system_state"),
                        dominant.get("priority"),
                        system.date.now()
                    ]
                )
            except Exception as e:
                ottoLogger.warn(
                    "Failed to write SystemState for robot " +
                    robotUUID + " - " + str(e)
                )

    except Exception as e:
        ottoLogger.error(
            "Otto API - Failed to update system states - " + str(e)
        )


def updateChargeLevels():
    """
    Updates the .ChargeLevel tag for all vehicles in [Otto_FleetManager]Robots
    by retrieving battery percentages from the API and matching by robot UUID.
    """
    #--- Config ---
    baseUrl = system.tag.read("[Otto_FleetManager]Url_ApiBase").value
    url = baseUrl + "/robots/batteries/?fields=percentage,robot"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    ottoLogger = system.util.getLogger("Otto_API_Logger")
    
    try:
        response = system.net.httpGet(url=url, bypassCertValidation=True, headerValues=headers)
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/batteries/")
            return

        batteryData = json.loads(response)

        # Browse all robot UDT instances
        basePath = "[Otto_FleetManager]Robots"
        allTags = system.tag.browse(basePath).getResults()

        # Build mapping: robot UUID -> UDT tag path
        robotTags = {}
        for tag in allTags:
            if str(tag["tagType"]) == "UdtInstance":
                robotPath = basePath + "/" + tag["name"]
                try:
                    robotID = system.tag.read(robotPath + "/ID").value
                    if robotID is not None:
                        robotID = str(robotID).strip()
                        robotTags[robotID] = robotPath
                except Exception as e:
                    ottoLogger.warn("Failed to read .ID for robot tag: " + robotPath + " - " + str(e))
            else:
            	ottoLogger.warn("Found a non-UDT tag in [Otto_FleetManager]Robots")

        # Update .ChargeLevel for each battery entry
        for battery in batteryData.get("results", []):
            batteryRobotID = battery.get("robot")
            chargeLevel = battery.get("percentage", 0)
            if batteryRobotID is None:
                continue

            batteryRobotID = str(batteryRobotID).strip()

            if batteryRobotID in robotTags:
                chargeTagPath = robotTags[batteryRobotID] + "/ChargeLevel"
                try:
                    system.tag.writeBlocking([chargeTagPath], [chargeLevel])
                except Exception as e:
                    ottoLogger.warn("Failed to write ChargeLevel for " + batteryRobotID + " - " + str(e))
            else:
                ottoLogger.warn("No matching robot tag found for robot ID " + batteryRobotID)

    except Exception as e:
        ottoLogger.error("Otto API - Failed to update charge levels - " + str(e))
        
        
def updateActivityStates():
    """
    Updates the .ActivityState tag for all vehicles in [Otto_FleetManager]Robots
    by retrieving activity states from the API and matching by robot UUID.
    """
    # --- Config ---
    baseUrl = system.tag.read("[Otto_FleetManager]Url_ApiBase").value
    url = baseUrl + "/robots/activities/?fields=activity,robot&offset=0&limit=100"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    try:
        response = system.net.httpGet(
            url=url,
            bypassCertValidation=True,
            headerValues=headers
        )
        if not response:
            ottoLogger.error("Otto API - HTTP GET failed for /robots/activities/")
            return

        activityData = json.loads(response)

        # Browse all robot UDT instances
        basePath = "[Otto_FleetManager]Robots"
        allTags = system.tag.browse(basePath).getResults()

        # Build mapping: robot UUID -> UDT tag path
        robotTags = {}
        for tag in allTags:
            if str(tag["tagType"]) == "UdtInstance":
                robotPath = basePath + "/" + tag["name"]
                try:
                    robotID = system.tag.read(robotPath + "/ID").value
                    if robotID is not None:
                        robotID = str(robotID).strip()
                        robotTags[robotID] = robotPath
                except Exception as e:
                    ottoLogger.warn(
                        "Failed to read .ID for robot tag: " +
                        robotPath + " - " + str(e)
                    )
            else:
                ottoLogger.warn(
                    "Found a non-UDT tag in [Otto_FleetManager]Robots"
                )

        # Update .ActivityState for each activity entry
        for entry in activityData.get("results", []):
            activityRobotID = entry.get("robot")
            activityState = entry.get("activity")

            if activityRobotID is None:
                continue

            activityRobotID = str(activityRobotID).strip()

            if activityRobotID in robotTags:
                activityTagPath = robotTags[activityRobotID] + "/ActivityState"
                try:
                    system.tag.writeBlocking(
                        [activityTagPath],
                        [activityState]
                    )
                except Exception as e:
                    ottoLogger.warn(
                        "Failed to write ActivityState for " +
                        activityRobotID + " - " + str(e)
                    )
            else:
                ottoLogger.warn(
                    "No matching robot tag found for robot ID " +
                    activityRobotID
                )

    except Exception as e:
        ottoLogger.error(
            "Otto API - Failed to update activity states - " + str(e)
        )


def updatePlaces():
    """
    Gets endpoint information from Otto and creates tags for each endpoint in [Otto_FleetManager]Places.
    Also removes UDT instances that no longer exist in the API response.
    Ignores TEMPLATE place types entirely.
    """
    #--- Config ---
    url = system.tag.read("[Otto_FleetManager]Url_ApiBase").value + "/places/"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    ottoLogger = system.util.getLogger("Otto_API_Logger")
    
    ottoLogger.info("Otto API - Updating /Places/")
    
    try:
        response = system.net.httpGet(url=url, bypassCertValidation=True, headerValues=headers)
        system.tag.write("[Otto_FleetManager]System/lastResponse", response)
        
        if response:
            try:
                data = json.loads(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return

            system.tag.writeAsync("[Otto_FleetManager]Places/jsonString", response)

            # Create Tag Paths
            basePath = "[Otto_FleetManager]Places"
            udtType = "api_Place"

            # Only include non-TEMPLATE places
            apiPlaces = [
                place["name"]
                for place in data
                if place.get("place_type") != "TEMPLATE"
            ]

            # Process each valid place
            for place in data:

                # Skip TEMPLATE places
                if place.get("place_type") == "TEMPLATE":
                    continue

                instanceName = place["name"]
                instancePath = basePath + "/" + instanceName

                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = {
                        "name": instanceName,
                        "typeID": udtType,
                        "tagType": "UdtInstance"
                    }
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created new place tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing place tag instance: " + instanceName)

                # Move values into tags
                tagDict = {
                    instancePath + "/Container_Types_Supported": place.get("container_types_supported"),
                    instancePath + "/Created": place.get("created"),
                    instancePath + "/Description": place.get("description"),
                    instancePath + "/Enabled": place.get("enabled"),
                    instancePath + "/Exit_Recipe": place.get("exit_recipe"),
                    instancePath + "/Feature_Queue": place.get("feature_queue"),
                    instancePath + "/ID": place.get("id"),
                    instancePath + "/Metadata": place.get("metadata"),
                    instancePath + "/Name": place.get("name"),
                    instancePath + "/Ownership_Queue": place.get("ownership_queue"),
                    instancePath + "/Place_Groups": place.get("place_groups"),
                    instancePath + "/Place_Type": place.get("place_type"),
                    instancePath + "/Primary_Marker_ID": place.get("primary_marker_id"),
                    instancePath + "/Primary_Marker_Intent": place.get("primary_marker_intent"),
                    instancePath + "/Source_ID": place.get("source_id"),
                    instancePath + "/Zone": place.get("zone"),
                }

                system.tag.writeBlocking(
                    list(tagDict.keys()),
                    list(tagDict.values())
                )

                # Handle recipes; Recipes have Task data per Endpoint
                recipes = place.get("recipes", {})

                recipeDict = {}
                for recipeName, recipeValue in recipes.items():
                    valuePath = "{}/recipes/{}/Value".format(instancePath, recipeName)
                    boolPath = "{}/recipes/{}/Able".format(instancePath, recipeName)
                    recipeDict[valuePath] = recipeValue
                    system.tag.writeAsync(boolPath, 1 if recipeValue is not None else 0)

                if recipeDict:
                    system.tag.writeBlocking(
                        list(recipeDict.keys()),
                        list(recipeDict.values())
                    )

            # Cleanup stale place tags
            try:
                existingTags = system.tag.browse(basePath).getResults()

                existingPlaces = [
                    t.get("name")
                    for t in existingTags
                    if str(t.get("tagType")) == "UdtInstance"
                ]

                for placeName in existingPlaces:
                    if placeName not in apiPlaces:
                        instancePath = basePath + "/" + placeName
                        system.tag.deleteTags([instancePath])
                        ottoLogger.info("Otto API - Removed stale place tag instance: " + placeName)

            except Exception as e:
                ottoLogger.warn("Otto API - Cleanup skipped due to error: " + str(e))

        else:
            ottoLogger.error("Otto API - HTTPGet Failed for /Places/")
            print("HTTP GET failed")

    except Exception as e:
        ottoLogger.error("Otto API - /Places/ Tag Update Failed - " + str(e))
        print("Otto API error occurred: {}".format(e))


def updateMaps():
    """
    Gets Map data from Otto and creates tags in [Otto_FleetManager]Maps/ for each map instance.
    Also determines the most recently modified map and stores its ID in ActiveMapID.
    Cleanup removes old map UDT instances but ignores the ActiveMapID memory tag.
    """
    #--- Config ---
    url = system.tag.read("[Otto_FleetManager]Url_ApiBase").value + "/maps/?offset=0&tagged=false"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    ottoLogger.info("Otto API - Updating /Maps/")

    try:
        response = system.net.httpGet(url=url, bypassCertValidation=True, headerValues=headers)
        system.tag.write("[Otto_FleetManager]Maps/updateResponse", response)
        
        if response:
            try:
                data = json.loads(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - JSON decode error: {}".format(jsonErr))
                return

            system.tag.writeAsync("[Otto_FleetManager]Maps/jsonString", response)

            basePath = "[Otto_FleetManager]Maps"
            udtType = "api_Map"
            activeMapTag = basePath + "/ActiveMapID"

            #--- Build list of map instance names ---
            apiMaps = [
                "{}_{}".format(sanitizeTagName(m.get("name")), m.get("revision"))
                for m in data
            ]

            # --- Determine Most Recently Modified Map ---
            try:
                #--- Sortable timestamps; if missing, treat as old ---
                sortedMaps = sorted(
                    data,
                    key=lambda m: m.get("last_modified", "1970-01-01T00:00:00Z"),
                    reverse=True
                )

                mostRecent = sortedMaps[0]
                mostRecentID = mostRecent.get("id")
                system.tag.write(activeMapTag, mostRecentID)
                ottoLogger.info("Otto API - ActiveMapID updated to: " + str(mostRecentID))

            except Exception as sortErr:
                ottoLogger.warn("Otto API - Failed to determine most recent map: " + str(sortErr))

            # --- Process Each Map ---
            for mapItem in data:
                name_part = sanitizeTagName(mapItem.get("name"))
                rev_part = mapItem.get("revision")
                instanceName = "{}_{}".format(name_part, rev_part)
                instancePath = basePath + "/" + instanceName

                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = {"name": instanceName, "typeID": udtType, "tagType": "UdtInstance"}
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created new map tag instance: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating existing map tag instance: " + instanceName)

                #--- Move values into tags ---
                tagDict = {
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

                system.tag.writeBlocking(list(tagDict.keys()), list(tagDict.values()))

            # --- Cleanup Old Map Tags ---
            try:
                existingTags = system.tag.browse(basePath).getResults()

                existingMaps = [
                    t.get("name")
                    for t in existingTags
                    if str(t.get("tagType")) == "UdtInstance"
                ]

                for mapName in existingMaps:
                    if mapName not in apiMaps:
                        # Do NOT delete the ActiveMapID tag
                        if mapName == "ActiveMapID":
                            continue

                        instancePath = basePath + "/" + mapName
                        system.tag.deleteTags([instancePath])
                        ottoLogger.info("Otto API - Removed stale map tag instance: " + mapName)

            except Exception as e:
                ottoLogger.warn("Otto API - Cleanup skipped due to error: " + str(e))

        else:
            ottoLogger.error("Otto API - HTTPGet Failed for /Maps/")
            print("HTTP GET failed")

    except Exception as e:
        ottoLogger.error("Otto API - /Maps/ Tag Update Failed - " + str(e))
        print("Otto API error occurred: {}".format(e))


def updateWorkflows():
    """
    Gets Workflows (called Mission Templates in the API documentation) from Otto and creates tags in /Workflows/ for each one.
    The full mission JSON (including tasks) is stored in jsonString for later reconstruction.
    """

    # --- Config ---
    baseUrl = system.tag.read("[Otto_FleetManager]Url_ApiBase").value + "/maps/mission_templates/?offset=0&map="
    mapUuid = system.tag.read("[Otto_FleetManager]Maps/ActiveMapID").value
    url = baseUrl + str(mapUuid)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    responseTag = "[Otto_FleetManager]System/lastResponse"
    basePath    = "[Otto_FleetManager]Workflows"
    ottoLogger = system.util.getLogger("Otto_API_Logger")

    ottoLogger.info("Otto API - Updating /Workflows/")
    try:
        response = system.net.httpGet(url=url, bypassCertValidation=True, headerValues=headers)
        system.tag.writeAsync(responseTag, response)
        if response:
            try:
                data = json.loads(response)
            except Exception as jsonErr:
                ottoLogger.error("Otto API - Mission templates JSON decode error: {}".format(jsonErr))
                return

            # List of all template names from API (used for cleanup)
            apiTemplates = [tmpl.get("name") for tmpl in data]

            # --- Create/update all mission template UDTs ---
            for tmpl in data:
                instanceName = tmpl.get("name")
                instancePath = basePath + "/" + instanceName

                exists = system.tag.exists(instancePath)

                if not exists:
                    tagDef = {
                        "name": instanceName,
                        "typeID": "api_Mission",
                        "tagType": "UdtInstance"
                    }
                    system.tag.configure(basePath, [tagDef], "a")
                    ottoLogger.info("Otto API - Created Workflow: " + instanceName)
                else:
                    ottoLogger.info("Otto API - Updating Workflow: " + instanceName)

                # Write mission template values
                missionDict = {
                    instancePath + "/ID":              tmpl.get("id"),
                    instancePath + "/Description":     tmpl.get("description", ""),
                    instancePath + "/Priority":        tmpl.get("priority", 0),
                    instancePath + "/NominalDuration": tmpl.get("nominal_duration"),
                    instancePath + "/MaxDuration":     tmpl.get("max_duration"),
                    instancePath + "/RobotTeam":       tmpl.get("robot_team"),
                    instancePath + "/OverridePrompts": tmpl.get("override_prompts_json"),
                    instancePath + "/jsonString":      json.dumps(tmpl)  # full mission JSON
                }

                system.tag.writeBlocking(
                    list(missionDict.keys()),
                    list(missionDict.values())
                )

            # --- Cleanup stale mission templates ---
            try:
                existingTags = system.tag.browse(basePath).getResults()

                # Your gateway returns dictionaries as browse results
                existingTemplates = [
                    t.get("name")
                    for t in existingTags
                    if str(t.get("tagType")) == "UdtInstance"
                ]

                for tmplName in existingTemplates:
                    if tmplName not in apiTemplates:
                        system.tag.deleteTags([basePath + "/" + tmplName])
                        ottoLogger.info("Otto API - Removed stale workflow: " + tmplName)

            except Exception as e:
                ottoLogger.warn("Otto API - Workflow cleanup skipped: {}".format(str(e)))

        else:
            ottoLogger.error("Otto API - HTTP GET failed for /Workflows/")

    except Exception as e:
        ottoLogger.error("Otto API - Workflows tag update failed: {}".format(str(e)))


def sanitizeTagName(text):
    # Convert mission or tag names into valid Ignition tag names
    if text is None:
        return "None"
    return (
        str(text)
        .replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
        .replace(".", "_")
    )