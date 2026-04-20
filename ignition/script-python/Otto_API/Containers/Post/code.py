import uuid

from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import getFleetContainersVerboseCleanupLoggingPath
from Otto_API.Common.TagHelpers import getOttoOperationsUrl
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValues
from Otto_API.Common.TagHelpers import writeLastSystemResponse
from Otto_API.Common.TagHelpers import writeLastTriggerResponse
from Otto_API.Fleet.ContentSync import sanitizeTagName
from Otto_API.Containers.Actions import buildCreateContainerPayload
from Otto_API.Containers.Actions import buildDeleteContainerPayload
from Otto_API.Containers.Actions import buildUpdateContainerPlacePayload
from Otto_API.Containers.Actions import buildUpdateContainerRobotPayload
from Otto_API.Containers.Actions import interpretCreateContainerResponse
from Otto_API.Containers.Actions import interpretDeleteContainerResponse
from Otto_API.Containers.Actions import interpretUpdateContainerPlaceResponse
from Otto_API.Containers.Actions import interpretUpdateContainerRobotResponse


def _log():
    return system.util.getLogger("Otto_API.Containers.Post")


def _buildResult(
    ok,
    level,
    message,
    containerId=None,
    placeId=None,
    robotId=None,
    responseText=None,
    payload=None,
    extraData=None,
):
    extraData = dict(extraData or {})
    data = {
        "container_id": containerId,
        "place_id": placeId,
        "robot_id": robotId,
        "response_text": responseText,
        "payload": payload,
    }
    data.update(extraData)

    return buildOperationResult(
        ok,
        level,
        message,
        data=data,
        container_id=containerId,
        place_id=placeId,
        robot_id=robotId,
        response_text=responseText,
        payload=payload,
        **extraData
    )


def _writeResponseAndLogResult(result, logger):
    if result["response_text"] is not None:
        writeLastSystemResponse(result["response_text"], asyncWrite=True)

    if result["level"] == "info":
        logger.info(result["message"])
    elif result["level"] == "warn":
        logger.warn(result["message"])
    else:
        logger.error(result["message"])

    writeLastTriggerResponse(result["message"], asyncWrite=True)
    return result


def _runCreateFromTagPath(containerTagPath, targetLabel, targetId, createFunc):
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Creating container from tag path [{}] at {} [{}]".format(containerTagPath, targetLabel, targetId))

    try:
        containerFields = _readCreateContainerBaseFields(containerTagPath)
    except Exception as e:
        msg = "Error reading container fields from [{}]: {}".format(containerTagPath, str(e))
        ottoLogger.error(msg)
        writeLastTriggerResponse(msg, asyncWrite=True)
        return _buildResult(False, "error", msg)

    result = createFunc(containerFields, targetId, fleetManagerURL, httpPost)
    return _writeResponseAndLogResult(result, ottoLogger)


def _runDirectOperation(logMessage, operationFunc, *args):
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info(logMessage)
    result = operationFunc(*(args + (fleetManagerURL, httpPost)))
    return _writeResponseAndLogResult(result, ottoLogger)


def _isVerboseCleanupLoggingEnabled():
    return bool(
        readOptionalTagValue(
            getFleetContainersVerboseCleanupLoggingPath(),
            False,
            allowEmptyString=False
        )
    )


def _hasLocationValue(value):
    """
    Return True when a location field is meaningfully populated.
    """
    if value is None:
        return False

    text = str(value).strip()
    if not text:
        return False

    if text.lower() in ["none", "null"]:
        return False

    return True


def _browseResultValue(browseResult, key, defaultValue=None):
    """
    Read one browse-result field across dict, Ignition browse rows, and shim rows.
    """
    if browseResult is None:
        return defaultValue

    if isinstance(browseResult, dict):
        return browseResult.get(key, defaultValue)

    getter = getattr(browseResult, "get", None)
    if getter is not None:
        try:
            return getter(key)
        except TypeError:
            try:
                return getter(key, defaultValue)
            except Exception:
                pass
        except Exception:
            pass

    return getattr(browseResult, key, defaultValue)


def _iterContainerInstancePaths(containersBase):
    """
    Yield browsed container UDT instance paths under Fleet/Containers.
    """
    for browseResult in browseTagResults(containersBase):
        instancePath = str(_browseResultValue(browseResult, "fullPath", "") or "")
        tagType = _browseResultValue(browseResult, "tagType", None)

        if not instancePath:
            continue

        tagTypeText = str(tagType or "").strip().lower()
        if "udtinstance" not in tagTypeText:
            continue

        yield instancePath


def _buildContainerCreateId(containerTagPath, uuidFactory=None):
    """
    Build a generated container id as a UUID string.
    """
    if uuidFactory is None:
        uuidFactory = uuid.uuid4

    return str(uuidFactory())


def _readCreateContainerBaseFields(containerTagPath, uuidFactory=None):
    """
    Read the common createContainer fields from one container UDT instance path.
    """
    requiredFieldSpecs = [
        ("container_type", containerTagPath + "/ContainerType", "Container type"),
        ("empty", containerTagPath + "/Empty", "Container empty"),
    ]
    requiredValues = readRequiredTagValues(
        [path for _, path, _ in requiredFieldSpecs],
        labels=[label for _, _, label in requiredFieldSpecs],
        allowEmptyString=False,
    )

    containerFields = {
        "id": _buildContainerCreateId(containerTagPath, uuidFactory),
    }
    for (fieldName, _path, _label), value in zip(requiredFieldSpecs, requiredValues):
        containerFields[fieldName] = value

    optionalFieldSpecs = [
        ("description", containerTagPath + "/Description"),
        ("name", containerTagPath + "/Name"),
    ]
    for fieldName, path in optionalFieldSpecs:
        value = readOptionalTagValue(path, None, allowEmptyString=True)
        if value in [None, ""]:
            continue
        containerFields[fieldName] = value
    return containerFields


def createContainerFromInputs(containerFields, fleetManagerURL, postFunc):
    """
    Create one container from explicit fields and return a structured result.
    """
    if not containerFields:
        return _buildResult(False, "warn", "No container fields supplied for create")

    try:
        payload = buildCreateContainerPayload(containerFields)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message, containerId = interpretCreateContainerResponse(response)
        return _buildResult(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            containerId=containerId,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _buildResult(
            False,
            "error",
            "Error creating container: {}".format(str(e)),
        )


def createContainerAtPlaceFromInputs(containerFields, placeId, fleetManagerURL, postFunc):
    """
    Create one container at an explicit place and return a structured result.
    """
    if not placeId:
        return _buildResult(False, "warn", "No place id supplied for container create")

    containerFields = dict(containerFields or {})
    containerFields.pop("robot", None)
    containerFields["place"] = placeId
    return createContainerFromInputs(containerFields, fleetManagerURL, postFunc)


def createContainerAtRobotFromInputs(containerFields, robotId, fleetManagerURL, postFunc):
    """
    Create one container at an explicit robot and return a structured result.
    """
    if not robotId:
        return _buildResult(False, "warn", "No robot id supplied for container create")

    containerFields = dict(containerFields or {})
    containerFields.pop("place", None)
    containerFields["robot"] = robotId
    return createContainerFromInputs(containerFields, fleetManagerURL, postFunc)


def updateContainerPlaceByIdFromInputs(containerId, placeId, fleetManagerURL, postFunc):
    """
    Update one container's place using explicit inputs and return a structured result.
    """
    if not containerId:
        return _buildResult(False, "warn", "No container id supplied for update", placeId=placeId)
    if not placeId:
        return _buildResult(False, "warn", "No place id supplied for container update", containerId=containerId)

    try:
        payload = buildUpdateContainerPlacePayload(containerId, placeId)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message = interpretUpdateContainerPlaceResponse(response, containerId, placeId)
        return _buildResult(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            containerId=containerId,
            placeId=placeId,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _buildResult(
            False,
            "error",
            "Error updating container [{}] place to [{}]: {}".format(containerId, placeId, str(e)),
            containerId=containerId,
            placeId=placeId,
        )


def updateContainerRobotByIdFromInputs(containerId, robotId, fleetManagerURL, postFunc):
    """
    Update one container's robot using explicit inputs and return a structured result.
    """
    if not containerId:
        return _buildResult(False, "warn", "No container id supplied for update", robotId=robotId)
    if not robotId:
        return _buildResult(False, "warn", "No robot id supplied for container update", containerId=containerId)

    try:
        payload = buildUpdateContainerRobotPayload(containerId, robotId)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message = interpretUpdateContainerRobotResponse(response, containerId, robotId)
        return _buildResult(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            containerId=containerId,
            robotId=robotId,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _buildResult(
            False,
            "error",
            "Error updating container [{}] robot to [{}]: {}".format(containerId, robotId, str(e)),
            containerId=containerId,
            robotId=robotId,
        )


def deleteContainerByIdFromInputs(containerId, fleetManagerURL, postFunc):
    """
    Delete one container by explicit inputs and return a structured result.
    """
    if not containerId:
        return _buildResult(False, "warn", "No container id supplied for delete")

    try:
        payload = buildDeleteContainerPayload(containerId)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message = interpretDeleteContainerResponse(response, containerId)
        return _buildResult(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            containerId=containerId,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _buildResult(
            False,
            "error",
            "Error deleting container [{}]: {}".format(containerId, str(e)),
            containerId=containerId,
        )


def deleteContainersAtPlaceFromInputs(placeId, fleetManagerURL, postFunc):
    """
    Delete every container currently assigned to the provided place id.
    """
    if not placeId:
        return _buildResult(False, "warn", "No place id supplied for delete-at-place")

    containersBase = getFleetContainersPath()
    matchedContainerIds = []

    for instancePath in _iterContainerInstancePaths(containersBase):
        if readOptionalTagValue(instancePath + "/Place", None) != placeId:
            continue

        containerId = readOptionalTagValue(instancePath + "/ID", None)
        if not containerId:
            continue
        matchedContainerIds.append(containerId)

    if not matchedContainerIds:
        return _buildResult(
            False,
            "warn",
            "No containers found at place [{}]".format(placeId),
            placeId=placeId,
            extraData={
                "matched_container_ids": [],
                "deleted_container_ids": [],
                "delete_results": [],
            },
        )

    deleteResults = []
    deletedContainerIds = []
    allSucceeded = True
    for containerId in matchedContainerIds:
        result = deleteContainerByIdFromInputs(containerId, fleetManagerURL, postFunc)
        deleteResults.append(result)
        if result["ok"]:
            deletedContainerIds.append(containerId)
        else:
            allSucceeded = False

    if allSucceeded:
        level = "info"
        message = "Deleted {} container(s) at place [{}]".format(len(deletedContainerIds), placeId)
    else:
        level = "warn"
        message = "Deleted {} of {} container(s) at place [{}]".format(
            len(deletedContainerIds),
            len(matchedContainerIds),
            placeId,
        )

    return _buildResult(
        allSucceeded,
        level,
        message,
        placeId=placeId,
        extraData={
            "matched_container_ids": matchedContainerIds,
            "deleted_container_ids": deletedContainerIds,
            "delete_results": deleteResults,
        },
    )


def deleteAllContainersFromInputs(fleetManagerURL, postFunc):
    """
    Delete every synced container currently present under Fleet/Containers.
    """
    containersBase = getFleetContainersPath()
    matchedContainerIds = []

    for instancePath in _iterContainerInstancePaths(containersBase):
        containerId = readOptionalTagValue(instancePath + "/ID", None)
        if not containerId:
            continue
        matchedContainerIds.append(containerId)

    if not matchedContainerIds:
        return _buildResult(
            False,
            "warn",
            "No containers found to delete",
            extraData={
                "matched_container_ids": [],
                "deleted_container_ids": [],
                "delete_results": [],
            },
        )

    deleteResults = []
    deletedContainerIds = []
    allSucceeded = True
    for containerId in matchedContainerIds:
        result = deleteContainerByIdFromInputs(containerId, fleetManagerURL, postFunc)
        deleteResults.append(result)
        if result["ok"]:
            deletedContainerIds.append(containerId)
        else:
            allSucceeded = False

    if allSucceeded:
        level = "info"
        message = "Deleted {} container(s)".format(len(deletedContainerIds))
    else:
        level = "warn"
        message = "Deleted {} of {} container(s)".format(
            len(deletedContainerIds),
            len(matchedContainerIds),
        )

    return _buildResult(
        allSucceeded,
        level,
        message,
        extraData={
            "matched_container_ids": matchedContainerIds,
            "deleted_container_ids": deletedContainerIds,
            "delete_results": deleteResults,
        },
    )


def deleteContainersWithoutLocationFromInputs(fleetManagerURL, postFunc):
    """
    Delete every synced container that has neither a Robot nor Place id.
    """
    containersBase = getFleetContainersPath()
    matchedContainerIds = []

    for instancePath in _iterContainerInstancePaths(containersBase):
        containerId = readOptionalTagValue(instancePath + "/ID", None)
        placeId = readOptionalTagValue(instancePath + "/Place", None)
        robotId = readOptionalTagValue(instancePath + "/Robot", None)
        if not containerId:
            continue
        if _hasLocationValue(placeId) or _hasLocationValue(robotId):
            continue
        matchedContainerIds.append(containerId)

    if not matchedContainerIds:
        return _buildResult(
            False,
            "warn",
            "No containers without robot or place were found",
            extraData={
                "matched_container_ids": [],
                "deleted_container_ids": [],
                "delete_results": [],
            },
        )

    deleteResults = []
    deletedContainerIds = []
    allSucceeded = True
    for containerId in matchedContainerIds:
        result = deleteContainerByIdFromInputs(containerId, fleetManagerURL, postFunc)
        deleteResults.append(result)
        if result["ok"]:
            deletedContainerIds.append(containerId)
        else:
            allSucceeded = False

    if allSucceeded:
        level = "info"
        message = "Deleted {} container(s) without robot or place".format(len(deletedContainerIds))
    else:
        level = "warn"
        message = "Deleted {} of {} container(s) without robot or place".format(
            len(deletedContainerIds),
            len(matchedContainerIds),
        )

    return _buildResult(
        allSucceeded,
        level,
        message,
        extraData={
            "matched_container_ids": matchedContainerIds,
            "deleted_container_ids": deletedContainerIds,
            "delete_results": deleteResults,
        },
    )


def createContainerAtPlace(containerTagPath, placeId):
    """
    Create one container through the OTTO operations endpoint at an explicit place.
    """
    return _runCreateFromTagPath(
        containerTagPath,
        "place",
        placeId,
        createContainerAtPlaceFromInputs,
    )


def createContainerAtRobot(containerTagPath, robotId):
    """
    Create one container through the OTTO operations endpoint at an explicit robot.
    """
    return _runCreateFromTagPath(
        containerTagPath,
        "robot",
        robotId,
        createContainerAtRobotFromInputs,
    )


def updateContainerPlaceById(containerId, placeId):
    """
    Update one container's place through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Updating container [{}] place to [{}]".format(containerId, placeId),
        updateContainerPlaceByIdFromInputs,
        containerId,
        placeId,
    )


def updateContainerRobotById(containerId, robotId):
    """
    Update one container's robot through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Updating container [{}] robot to [{}]".format(containerId, robotId),
        updateContainerRobotByIdFromInputs,
        containerId,
        robotId,
    )


def deleteContainerById(containerId):
    """
    Delete one container through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Deleting container [{}]".format(containerId),
        deleteContainerByIdFromInputs,
        containerId,
    )


def deleteContainersAtPlace(placeId):
    """
    Delete every synced container currently assigned to the provided place id.
    """
    return _runDirectOperation(
        "Deleting containers at place [{}]".format(placeId),
        deleteContainersAtPlaceFromInputs,
        placeId,
    )


def deleteAllContainers():
    """
    Delete every synced container through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Deleting all containers",
        deleteAllContainersFromInputs,
    )


def cleanupContainersWithoutLocation():
    """
    Delete every synced container that has neither a Robot nor Place id.
    """
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()
    verboseLogging = _isVerboseCleanupLoggingEnabled()

    if verboseLogging:
        ottoLogger.info("Deleting containers without robot or place")

    result = deleteContainersWithoutLocationFromInputs(fleetManagerURL, httpPost)

    if result["response_text"] is not None:
        writeLastSystemResponse(result["response_text"], asyncWrite=True)

    if result["ok"] and result.get("deleted_container_ids"):
        ottoLogger.info(result["message"])
    elif not result["ok"] and result["level"] == "warn":
        if verboseLogging or result.get("matched_container_ids"):
            ottoLogger.warn(result["message"])
    elif result["level"] == "error":
        ottoLogger.error(result["message"])

    writeLastTriggerResponse(result["message"], asyncWrite=True)
    return result


def CreateAtPlace(containerUdtPath, placeId):
    return createContainerAtPlace(containerUdtPath, placeId)


def CreateAtRobot(containerUdtPath, robotId):
    return createContainerAtRobot(containerUdtPath, robotId)


def UpdatePlaceById(containerId, placeId):
    return updateContainerPlaceById(containerId, placeId)


def UpdateRobotById(containerId, robotId):
    return updateContainerRobotById(containerId, robotId)


def DeleteById(containerId):
    return deleteContainerById(containerId)


def DeleteAtPlace(placeId):
    return deleteContainersAtPlace(placeId)


def DeleteAll():
    return deleteAllContainers()


def CleanupWithoutLocation():
    return cleanupContainersWithoutLocation()
