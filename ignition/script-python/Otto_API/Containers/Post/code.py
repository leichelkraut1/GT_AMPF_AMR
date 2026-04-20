from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import getOttoOperationsUrl
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValues
from Otto_API.Common.TagHelpers import writeLastSystemResponse
from Otto_API.Common.TagHelpers import writeLastTriggerResponse
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


def _readCreateContainerBaseFields(containerTagPath):
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

    containerFields = {}
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

    for browseResult in browseTagResults(containersBase):
        if isinstance(browseResult, dict):
            instancePath = str(browseResult.get("fullPath", "") or "")
            tagType = browseResult.get("tagType")
        else:
            instancePath = str(getattr(browseResult, "fullPath", "") or "")
            tagType = getattr(browseResult, "tagType", None)

        if not instancePath:
            continue
        if tagType != "UdtInstance":
            continue

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
