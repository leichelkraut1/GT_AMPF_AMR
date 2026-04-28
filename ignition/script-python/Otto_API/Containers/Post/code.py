import uuid

from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.OperationHelpers import buildDataResult
from Otto_API.Common.OperationHelpers import logOperationResult
from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import getOttoOperationsUrl
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readRequiredTagValues
from Otto_API.Common.TagPaths import getFleetContainersPath
from Otto_API.Common.TagPaths import getFleetContainersVerboseCleanupLoggingPath
from Otto_API.Containers.Actions import buildCreateContainerPayload
from Otto_API.Containers.Actions import buildDeleteContainerPayload
from Otto_API.Containers.Actions import buildUpdateContainerPlacePayload
from Otto_API.Containers.Actions import buildUpdateContainerRobotPayload
from Otto_API.Models.Containers import ContainerCreateFields
from Otto_API.Models.Containers import ContainerLocationTarget
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
    data = {
        "container_id": containerId,
        "place_id": placeId,
        "robot_id": robotId,
        "response_text": responseText,
        "payload": payload,
    }
    data.update(dict(extraData or {}))
    return buildDataResult(ok, level, message, **data)


def _resultWithDefaults(defaultFields=None, defaultExtraData=None):
    defaultFields = dict(defaultFields or {})
    defaultExtraData = dict(defaultExtraData or {})

    def _result(
        ok,
        level,
        message,
        responseText=None,
        payload=None,
        extraData=None,
        **overrideFields
    ):
        resultFields = dict(defaultFields)
        resultFields.update({
            key: value for key, value in overrideFields.items()
            if key in ["containerId", "placeId", "robotId"]
        })

        mergedExtraData = dict(defaultExtraData)
        mergedExtraData.update(dict(extraData or {}))

        return _buildResult(
            ok=ok,
            level=level,
            message=message,
            responseText=responseText,
            payload=payload,
            extraData=mergedExtraData,
            **resultFields
        )

    return _result


def _writeResponseAndLogResult(result, logger):
    return logOperationResult(result, logger)


def _applyContainerLocationTarget(containerFields, locationTarget):
    containerFields = ContainerCreateFields.fromDict(containerFields)
    if locationTarget.isRobot():
        return containerFields.withRobot(locationTarget.value)
    return containerFields.withPlace(locationTarget.value)


def _runCreateFromTagPath(containerTagPath, targetLabel, targetId, createFunc):
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Creating container from tag path [{}] at {} [{}]".format(containerTagPath, targetLabel, targetId))

    try:
        containerFields = _readCreateContainerBaseFields(containerTagPath)
    except Exception as e:
        msg = "Error reading container fields from [{}]: {}".format(containerTagPath, str(e))
        ottoLogger.error(msg)
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


def _buildContainerCreateId(uuidFactory=None):
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
        "id": _buildContainerCreateId(uuidFactory),
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
    return ContainerCreateFields.fromDict(containerFields)


def createContainerFromInputs(containerFields, fleetManagerURL, postFunc):
    """
    Create one container from explicit fields and return a structured result.
    """
    _result = _resultWithDefaults()

    if not containerFields:
        return _result(False, "warn", "No container fields supplied for create")

    try:
        containerFields = ContainerCreateFields.fromDict(containerFields)
        payload = buildCreateContainerPayload(containerFields)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message, containerId = interpretCreateContainerResponse(response)
        return _result(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            containerId=containerId,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _result(
            False,
            "error",
            "Error creating container: {}".format(str(e)),
        )


def createContainerAtPlaceFromInputs(containerFields, placeId, fleetManagerURL, postFunc):
    """
    Create one container at an explicit place and return a structured result.
    """
    _result = _resultWithDefaults({"placeId": placeId})

    if not placeId:
        return _result(False, "warn", "No place id supplied for container create")

    locationTarget = ContainerLocationTarget.fromKindValue("place", placeId)
    containerFields = _applyContainerLocationTarget(containerFields, locationTarget)
    return createContainerFromInputs(containerFields, fleetManagerURL, postFunc)


def createContainerAtRobotFromInputs(containerFields, robotId, fleetManagerURL, postFunc):
    """
    Create one container at an explicit robot and return a structured result.
    """
    _result = _resultWithDefaults({"robotId": robotId})

    if not robotId:
        return _result(False, "warn", "No robot id supplied for container create")

    locationTarget = ContainerLocationTarget.fromKindValue("robot", robotId)
    containerFields = _applyContainerLocationTarget(containerFields, locationTarget)
    return createContainerFromInputs(containerFields, fleetManagerURL, postFunc)


def updateContainerPlaceByIdFromInputs(containerId, placeId, fleetManagerURL, postFunc):
    """
    Update one container's place using explicit inputs and return a structured result.
    """
    _result = _resultWithDefaults({"containerId": containerId, "placeId": placeId})

    if not containerId:
        return _result(False, "warn", "No container id supplied for update", containerId=None)
    if not placeId:
        return _result(False, "warn", "No place id supplied for container update", placeId=None)

    try:
        payload = buildUpdateContainerPlacePayload(containerId, placeId)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message = interpretUpdateContainerPlaceResponse(response, containerId, placeId)
        return _result(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _result(
            False,
            "error",
            "Error updating container [{}] place to [{}]: {}".format(containerId, placeId, str(e)),
        )


def updateContainerRobotByIdFromInputs(containerId, robotId, fleetManagerURL, postFunc):
    """
    Update one container's robot using explicit inputs and return a structured result.
    """
    _result = _resultWithDefaults({"containerId": containerId, "robotId": robotId})

    if not containerId:
        return _result(False, "warn", "No container id supplied for update", containerId=None)
    if not robotId:
        return _result(False, "warn", "No robot id supplied for container update", robotId=None)

    try:
        payload = buildUpdateContainerRobotPayload(containerId, robotId)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message = interpretUpdateContainerRobotResponse(response, containerId, robotId)
        return _result(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _result(
            False,
            "error",
            "Error updating container [{}] robot to [{}]: {}".format(containerId, robotId, str(e)),
        )


def deleteContainerByIdFromInputs(containerId, fleetManagerURL, postFunc):
    """
    Delete one container by explicit inputs and return a structured result.
    """
    _result = _resultWithDefaults({"containerId": containerId})

    if not containerId:
        return _result(False, "warn", "No container id supplied for delete", containerId=None)

    try:
        payload = buildDeleteContainerPayload(containerId)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=fleetManagerURL,
            postData=jsonBody,
        )

        logLevel, message = interpretDeleteContainerResponse(response, containerId)
        return _result(
            ok=(logLevel == "info"),
            level=logLevel,
            message=message,
            responseText=response,
            payload=payload,
        )
    except Exception as e:
        return _result(
            False,
            "error",
            "Error deleting container [{}]: {}".format(containerId, str(e)),
        )


def _deleteMatchedContainerIds(
    matchedContainerIds,
    fleetManagerURL,
    postFunc,
    noMatchMessage,
    successMessageBuilder,
    partialMessageBuilder,
    resultFields=None,
):
    _result = _resultWithDefaults(resultFields)

    if not matchedContainerIds:
        return _result(
            False,
            "warn",
            noMatchMessage,
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
        message = successMessageBuilder(deletedContainerIds, matchedContainerIds)
    else:
        level = "warn"
        message = partialMessageBuilder(deletedContainerIds, matchedContainerIds)

    return _result(
        allSucceeded,
        level,
        message,
        extraData={
            "matched_container_ids": matchedContainerIds,
            "deleted_container_ids": deletedContainerIds,
            "delete_results": deleteResults,
        },
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

    return _deleteMatchedContainerIds(
        matchedContainerIds,
        fleetManagerURL,
        postFunc,
        "No containers found at place [{}]".format(placeId),
        lambda deletedIds, _matchedIds: "Deleted {} container(s) at place [{}]".format(
            len(deletedIds),
            placeId
        ),
        lambda deletedIds, matchedIds: "Deleted {} of {} container(s) at place [{}]".format(
            len(deletedIds),
            len(matchedIds),
            placeId
        ),
        resultFields={"placeId": placeId},
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

    return _deleteMatchedContainerIds(
        matchedContainerIds,
        fleetManagerURL,
        postFunc,
        "No containers found to delete",
        lambda deletedIds, _matchedIds: "Deleted {} container(s)".format(len(deletedIds)),
        lambda deletedIds, matchedIds: "Deleted {} of {} container(s)".format(
            len(deletedIds),
            len(matchedIds)
        ),
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

    return _deleteMatchedContainerIds(
        matchedContainerIds,
        fleetManagerURL,
        postFunc,
        "No containers without robot or place were found",
        lambda deletedIds, _matchedIds: "Deleted {} container(s) without robot or place".format(
            len(deletedIds)
        ),
        lambda deletedIds, matchedIds: "Deleted {} of {} container(s) without robot or place".format(
            len(deletedIds),
            len(matchedIds)
        ),
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

    resultData = dict(result.get("data") or {})
    if result["ok"] and resultData.get("deleted_container_ids"):
        ottoLogger.info(result["message"])
    elif not result["ok"] and result["level"] == "warn":
        if verboseLogging or resultData.get("matched_container_ids"):
            ottoLogger.warn(result["message"])
    elif result["level"] == "error":
        ottoLogger.error(result["message"])
    return result
