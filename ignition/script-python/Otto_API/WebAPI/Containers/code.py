from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseListPayload
from Otto_API.Containers.Actions import buildCreateContainerPayload
from Otto_API.Containers.Actions import buildDeleteContainerPayload
from Otto_API.Containers.Actions import buildUpdateContainerPlacePayload
from Otto_API.Containers.Actions import buildUpdateContainerRobotPayload
from Otto_API.Containers.Actions import interpretCreateContainerResponse
from Otto_API.Containers.Actions import interpretDeleteContainerResponse
from Otto_API.Containers.Actions import interpretUpdateContainerPlaceResponse
from Otto_API.Containers.Actions import interpretUpdateContainerRobotResponse
from Otto_API.Models.Containers import ContainerCreateFields
from Otto_API.Models.Containers import ContainerLocationTarget
from Otto_API.Models.Results import OperationalResult
from Otto_API.Models.Results import RecordSyncResult


def _operationResult(
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
    dataFields = {
        "container_id": containerId,
        "place_id": placeId,
        "robot_id": robotId,
        "response_text": responseText,
        "payload": payload,
    }
    dataFields.update(dict(extraData or {}))
    return OperationalResult(
        ok,
        level,
        message,
        dataFields=dataFields,
    )


def _resultWithDefaults(defaultFields=None, defaultExtraData=None):
    defaultFields = dict(defaultFields or {})
    defaultExtraData = dict(defaultExtraData or {})

    def _fieldValue(fieldName, overrideFields):
        if fieldName in overrideFields:
            return overrideFields.get(fieldName)
        return defaultFields.get(fieldName)

    def _result(
        ok,
        level,
        message,
        responseText=None,
        payload=None,
        extraData=None,
        **overrideFields
    ):
        mergedExtraData = dict(defaultExtraData)
        mergedExtraData.update(dict(extraData or {}))

        return _operationResult(
            ok=ok,
            level=level,
            message=message,
            containerId=_fieldValue("containerId", overrideFields),
            placeId=_fieldValue("placeId", overrideFields),
            robotId=_fieldValue("robotId", overrideFields),
            responseText=responseText,
            payload=payload,
            extraData=mergedExtraData,
        )

    return _result


def _applyContainerLocationTarget(containerFields, locationTarget):
    containerFields = ContainerCreateFields.fromDict(containerFields)
    if locationTarget.isRobot():
        return containerFields.withRobot(locationTarget.value)
    return containerFields.withPlace(locationTarget.value)


def fetchContainers(apiBaseUrl, getFunc=httpGet):
    """
    Fetch OTTO container records.
    """
    url = str(apiBaseUrl or "").rstrip("/") + "/containers/?fields=%2A"
    response = getFunc(url=url, headerValues=jsonHeaders())

    if not response:
        return RecordSyncResult(
            False,
            "error",
            "HTTP GET failed for /Containers/",
            records=[],
            dataFields={"response_text": response},
        )

    try:
        records = parseListPayload(response)
    except Exception as exc:
        return RecordSyncResult(
            False,
            "error",
            "Containers JSON decode error - {}".format(exc),
            records=[],
            dataFields={"response_text": response},
        )

    return RecordSyncResult(
        True,
        "info",
        "Fetched {} container record(s)".format(len(records)),
        records=records,
        dataFields={"response_text": response},
    )


def postCreateContainer(containerFields, operationsUrl, postFunc=httpPost):
    """
    Create one container through the OTTO operations endpoint.
    """
    _result = _resultWithDefaults()

    if not containerFields:
        return _result(False, "warn", "No container fields supplied for create")

    try:
        containerFields = ContainerCreateFields.fromDict(containerFields)
        payload = buildCreateContainerPayload(containerFields)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=operationsUrl,
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


def postCreateContainerAtPlace(containerFields, placeId, operationsUrl, postFunc=httpPost):
    """
    Create one container at an explicit place.
    """
    _result = _resultWithDefaults({"placeId": placeId})

    if not placeId:
        return _result(False, "warn", "No place id supplied for container create")

    locationTarget = ContainerLocationTarget.fromKindValue("place", placeId)
    containerFields = _applyContainerLocationTarget(containerFields, locationTarget)
    return postCreateContainer(containerFields, operationsUrl, postFunc)


def postCreateContainerAtRobot(containerFields, robotId, operationsUrl, postFunc=httpPost):
    """
    Create one container at an explicit robot.
    """
    _result = _resultWithDefaults({"robotId": robotId})

    if not robotId:
        return _result(False, "warn", "No robot id supplied for container create")

    locationTarget = ContainerLocationTarget.fromKindValue("robot", robotId)
    containerFields = _applyContainerLocationTarget(containerFields, locationTarget)
    return postCreateContainer(containerFields, operationsUrl, postFunc)


def postUpdateContainerPlace(containerId, placeId, operationsUrl, postFunc=httpPost):
    """
    Update one container's place.
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
            url=operationsUrl,
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


def postUpdateContainerRobot(containerId, robotId, operationsUrl, postFunc=httpPost):
    """
    Update one container's robot.
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
            url=operationsUrl,
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


def postDeleteContainer(containerId, operationsUrl, postFunc=httpPost):
    """
    Delete one container.
    """
    _result = _resultWithDefaults({"containerId": containerId})

    if not containerId:
        return _result(False, "warn", "No container id supplied for delete", containerId=None)

    try:
        payload = buildDeleteContainerPayload(containerId)
        jsonBody = system.util.jsonEncode(payload)
        response = postFunc(
            url=operationsUrl,
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
