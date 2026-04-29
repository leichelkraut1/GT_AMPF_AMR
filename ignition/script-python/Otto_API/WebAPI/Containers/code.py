import json
import time

from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseListPayload
from Otto_API.Models.Containers import ContainerCreateFields
from Otto_API.Models.Containers import ContainerLocationTarget
from Otto_API.Models.Results import OperationalResult
from Otto_API.Models.Results import RecordSyncResult


def _containerFieldsPayload(containerFields):
    if hasattr(containerFields, "toPayloadFields"):
        return containerFields.toPayloadFields()
    return dict(containerFields or {})


def buildCreateContainerPayload(containerFields, nowEpoch=None):
    """
    Build the JSON-RPC payload for createContainer.
    """
    if nowEpoch is None:
        nowEpoch = time.time()

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": "createContainer",
        "params": {
            "container": _containerFieldsPayload(containerFields)
        }
    }


def buildUpdateContainerPlacePayload(containerId, placeId, nowEpoch=None):
    """
    Build the JSON-RPC payload for updateContainer(place=<placeId>).
    """
    if nowEpoch is None:
        nowEpoch = time.time()

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": "updateContainer",
        "params": {
            "fields": {
                "place": placeId
            },
            "id": containerId
        }
    }


def buildUpdateContainerRobotPayload(containerId, robotId, nowEpoch=None):
    """
    Build the JSON-RPC payload for updateContainer(robot=<robotId>).
    """
    if nowEpoch is None:
        nowEpoch = time.time()

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": "updateContainer",
        "params": {
            "fields": {
                "robot": robotId
            },
            "id": containerId
        }
    }


def buildDeleteContainerPayload(containerId, nowEpoch=None):
    """
    Build the JSON-RPC payload for deleteContainer.
    """
    if nowEpoch is None:
        nowEpoch = time.time()

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": "deleteContainer",
        "params": {
            "id": containerId
        }
    }


def interpretCreateContainerResponse(responseText):
    """
    Parse a createContainer response and return (logLevel, message, containerId).
    """
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return ("error", "Non-JSON response while creating container: {}".format(str(e)), None)

    if "result" in respJson:
        resultPayload = respJson.get("result") or {}
        if isinstance(resultPayload, dict):
            containerId = resultPayload.get("id") or resultPayload.get("uuid")
        else:
            containerId = None
        message = "Container created successfully"
        if containerId:
            message += " - ID: {}".format(containerId)
        return ("info", message, containerId)

    if "error" in respJson:
        return ("warn", "API Error while creating container: {}".format(json.dumps(respJson["error"])), None)

    return ("warn", "Unexpected response while creating container: {}".format(responseText), None)


def interpretUpdateContainerPlaceResponse(responseText, containerId, placeId):
    """
    Parse an updateContainer response and return (logLevel, message).
    """
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return (
            "error",
            "Non-JSON response while updating container [{}] place to [{}]: {}".format(
                containerId,
                placeId,
                str(e)
            )
        )

    if "result" in respJson:
        return (
            "info",
            "Container [{}] place updated to [{}] successfully".format(containerId, placeId)
        )

    if "error" in respJson:
        return (
            "warn",
            "API Error while updating container [{}] place to [{}]: {}".format(
                containerId,
                placeId,
                json.dumps(respJson["error"])
            )
        )

    return (
        "warn",
        "Unexpected response while updating container [{}] place to [{}]: {}".format(
            containerId,
            placeId,
            responseText
        )
    )


def interpretUpdateContainerRobotResponse(responseText, containerId, robotId):
    """
    Parse an updateContainer(robot=<robotId>) response and return (logLevel, message).
    """
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return (
            "error",
            "Non-JSON response while updating container [{}] robot to [{}]: {}".format(
                containerId,
                robotId,
                str(e)
            )
        )

    if "result" in respJson:
        return (
            "info",
            "Container [{}] robot updated to [{}] successfully".format(containerId, robotId)
        )

    if "error" in respJson:
        return (
            "warn",
            "API Error while updating container [{}] robot to [{}]: {}".format(
                containerId,
                robotId,
                json.dumps(respJson["error"])
            )
        )

    return (
        "warn",
        "Unexpected response while updating container [{}] robot to [{}]: {}".format(
            containerId,
            robotId,
            responseText
        )
    )


def interpretDeleteContainerResponse(responseText, containerId):
    """
    Parse a deleteContainer response and return (logLevel, message).
    """
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return (
            "error",
            "Non-JSON response while deleting container [{}]: {}".format(
                containerId,
                str(e)
            )
        )

    if "result" in respJson:
        return (
            "info",
            "Container [{}] deleted successfully".format(containerId)
        )

    if "error" in respJson:
        return (
            "warn",
            "API Error while deleting container [{}]: {}".format(
                containerId,
                json.dumps(respJson["error"])
            )
        )

    return (
        "warn",
        "Unexpected response while deleting container [{}]: {}".format(
            containerId,
            responseText
        )
    )


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
