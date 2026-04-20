import json
import time


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
            "container": dict(containerFields or {})
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


def interpretUpdateContainerResponse(responseText, containerId, placeId):
    """
    Backwards-compatible alias for place-specific update parsing.
    """
    return interpretUpdateContainerPlaceResponse(responseText, containerId, placeId)


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
