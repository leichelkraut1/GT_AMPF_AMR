import json
import time
import uuid

from Otto_API.Fleet.ContentSync import sanitizeTagName


def parseTemplateJson(templateJsonStr):
    """
    Parse a workflow template JSON string and return the template dict.
    """
    try:
        template = json.loads(templateJsonStr)
    except Exception as e:
        raise ValueError("Invalid template JSON: {}".format(str(e)))

    if not isinstance(template, dict):
        raise ValueError("Template JSON was returned as invalid")

    return template


def buildCreateMissionPayload(templateDict, robotId, missionName, nowEpoch=None, uuidFactory=None):
    """
    Build the JSON-RPC payload for createMission from explicit inputs.
    """
    if nowEpoch is None:
        nowEpoch = time.time()

    if uuidFactory is None:
        uuidFactory = uuid.uuid4

    tasks = templateDict.get("tasks", [])
    if not isinstance(tasks, list):
        tasks = []

    missionPriority = templateDict.get("priority", 100)
    suffix = sanitizeTagName(str(nowEpoch))

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": "createMission",
        "params": {
            "mission": {
                "client_reference_id": str(uuidFactory()),
                "description": missionName + " - " + suffix,
                "finalized": False,
                "force_robot": robotId,
                "force_team": None,
                "max_duration": "0",
                "metadata": "",
                "name": missionName + " - " + suffix,
                "nominal_duration": "0",
                "priority": missionPriority
            },
            "tasks": tasks
        }
    }


def interpretCreateMissionResponse(responseText):
    """
    Parse a createMission response and return (logLevel, message).
    """
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return ("error", "Fleet Manager returned non-JSON response: {}".format(str(e)))

    if "result" in respJson:
        result = respJson["result"]
        missionId = result.get("uuid") or result.get("id")
        if missionId:
            return ("info", "Mission created successfully - ID: {}".format(missionId))
        return ("warn", "Mission created, but no mission ID found: {}".format(json.dumps(result)))

    if "error" in respJson:
        return ("warn", "API Error: {}".format(json.dumps(respJson["error"])))

    return ("warn", "Unexpected response: {}".format(responseText))


def resolveMissionRobotId(missionRecord):
    """
    Resolve the robot affinity for a mission record.
    Preference order:
    1. assigned_robot
    2. force_robot
    3. forced_robot
    Accepts either lowercase or title-cased tag-style keys.
    """
    missionRecord = dict(missionRecord or {})
    for key in [
        "assigned_robot",
        "Assigned_Robot",
        "force_robot",
        "Force_Robot",
        "forced_robot",
        "Forced_Robot",
    ]:
        value = missionRecord.get(key)
        if value is None:
            continue
        value = str(value).strip().lower()
        if value:
            return value
    return None


def findActiveMissionIdForRobot(robotId, missionRecords):
    """
    Find the active mission ID assigned to the given robot ID.
    Returns (missionId, warningMessage).
    """
    for missionRecord in missionRecords:
        resolvedRobotId = resolveMissionRobotId(missionRecord)
        if resolvedRobotId is None:
            continue

        if resolvedRobotId != robotId:
            continue

        missionId = missionRecord.get("id")
        if missionId:
            return (str(missionId), None)

        return (
            None,
            "Matched mission for robot ID [{}], but no mission id found".format(robotId)
        )

    return (None, None)


def findActiveMissionIdsForRobot(robotId, missionRecords):
    """
    Find all active mission IDs assigned to the given robot ID.
    Returns (missionIds, warnings).
    """
    missionIds = []
    warnings = []

    for missionRecord in missionRecords:
        resolvedRobotId = resolveMissionRobotId(missionRecord)
        if resolvedRobotId is None:
            continue

        if resolvedRobotId != robotId:
            continue

        missionId = missionRecord.get("id")
        if missionId:
            missionIds.append(str(missionId))
        else:
            warnings.append(
                "Matched mission for robot ID [{}], but no mission id found".format(robotId)
            )

    return (missionIds, warnings)


def buildFinalizeMissionPayload(missionId, nowEpoch=None):
    """
    Build the JSON-RPC payload for updateMission(finalized=True).
    """
    if nowEpoch is None:
        nowEpoch = time.time()

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": "updateMission",
        "params": {
            "append_tasks": [],
            "fields": {
                "finalized": True
            },
            "id": missionId
        }
    }


def buildCancelMissionPayload(missionId, nowEpoch=None):
    """
    Build the JSON-RPC payload for cancelMission.
    """
    if nowEpoch is None:
        nowEpoch = time.time()

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": "cancelMission",
        "params": {
            "id": missionId
        }
    }


def interpretFinalizeMissionResponse(responseText, missionId):
    """
    Parse an updateMission response and return (logLevel, message).
    """
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return ("error", "Non-JSON response while finalizing mission: {}".format(str(e)))

    if "result" in respJson:
        return ("info", "Mission [{}] finalized successfully".format(missionId))

    if "error" in respJson:
        return ("warn", "API Error while finalizing mission: {}".format(json.dumps(respJson["error"])))

    return ("warn", "Unexpected response while finalizing mission: {}".format(responseText))


def interpretCancelMissionResponse(responseText, missionId):
    """
    Parse a cancelMission response and return (logLevel, message).
    """
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return ("error", "Non-JSON response while canceling mission: {}".format(str(e)))

    if "result" in respJson:
        return ("info", "Mission [{}] canceled successfully".format(missionId))

    if "error" in respJson:
        return ("warn", "API Error while canceling mission: {}".format(json.dumps(respJson["error"])))

    return ("warn", "Unexpected response while canceling mission: {}".format(responseText))
