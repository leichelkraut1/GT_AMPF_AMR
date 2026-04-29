import json
import time
import uuid

from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.JsonRpc import postJsonRpcPayload
from Otto_API.Common.SyncHelpers import sanitizeTagName
from Otto_API.Models.Results import OperationalResult


def parseTemplateJson(templateJsonStr):
    try:
        template = json.loads(templateJsonStr)
    except Exception as e:
        raise ValueError("Invalid template JSON: {}".format(str(e)))

    if not isinstance(template, dict):
        raise ValueError("Template JSON was returned as invalid")

    return template


def buildCreateMissionPayload(templateDict, robotId, missionName, nowEpoch=None, uuidFactory=None):
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
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return ("error", "Fleet Manager returned non-JSON response: {}".format(str(e)), None)

    if "result" in respJson:
        result = respJson["result"]
        missionId = result.get("uuid") or result.get("id")
        if missionId:
            return ("info", "Mission created successfully - ID: {}".format(missionId), missionId)
        return ("warn", "Mission created, but no mission ID found: {}".format(json.dumps(result)), None)

    if "error" in respJson:
        return ("warn", "API Error: {}".format(json.dumps(respJson["error"])), None)

    return ("warn", "Unexpected response: {}".format(responseText), None)


def buildFinalizeMissionPayload(missionId, nowEpoch=None):
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
    try:
        respJson = json.loads(responseText)
    except Exception as e:
        return ("error", "Non-JSON response while canceling mission: {}".format(str(e)))

    if "result" in respJson:
        return ("info", "Mission [{}] canceled successfully".format(missionId))

    if "error" in respJson:
        return ("warn", "API Error while canceling mission: {}".format(json.dumps(respJson["error"])))

    return ("warn", "Unexpected response while canceling mission: {}".format(responseText))


class MissionCommandResult(OperationalResult):
    def __init__(self, ok, level, message, missionId=None, responseText=None, payload=None):
        self.mission_id = missionId
        self.response_text = responseText
        self.payload = payload
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            dataFields={
                "mission_id": self.mission_id,
                "response_text": self.response_text,
                "payload": self.payload,
            },
        )


class MissionBatchCommandResult(OperationalResult):
    def __init__(
        self,
        ok,
        level,
        message,
        missionIds=None,
        responseTexts=None,
        payloads=None,
    ):
        self.mission_ids = list(missionIds or [])
        self.response_texts = list(responseTexts or [])
        self.payloads = list(payloads or [])
        self.mission_id = self.mission_ids[0] if self.mission_ids else None
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            dataFields={
                "mission_ids": self.mission_ids,
                "response_texts": self.response_texts,
                "payloads": self.payloads,
                "mission_id": self.mission_id,
            },
        )


def _missionResult(ok, level, message, missionId=None, responseText=None, payload=None):
    return MissionCommandResult(
        ok,
        level,
        message,
        missionId=missionId,
        responseText=responseText,
        payload=payload,
    )


def _cancelBatchResult(
    ok,
    level,
    message,
    missionIds=None,
    responseTexts=None,
    payloads=None,
):
    return MissionBatchCommandResult(
        ok,
        level,
        message,
        missionIds=missionIds,
        responseTexts=responseTexts,
        payloads=payloads,
    )


def postCreateMission(
    operationsUrl,
    templateDict,
    robotId,
    missionName,
    postFunc=httpPost,
):
    try:
        missionPayload = buildCreateMissionPayload(templateDict, robotId, missionName)
        response = postJsonRpcPayload(operationsUrl, missionPayload, postFunc)

        level, message, missionId = interpretCreateMissionResponse(response)

        return _missionResult(
            level == "info",
            level,
            message,
            missionId=missionId,
            responseText=response,
            payload=missionPayload,
        )
    except Exception as exc:
        return _missionResult(
            False,
            "error",
            "Error posting mission: {}".format(exc),
            payload=None,
        )


def postFinalizeMission(operationsUrl, missionId, postFunc=httpPost):
    missionId = str(missionId or "").strip()
    if not missionId:
        return _missionResult(
            False,
            "warn",
            "No mission id supplied for finalize mission",
        )

    try:
        missionPayload = buildFinalizeMissionPayload(missionId)
        response = postJsonRpcPayload(operationsUrl, missionPayload, postFunc)

        level, message = interpretFinalizeMissionResponse(response, missionId)
        return _missionResult(
            level == "info",
            level,
            message,
            missionId=missionId,
            responseText=response,
            payload=missionPayload,
        )
    except Exception as exc:
        return _missionResult(
            False,
            "error",
            "Error finalizing mission [{}]: {}".format(missionId, exc),
            missionId=missionId,
        )


def _postCancelMission(operationsUrl, missionId, postFunc):
    missionId = str(missionId or "").strip()
    if not missionId:
        return _missionResult(
            False,
            "warn",
            "No mission id supplied for cancel mission",
        )

    try:
        missionPayload = buildCancelMissionPayload(missionId)
        response = postJsonRpcPayload(operationsUrl, missionPayload, postFunc)

        level, message = interpretCancelMissionResponse(response, missionId)
        return _missionResult(
            level == "info",
            level,
            message,
            missionId=missionId,
            responseText=response,
            payload=missionPayload,
        )
    except Exception as exc:
        return _missionResult(
            False,
            "error",
            "Error canceling mission [{}]: {}".format(missionId, exc),
            missionId=missionId,
        )


def postCancelMissions(
    operationsUrl,
    missionIds,
    postFunc=httpPost,
    emptyWarnMessage="No mission ids found to cancel",
    successMessage="Canceled {} mission(s)",
    errorMessage="Error canceling missions: {}",
):
    targetMissionIds = [
        str(missionId)
        for missionId in list(missionIds or [])
        if str(missionId or "").strip()
    ]
    if not targetMissionIds:
        return _cancelBatchResult(False, "warn", emptyWarnMessage)

    responseTexts = []
    payloads = []
    canceledMissionIds = []

    try:
        for missionId in targetMissionIds:
            result = _postCancelMission(operationsUrl, missionId, postFunc)
            if not result.ok:
                return _cancelBatchResult(
                    False,
                    result.level or "warn",
                    result.message or "",
                    missionIds=canceledMissionIds,
                    responseTexts=responseTexts + [result.response_text],
                    payloads=payloads + [result.payload],
                )

            canceledMissionIds.append(missionId)
            responseTexts.append(result.response_text)
            payloads.append(result.payload)

        return _cancelBatchResult(
            True,
            "info",
            successMessage.format(len(canceledMissionIds)),
            missionIds=canceledMissionIds,
            responseTexts=responseTexts,
            payloads=payloads,
        )
    except Exception as exc:
        return _cancelBatchResult(
            False,
            "error",
            errorMessage.format(exc),
            missionIds=canceledMissionIds,
            responseTexts=responseTexts,
            payloads=payloads,
        )
