from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseMissionResults
from Otto_API.Missions.MissionActions import buildCancelMissionPayload
from Otto_API.Missions.MissionActions import buildCreateMissionPayload
from Otto_API.Missions.MissionActions import buildFinalizeMissionPayload
from Otto_API.Missions.MissionActions import interpretCancelMissionResponse
from Otto_API.Missions.MissionActions import interpretCreateMissionResponse
from Otto_API.Missions.MissionActions import interpretFinalizeMissionResponse
from Otto_API.Missions.QueryHelpers import buildMissionsUrl
from Otto_API.Models.Missions import MissionRecord
from Otto_API.Models.Results import OperationalResult
from Otto_API.Models.Results import RecordSyncResult


def _log():
    return system.util.getLogger("Otto_API.WebAPI.Missions")


def _missionRecordsById(missionRecords):
    recordsById = {}
    for missionRecord in list(missionRecords or []):
        missionId = str(missionRecord.id or "").strip()
        if missionId:
            recordsById[missionId] = missionRecord
    return recordsById


def fetchMissions(
    apiBaseUrl,
    missionStatus=None,
    limit=None,
    ordering=None,
    getFunc=httpGet,
    logger=None,
    debug=False,
):
    """
    Fetch OTTO missions for one or more statuses.
    """
    logger = logger or _log()
    if not missionStatus:
        message = "fetchMissions called with no missionStatus"
        if debug:
            logger.warn(message)
        return RecordSyncResult(
            True,
            "warn",
            message,
            records=[],
            recordsByName={},
            sharedFields={
                "warnings": [message],
                "issues": [],
            },
        )

    try:
        url = buildMissionsUrl(apiBaseUrl, missionStatus, limit, ordering=ordering)
        if isinstance(missionStatus, (list, tuple)):
            statusLabel = ",".join([str(status) for status in missionStatus])
        else:
            statusLabel = str(missionStatus)

        if debug:
            logger.debug(
                "Otto API - Requesting missions status={} url={}".format(
                    statusLabel,
                    url,
                )
            )

        response = getFunc(url=url, headerValues=jsonHeaders())
        missionRecords = MissionRecord.listFromDicts(parseMissionResults(response))

        if debug:
            logger.debug(
                "Otto API - Received {} missions for status {}".format(
                    len(missionRecords),
                    statusLabel,
                )
            )

        return RecordSyncResult(
            True,
            "info",
            "Fetched {} mission(s)".format(len(missionRecords)),
            records=missionRecords,
            recordsByName=_missionRecordsById(missionRecords),
            dataFields={"response_text": response},
            sharedFields={
                "warnings": [],
                "issues": [],
            },
        )
    except Exception as exc:
        message = "Error fetching missions (status={}): {}".format(missionStatus, exc)
        logger.error("Otto API - " + message)
        return RecordSyncResult(
            False,
            "error",
            message,
            records=[],
            recordsByName={},
            sharedFields={
                "warnings": [],
                "issues": [],
            },
        )


def _missionResult(ok, level, message, missionId=None, responseText=None, payload=None):
    return OperationalResult(
        ok,
        level,
        message,
        dataFields={
            "mission_id": missionId,
            "response_text": responseText,
            "payload": payload,
        },
    )


def _cancelBatchResult(
    ok,
    level,
    message,
    missionIds=None,
    responseTexts=None,
    payloads=None,
):
    missionIds = list(missionIds or [])
    return OperationalResult(
        ok,
        level,
        message,
        dataFields={
            "mission_ids": missionIds,
            "response_texts": list(responseTexts or []),
            "payloads": list(payloads or []),
            "mission_id": missionIds[0] if missionIds else None,
        },
    )


def postCreateMission(
    operationsUrl,
    templateDict,
    robotId,
    missionName,
    postFunc=httpPost,
):
    """
    Create one mission through the OTTO operations endpoint.
    """
    try:
        missionPayload = buildCreateMissionPayload(templateDict, robotId, missionName)
        response = postFunc(
            url=operationsUrl,
            postData=system.util.jsonEncode(missionPayload),
        )

        level, message = interpretCreateMissionResponse(response)
        missionId = None
        if "ID:" in message:
            missionId = message.split("ID:", 1)[1].strip()

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
    """
    Finalize one mission through the OTTO operations endpoint.
    """
    missionId = str(missionId or "").strip()
    if not missionId:
        return _missionResult(
            False,
            "warn",
            "No mission id supplied for finalize mission",
        )

    try:
        missionPayload = buildFinalizeMissionPayload(missionId)
        response = postFunc(
            url=operationsUrl,
            postData=system.util.jsonEncode(missionPayload),
        )

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
        response = postFunc(
            url=operationsUrl,
            postData=system.util.jsonEncode(missionPayload),
        )

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
    """
    Cancel an explicit mission-id list through the OTTO operations endpoint.
    """
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
                resultData = dict(result.data or {})
                return _cancelBatchResult(
                    False,
                    result.level or "warn",
                    result.message or "",
                    missionIds=canceledMissionIds,
                    responseTexts=responseTexts + [resultData.get("response_text")],
                    payloads=payloads + [resultData.get("payload")],
                )

            canceledMissionIds.append(missionId)
            resultData = dict(result.data or {})
            responseTexts.append(resultData.get("response_text"))
            payloads.append(resultData.get("payload"))

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
