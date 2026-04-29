from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseMissionResults
from Otto_API.Models.Missions import MissionRecord
from Otto_API.Models.Results import RecordSyncResult


def _log():
    return system.util.getLogger("Otto_API.WebAPI.Missions.Fetch")


def _normalizeMissionStatusList(missionStatus):
    if missionStatus is None:
        return []

    if isinstance(missionStatus, (list, tuple)):
        values = missionStatus
    else:
        values = [missionStatus]

    normalized = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized.append(text)
    return normalized


def buildMissionsUrl(baseUrl, missionStatus, limit=None, offset=None, ordering=None):
    statuses = _normalizeMissionStatusList(missionStatus)
    if not statuses:
        raise ValueError("At least one mission status is required")

    url = str(baseUrl or "").rstrip("/") + "/missions/?fields=%2A"
    if offset is not None:
        url += "&offset=" + str(offset)
    if ordering is not None:
        url += "&ordering=" + str(ordering)

    for status in statuses:
        url += "&mission_status=" + status

    if limit is not None:
        url += "&limit=" + str(limit)

    return url


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
