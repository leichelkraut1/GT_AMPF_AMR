from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseServerStatus
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import getApiBaseUrl
from Otto_API.Common.TagHelpers import getFleetSystemPath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import writeTagValueAsync


SYSTEM_BASE_PATH = getFleetSystemPath()


def _log():
    return system.util.getLogger("Otto_API.System.Get")


def _buildSyncResult(ok, level, message, data=None):
    return buildOperationResult(
        ok,
        level,
        message,
        data={"value": data},
        value=data,
    )


def getServerStatus():
    """
    Read Fleet Manager server state and mirror it into Fleet/System/ServerStatus.
    """
    url = getApiBaseUrl() + "/system/state/"
    logger = _log()

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        if response:
            status = parseServerStatus(response)
            writeTagValueAsync(SYSTEM_BASE_PATH + "/ServerStatus", status)
            return _buildSyncResult(True, "info", "Server status updated", data=status)

        logger.warn("Otto Fleet Manager did not respond to status update request")
        writeTagValueAsync(SYSTEM_BASE_PATH + "/ServerStatus", "ResponseError")
        return _buildSyncResult(False, "warn", "Otto Fleet Manager did not respond")
    except Exception as exc:
        logger.error("Otto API - Status update failed - {}".format(str(exc)))
        return _buildSyncResult(False, "error", "Status update failed - {}".format(str(exc)))


def readCachedServerStatus():
    """
    Read the most recent server-status value written by the slower status timer.
    """
    status = readOptionalTagValue(
        SYSTEM_BASE_PATH + "/ServerStatus",
        None,
        allowEmptyString=False
    )
    if status in [None, "", "ResponseError"]:
        return _buildSyncResult(
            False,
            "warn",
            "Cached server status is unavailable",
            data=status
        )

    return _buildSyncResult(
        True,
        "info",
        "Cached server status read",
        data=status
    )
