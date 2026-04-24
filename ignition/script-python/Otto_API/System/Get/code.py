from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseServerStatus
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import writeTagValueAsync
from Otto_API.Common.TagPaths import getFleetSystemPath


SYSTEM_BASE_PATH = getFleetSystemPath()


def _log():
    return system.util.getLogger("Otto_API.System.Get")


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
            return buildOperationResult(
                True,
                "info",
                "Server status updated",
                data={"value": status},
                value=status,
                issues=[],
            )

        writeTagValueAsync(SYSTEM_BASE_PATH + "/ServerStatus", "ResponseError")
        message = "Otto Fleet Manager did not respond"
        return buildOperationResult(
            False,
            "warn",
            message,
            data={"value": None},
            value=None,
            issues=[
                buildRuntimeIssue(
                    "server_status.http_no_response",
                    "Otto_API.System.Get",
                    "warn",
                    message,
                )
            ],
        )
    except Exception as exc:
        message = "Status update failed - {}".format(str(exc))
        logger.error("Otto API - {}".format(message))
        return buildOperationResult(
            False,
            "error",
            message,
            data={"value": None},
            value=None,
            issues=[
                buildRuntimeIssue(
                    "server_status.fetch_failed",
                    "Otto_API.System.Get",
                    "error",
                    message,
                )
            ],
        )


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
        message = "Cached server status is unavailable"
        return buildOperationResult(
            False,
            "warn",
            message,
            data={"value": status},
            value=status,
            issues=[
                buildRuntimeIssue(
                    "server_status.cached_unavailable",
                    "Otto_API.System.Get",
                    "warn",
                    message,
                )
            ],
        )

    return buildOperationResult(
        True,
        "info",
        "Cached server status read",
        data={"value": status},
        value=status,
        issues=[],
    )
