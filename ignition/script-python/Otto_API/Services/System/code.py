from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import writeTagValueAsync
from Otto_API.Common.TagPaths import getFleetSystemPath
from Otto_API.Models.Results import OperationalResult
from Otto_API.WebAPI.System import fetchServerStatus


SYSTEM_BASE_PATH = getFleetSystemPath()


class SystemStatusResult(OperationalResult):
    def __init__(self, ok, level, message, value=None, issues=None):
        self.value = value
        self.issues = list(issues or [])
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            typedFields={"value": self.value},
            sharedFields={"issues": self.issues},
        )


def _log():
    return system.util.getLogger("Otto_API.Services.System")


def _statusResult(ok, level, message, value=None, issues=None):
    return SystemStatusResult(ok, level, message, value=value, issues=issues).toDict()


def getServerStatus():
    """
    Read Fleet Manager server state and mirror it into Fleet/System/ServerStatus.
    """
    logger = _log()

    try:
        fetchResult = fetchServerStatus(getApiBaseUrl())
        if fetchResult.ok:
            status = fetchResult.value
            writeTagValueAsync(SYSTEM_BASE_PATH + "/ServerStatus", status)
            return _statusResult(
                True,
                "info",
                "Server status updated",
                value=status,
                issues=[],
            )

        if fetchResult.level == "warn":
            writeTagValueAsync(SYSTEM_BASE_PATH + "/ServerStatus", "ResponseError")
            return _statusResult(
                False,
                "warn",
                fetchResult.message,
                value=None,
                issues=[
                    buildRuntimeIssue(
                        "server_status.http_no_response",
                        "Otto_API.Services.System",
                        "warn",
                        fetchResult.message,
                    )
                ],
            )

        logger.error("Otto API - {}".format(fetchResult.message))
        return _statusResult(
            False,
            "error",
            fetchResult.message,
            value=None,
            issues=[
                buildRuntimeIssue(
                    "server_status.fetch_failed",
                    "Otto_API.Services.System",
                    "error",
                    fetchResult.message,
                )
            ],
        )
    except Exception as exc:
        message = "Status update failed - {}".format(str(exc))
        logger.error("Otto API - {}".format(message))
        return _statusResult(
            False,
            "error",
            message,
            value=None,
            issues=[
                buildRuntimeIssue(
                    "server_status.fetch_failed",
                    "Otto_API.Services.System",
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
        return _statusResult(
            False,
            "warn",
            message,
            value=status,
            issues=[
                buildRuntimeIssue(
                    "server_status.cached_unavailable",
                    "Otto_API.Services.System",
                    "warn",
                    message,
                )
            ],
        )

    return _statusResult(
        True,
        "info",
        "Cached server status read",
        value=status,
        issues=[],
    )
