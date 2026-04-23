from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import fetchListResource
from Otto_API.Common.TagIO import writeLastSystemResponse
from Otto_API.Containers.Apply import applyContainerSync


def _log():
    return system.util.getLogger("Otto_API.Containers.Get")


def updateContainers():
    """
    Get container data from OTTO and sync Fleet/Containers.
    """
    url = getApiBaseUrl() + "/containers/?fields=%2A"
    ottoLogger = _log()

    try:
        response, data, errorResult = fetchListResource(
            url,
            ottoLogger,
            "Containers",
            responseWriter=writeLastSystemResponse,
            parseErrorLabel="Containers"
        )
        if errorResult is not None:
            return errorResult

        return applyContainerSync(data, ottoLogger)

    except Exception as e:
        ottoLogger.error("Otto API - Containers tag update failed: {}".format(str(e)))
        return buildSyncResult(False, "error", "Containers tag update failed: {}".format(str(e)))
