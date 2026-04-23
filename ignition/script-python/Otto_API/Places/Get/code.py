from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import writeLastSystemResponse
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import fetchListResource
from Otto_API.Places.Apply import applyPlaceSync


def _log():
    return system.util.getLogger("Otto_API.Places.Get")


def updatePlaces():
    """
    Get endpoint information from OTTO and sync Fleet/Places.
    """
    url = getApiBaseUrl() + "/places/"
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Places/")

    try:
        response, data, errorResult = fetchListResource(
            url,
            ottoLogger,
            "Places",
            responseWriter=writeLastSystemResponse,
        )
        if errorResult is not None:
            return errorResult

        return applyPlaceSync(data, response, ottoLogger)

    except Exception as e:
        ottoLogger.error("Otto API - /Places/ Tag Update Failed - " + str(e))
        return buildSyncResult(False, "error", "Places tag update failed - " + str(e))
