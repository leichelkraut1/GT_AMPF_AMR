from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.TagSync.Places import applyPlaceSync
from Otto_API.WebAPI.Places import fetchPlaces


def _log():
    return system.util.getLogger("Otto_API.Services.Places")


def updatePlaces():
    """
    Fetch OTTO place records and sync Fleet/Places.
    """
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Places/")

    try:
        fetchResult = fetchPlaces(getApiBaseUrl())
        if not fetchResult.ok:
            return fetchResult.toDict()

        responseText = str(fetchResult.data.get("response_text") or "")
        return applyPlaceSync(fetchResult.records, responseText, ottoLogger)

    except Exception as e:
        ottoLogger.error("Otto API - /Places/ Tag Update Failed - " + str(e))
        return buildSyncResult(False, "error", "Places tag update failed - " + str(e))
