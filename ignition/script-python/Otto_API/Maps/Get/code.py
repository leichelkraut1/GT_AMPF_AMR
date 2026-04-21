from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.TagHelpers import getApiBaseUrl
from Otto_API.Common.ParseHelpers import parseJsonResponse
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import fetchListResource
from Otto_API.Maps.Apply import applyMapSync
from Otto_API.Maps.Normalize import extractLiveMapReference
from Otto_API.Maps.Normalize import selectMostRecentMap


def _log():
    return system.util.getLogger("Otto_API.Maps.Get")


def readLiveMapReference():
    """
    Read the active OTTO map reference/id from the dedicated live_map endpoint.
    """
    url = getApiBaseUrl() + "/live_map/?fields=reference&offset=0&limit=100"

    response = httpGet(url=url, headerValues=jsonHeaders())
    payload = parseJsonResponse(response)
    reference = extractLiveMapReference(payload)

    if not reference:
        raise ValueError("No live map reference returned from /live_map/")

    return reference, response


def updateMaps():
    """
    Get map data from OTTO and sync Fleet/Maps.
    """
    url = getApiBaseUrl() + "/maps/?offset=0&tagged=false"
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Maps/")

    try:
        response, data, errorResult = fetchListResource(
            url,
            ottoLogger,
            "Maps",
        )
        if errorResult is not None:
            return errorResult

        activeMapId = None
        try:
            activeMapId, _liveMapResponse = readLiveMapReference()
            ottoLogger.info("Otto API - ActiveMapID updated from /live_map/ to: " + str(activeMapId))
        except Exception as liveMapErr:
            ottoLogger.warn("Otto API - Failed to read /live_map/ reference: " + str(liveMapErr))
            try:
                # Keep a bounded fallback here so map sync still completes if the
                # dedicated live_map endpoint is temporarily unavailable.
                mostRecent = selectMostRecentMap(data)
                if mostRecent is not None:
                    activeMapId = mostRecent.get("id")
                    ottoLogger.warn("Otto API - Falling back to most recent map for ActiveMapID: " + str(activeMapId))
            except Exception as sortErr:
                ottoLogger.warn("Otto API - Failed to determine fallback active map: " + str(sortErr))

        return applyMapSync(data, response, activeMapId, ottoLogger)

    except Exception as e:
        ottoLogger.error("Otto API - /Maps/ Tag Update Failed - " + str(e))
        return buildSyncResult(False, "error", "Maps tag update failed - " + str(e))
