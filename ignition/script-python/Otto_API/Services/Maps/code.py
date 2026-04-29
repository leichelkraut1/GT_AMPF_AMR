from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.TagSync.Maps import applyMapSync
from Otto_API.WebAPI.Maps import fetchLiveMapReference
from Otto_API.WebAPI.Maps import fetchMaps


def _log():
    return system.util.getLogger("Otto_API.Services.Maps")


def _selectMostRecentMap(mapItems):
    """
    Select the map with the newest last_modified timestamp.
    """
    items = list(mapItems or [])
    if not items:
        return None

    return sorted(
        items,
        key=lambda item: str(item.get("last_modified", "1970-01-01T00:00:00Z")),
        reverse=True
    )[0]


def updateMaps():
    """
    Fetch OTTO map records and sync Fleet/Maps.
    """
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Maps/")

    try:
        apiBaseUrl = getApiBaseUrl()
        fetchResult = fetchMaps(apiBaseUrl)
        if not fetchResult.ok:
            return fetchResult.toDict()

        activeMapId = None
        try:
            liveMapResult = fetchLiveMapReference(apiBaseUrl)
            activeMapId = liveMapResult.value
            ottoLogger.info("Otto API - ActiveMapID updated from /live_map/ to: " + str(activeMapId))
        except Exception as liveMapErr:
            ottoLogger.warn("Otto API - Failed to read /live_map/ reference: " + str(liveMapErr))
            try:
                # Keep a bounded fallback here so map sync still completes if the
                # dedicated live_map endpoint is temporarily unavailable.
                mostRecent = _selectMostRecentMap(fetchResult.records)
                if mostRecent is not None:
                    activeMapId = mostRecent.get("id")
                    ottoLogger.warn("Otto API - Falling back to most recent map for ActiveMapID: " + str(activeMapId))
            except Exception as sortErr:
                ottoLogger.warn("Otto API - Failed to determine fallback active map: " + str(sortErr))

        responseText = str(fetchResult.data.get("response_text") or "")
        return applyMapSync(fetchResult.records, responseText, activeMapId, ottoLogger)

    except Exception as e:
        ottoLogger.error("Otto API - /Maps/ Tag Update Failed - " + str(e))
        return buildSyncResult(False, "error", "Maps tag update failed - " + str(e))
