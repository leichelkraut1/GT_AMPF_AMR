from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseJsonResponse
from Otto_API.Common.ParseHelpers import parseListPayload
from Otto_API.Maps.Normalize import extractLiveMapReference
from Otto_API.Models.Results import RecordSyncResult


def fetchMaps(apiBaseUrl, getFunc=httpGet):
    """
    Fetch OTTO map records.
    """
    url = str(apiBaseUrl or "").rstrip("/") + "/maps/?offset=0&tagged=false"
    response = getFunc(url=url, headerValues=jsonHeaders())

    if not response:
        return RecordSyncResult(
            False,
            "error",
            "HTTP GET failed for /Maps/",
            records=[],
            dataFields={"response_text": response},
        )

    try:
        records = parseListPayload(response)
    except Exception as exc:
        return RecordSyncResult(
            False,
            "error",
            "Maps JSON decode error - {}".format(exc),
            records=[],
            dataFields={"response_text": response},
        )

    return RecordSyncResult(
        True,
        "info",
        "Fetched {} map record(s)".format(len(records)),
        records=records,
        dataFields={"response_text": response},
    )


def fetchLiveMapReference(apiBaseUrl, getFunc=httpGet):
    """
    Fetch the active OTTO map reference/id from the dedicated live_map endpoint.
    """
    url = str(apiBaseUrl or "").rstrip("/") + "/live_map/?fields=reference&offset=0&limit=100"
    response = getFunc(url=url, headerValues=jsonHeaders())
    payload = parseJsonResponse(response)
    reference = extractLiveMapReference(payload)

    if not reference:
        raise ValueError("No live map reference returned from /live_map/")

    return RecordSyncResult(
        True,
        "info",
        "Fetched live map reference",
        records=[],
        value=reference,
        dataFields={"response_text": response},
    )
