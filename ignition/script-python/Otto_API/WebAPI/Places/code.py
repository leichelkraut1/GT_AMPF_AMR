from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseListPayload
from Otto_API.Models.Results import RecordSyncResult


def fetchPlaces(apiBaseUrl, getFunc=httpGet):
    """
    Fetch OTTO place records.
    """
    url = str(apiBaseUrl or "").rstrip("/") + "/places/"
    response = getFunc(url=url, headerValues=jsonHeaders())

    if not response:
        return RecordSyncResult(
            False,
            "error",
            "HTTP GET failed for /Places/",
            records=[],
            dataFields={"response_text": response},
        )

    try:
        records = parseListPayload(response)
    except Exception as exc:
        return RecordSyncResult(
            False,
            "error",
            "Places JSON decode error - {}".format(exc),
            records=[],
            dataFields={"response_text": response},
        )

    return RecordSyncResult(
        True,
        "info",
        "Fetched {} place record(s)".format(len(records)),
        records=records,
        dataFields={"response_text": response},
    )
