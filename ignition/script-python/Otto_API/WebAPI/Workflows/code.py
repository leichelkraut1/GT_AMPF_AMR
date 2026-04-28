from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseListPayload
from Otto_API.Models.Results import RecordSyncResult


def fetchWorkflows(apiBaseUrl, mapUuid, getFunc=httpGet):
    """
    Fetch workflow mission templates for one active OTTO map id.
    """
    url = str(apiBaseUrl or "").rstrip("/") + "/maps/mission_templates/?offset=0&map=" + str(mapUuid)
    response = getFunc(url=url, headerValues=jsonHeaders())

    if not response:
        return RecordSyncResult(
            False,
            "error",
            "HTTP GET failed for /Workflows/",
            records=[],
            dataFields={"response_text": response},
        )

    try:
        records = parseListPayload(response)
    except Exception as exc:
        return RecordSyncResult(
            False,
            "error",
            "Workflow JSON decode error - {}".format(exc),
            records=[],
            dataFields={"response_text": response},
        )

    return RecordSyncResult(
        True,
        "info",
        "Fetched {} workflow template(s)".format(len(records)),
        records=records,
        dataFields={"response_text": response},
    )
