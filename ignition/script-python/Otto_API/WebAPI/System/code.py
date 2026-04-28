from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseServerStatus
from Otto_API.Models.Results import OperationalResult


class ServerStatusResult(OperationalResult):
    value = None
    response_text = None


def fetchServerStatus(apiBaseUrl, getFunc=httpGet):
    """
    Fetch Fleet Manager server state.
    """
    url = str(apiBaseUrl or "").rstrip("/") + "/system/state/"
    response = getFunc(url=url, headerValues=jsonHeaders())

    if not response:
        return ServerStatusResult(
            False,
            "warn",
            "Otto Fleet Manager did not respond",
            dataFields={
                "value": None,
                "response_text": response,
            },
        )

    try:
        status = parseServerStatus(response)
    except Exception as exc:
        return ServerStatusResult(
            False,
            "error",
            "Status update failed - {}".format(str(exc)),
            dataFields={
                "value": None,
                "response_text": response,
            },
        )

    return ServerStatusResult(
        True,
        "info",
        "Server status fetched",
        typedFields={"value": status},
        dataFields={"response_text": response},
    )
