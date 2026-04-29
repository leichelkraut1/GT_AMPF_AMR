from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseServerStatus
from Otto_API.Models.Results import OperationalResult


class ServerStatusResult(OperationalResult):
    def __init__(
        self,
        ok,
        level,
        message,
        value=None,
        responseText=None,
        topLevelValue=False,
    ):
        self.value = value
        self.response_text = responseText

        dataFields = {"response_text": responseText}
        typedFields = {}
        if topLevelValue:
            typedFields["value"] = value
        else:
            dataFields["value"] = value

        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            typedFields=typedFields,
            dataFields=dataFields,
        )


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
            value=None,
            responseText=response,
        )

    try:
        status = parseServerStatus(response)
    except Exception as exc:
        return ServerStatusResult(
            False,
            "error",
            "Status update failed - {}".format(str(exc)),
            value=None,
            responseText=response,
        )

    return ServerStatusResult(
        True,
        "info",
        "Server status fetched",
        value=status,
        responseText=response,
        topLevelValue=True,
    )
