import json

from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Models.Results import OperationalResult
from Otto_API.Models.Robots import RobotSystemStateEntry


class RobotFetchResult(OperationalResult):
    records = None
    response_text = None
    endpoint = ""


def _resultsUrl(apiBaseUrl, pathAndQuery):
    return str(apiBaseUrl or "").rstrip("/") + str(pathAndQuery or "")


def _fetchJsonResults(apiBaseUrl, pathAndQuery, endpoint, getFunc=httpGet):
    url = _resultsUrl(apiBaseUrl, pathAndQuery)
    response = getFunc(url=url, headerValues=jsonHeaders())
    endpoint = str(endpoint or "").strip()

    if not response:
        return RobotFetchResult(
            False,
            "error",
            "HTTP GET failed for {}".format(endpoint),
            dataFields={
                "records": [],
                "response_text": response,
                "endpoint": endpoint,
            },
        )

    try:
        data = json.loads(response)
    except Exception as exc:
        return RobotFetchResult(
            False,
            "error",
            "JSON decode error - {}".format(exc),
            dataFields={
                "records": [],
                "response_text": response,
                "endpoint": endpoint,
            },
        )

    return RobotFetchResult(
        True,
        "info",
        "{} fetched".format(endpoint),
        typedFields={"records": list(data.get("results", []) or [])},
        dataFields={
            "response_text": response,
            "endpoint": endpoint,
        },
    )


def fetchRobots(apiBaseUrl, getFunc=httpGet):
    return _fetchJsonResults(
        apiBaseUrl,
        "/robots/?fields=id,hostname,name,serial_number",
        "/robots/",
        getFunc=getFunc,
    )


def fetchRobotSystemStates(apiBaseUrl, getFunc=httpGet):
    result = _fetchJsonResults(
        apiBaseUrl,
        "/robots/states/?fields=%2A",
        "/robots/system_states/",
        getFunc=getFunc,
    )
    if not result.ok:
        return result
    return RobotFetchResult(
        True,
        "info",
        result.message,
        typedFields={
            "records": [
                RobotSystemStateEntry.fromDict(record)
                for record in list(result.records or [])
            ],
        },
        dataFields={
            "response_text": result.response_text,
            "endpoint": result.endpoint,
        },
    )


def fetchRobotActivities(apiBaseUrl, getFunc=httpGet):
    return _fetchJsonResults(
        apiBaseUrl,
        "/robots/activities/?fields=activity,robot&offset=0&limit=100",
        "/robots/activities/",
        getFunc=getFunc,
    )


def fetchRobotBatteries(apiBaseUrl, getFunc=httpGet):
    return _fetchJsonResults(
        apiBaseUrl,
        "/robots/batteries/?fields=percentage,robot",
        "/robots/batteries/",
        getFunc=getFunc,
    )


def fetchRobotPlaces(apiBaseUrl, getFunc=httpGet):
    return _fetchJsonResults(
        apiBaseUrl,
        "/robots/places/?fields=%2A&offset=0&limit=100",
        "/robots/places/",
        getFunc=getFunc,
    )
