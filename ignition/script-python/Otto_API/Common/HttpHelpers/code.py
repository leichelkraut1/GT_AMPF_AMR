import time

from Otto_API.Common.HttpLogPolicy import ensureHttpLogConfigTags
from Otto_API.Common.HttpLogPolicy import normalizedEndpointKey
from Otto_API.Common.HttpLogPolicy import parseEndpointList
from Otto_API.Common.RuntimeHistory import appendHttpHistoryRow
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.TagHelpers import getDisableLogOfMainCycleHttpPath
from Otto_API.Common.TagHelpers import getMainCycleEndpointsPath
from Otto_API.Common.TagHelpers import readOptionalTagValue


def jsonHeaders(extraHeaders=None):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if extraHeaders:
        headers.update(dict(extraHeaders))
    return headers

def _configuredMainCycleEndpoints():
    ensureHttpLogConfigTags()
    rawValue = readOptionalTagValue(getMainCycleEndpointsPath(), None, allowEmptyString=True)
    return parseEndpointList(rawValue)


def _shouldLogHttpHistory(method, url, ok):
    ensureHttpLogConfigTags()
    if not ok:
        return True

    normalizedMethod = str(method or "").strip().upper()
    if normalizedMethod != "GET":
        return True

    if not bool(readOptionalTagValue(getDisableLogOfMainCycleHttpPath(), False)):
        return True

    return normalizedEndpointKey(method, url) not in _configuredMainCycleEndpoints()


def _logHttpHistory(method, url, requestBody, responseBody, ok, startEpochMs, errorText=""):
    if not _shouldLogHttpHistory(method, url, ok):
        return
    endEpochMs = int(time.time() * 1000)
    appendHttpHistoryRow(
        timestampString(endEpochMs),
        method,
        url,
        requestBody,
        responseBody,
        ok,
        max(0, endEpochMs - int(startEpochMs or endEpochMs)),
        errorText,
    )


def httpGet(url, headerValues=None):
    startEpochMs = int(time.time() * 1000)
    try:
        response = system.net.httpGet(
            url=url,
            bypassCertValidation=True,
            headerValues=headerValues or jsonHeaders(),
        )
        _logHttpHistory("GET", url, "", response, True, startEpochMs)
        return response
    except Exception as exc:
        _logHttpHistory("GET", url, "", "", False, startEpochMs, str(exc))
        raise


def httpPost(url, postData, contentType="application/json", headerValues=None):
    startEpochMs = int(time.time() * 1000)
    try:
        response = system.net.httpPost(
            url=url,
            postData=postData,
            contentType=contentType,
            headerValues=headerValues or {"Accept": "application/json"},
            bypassCertValidation=True,
        )
        _logHttpHistory("POST", url, postData, response, True, startEpochMs)
        return response
    except Exception as exc:
        _logHttpHistory("POST", url, postData, "", False, startEpochMs, str(exc))
        raise
