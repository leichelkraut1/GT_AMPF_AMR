from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseListPayload
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import deleteTagPath
from Otto_API.Common.TagHelpers import writeObservedTagValues


def buildSyncResult(ok, level, message, records=None, writes=None, value=None, **extra):
    records = list(records or [])
    writes = list(writes or [])
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "records": records,
            "writes": writes,
            "value": value,
        },
        records=records,
        writes=writes,
        **extra
    )


def listUdtInstanceNames(browseResults):
    """
    Return the names of browsed UDT instances only.
    """
    names = []
    for row in list(browseResults or []):
        if str(row.get("tagType")) == "UdtInstance":
            names.append(row.get("name"))
    return names


def writeObservedTagDict(tagDict, label, logger):
    """
    Write a tag/value dict through the observed bulk writer.
    """
    if not tagDict:
        return

    writeObservedTagValues(
        list(tagDict.keys()),
        list(tagDict.values()),
        labels=[label] * len(tagDict),
        logger=logger
    )


def cleanupStaleUdtInstances(
    basePath,
    wantedNames,
    logger,
    staleMessagePrefix,
    cleanupWarnPrefix="Otto API - Cleanup skipped due to error: ",
    skipNames=None,
):
    """
    Remove stale UDT instances under a collection path.
    """
    wantedNames = set(list(wantedNames or []))
    skipNames = set(list(skipNames or []))

    try:
        existingNames = listUdtInstanceNames(browseTagResults(basePath))
        for instanceName in existingNames:
            if instanceName in wantedNames or instanceName in skipNames:
                continue
            deleteTagPath(basePath + "/" + instanceName)
            logger.info(staleMessagePrefix + str(instanceName))
    except Exception as e:
        logger.warn(cleanupWarnPrefix + str(e))


def fetchListResource(url, logger, resourceLabel, responseWriter=None, parseErrorLabel=None):
    """
    Fetch and parse a standard OTTO list payload.

    Returns:
    - (responseText, parsedList, None) on success
    - (responseTextOrNone, None, errorResult) on failure
    """
    response = httpGet(url=url, headerValues=jsonHeaders())
    if responseWriter is not None:
        responseWriter(response)

    if not response:
        logger.error("Otto API - HTTP GET failed for /{}/".format(resourceLabel))
        return None, None, buildSyncResult(
            False,
            "error",
            "HTTP GET failed for /{}/".format(resourceLabel)
        )

    try:
        data = parseListPayload(response)
    except Exception as jsonErr:
        parseErrorLabel = parseErrorLabel or resourceLabel
        logger.error("Otto API - {} JSON decode error: {}".format(parseErrorLabel, jsonErr))
        return response, None, buildSyncResult(
            False,
            "error",
            "{} JSON decode error - {}".format(parseErrorLabel, jsonErr)
        )

    return response, data, None
