from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import deleteTagPath
from Otto_API.Common.TagIO import writeObservedTagValues


def listUdtInstanceNames(browseResults):
    """
    Return the names of browsed UDT instances only.
    """
    names = []
    for row in list(browseResults or []):
        if str(row.get("tagType")) == "UdtInstance":
            names.append(row.get("name"))
    return names


def sanitizeTagName(text):
    """Convert mission/tag names into a safe Ignition tag name."""
    import re

    if text is None:
        return "None"
    sanitized = re.sub(r"[^A-Za-z0-9_-]", "_", str(text))
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "None"


def compactTagSuffix(rawId):
    """
    Build a shorter safe suffix for record identifiers used in tag names.
    For UUID-style ids, keep only the first segment so place instance paths stay
    readable on the gateway. This trades full-id uniqueness for brevity.
    """
    import re

    text = str(rawId or "").strip()
    if not text:
        return ""

    if re.match(r"^[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}$", text):
        return text.split("-", 1)[0]

    return sanitizeTagName(text)


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
