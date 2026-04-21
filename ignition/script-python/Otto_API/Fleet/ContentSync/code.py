import json
import re


def listUdtInstanceNames(browseResults):
    """
    Return the names of browsed UDT instances only.
    """
    names = []
    for row in list(browseResults or []):
        if str(row.get("tagType")) == "UdtInstance":
            names.append(row.get("name"))
    return names


def buildUdtInstanceDef(instanceName, typeId):
    return {
        "name": instanceName,
        "typeID": typeId,
        "tagType": "UdtInstance"
    }


def buildRobotTagValues(basePath, robotRecord):
    """
    Build the tag value map for a robot record.
    """
    from Otto_API.Robots.Normalize import buildRobotTagValues as _buildRobotTagValues
    return _buildRobotTagValues(basePath, robotRecord)


def normalizePlaceRecord(placeRecord):
    """
    Normalize a place record and skip TEMPLATE entries.
    """
    from Otto_API.Places.Normalize import normalizePlaceRecord as _normalizePlaceRecord
    return _normalizePlaceRecord(placeRecord)


def buildPlaceRecipeWrites(instancePath, recipes):
    """
    Build value and enabled writes for place recipes.
    """
    from Otto_API.Places.Normalize import buildPlaceRecipeWrites as _buildPlaceRecipeWrites
    return _buildPlaceRecipeWrites(instancePath, recipes)


def buildPlaceInstanceName(placeRecord):
    """
    Build a safe unique Ignition instance name for a place record.
    """
    from Otto_API.Places.Normalize import buildPlaceInstanceName as _buildPlaceInstanceName
    return _buildPlaceInstanceName(placeRecord)


def compactTagSuffix(rawId):
    """
    Build a shorter safe suffix for record identifiers used in tag names.
    For UUID-style ids, keep only the first segment so place instance paths stay
    readable on the gateway. This trades full-id uniqueness for brevity.
    """
    text = str(rawId or "").strip()
    if not text:
        return ""

    if re.match(r"^[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}$", text):
        return text.split("-", 1)[0]

    return sanitizeTagName(text)


def buildMapInstanceName(mapItem):
    """
    Build the Ignition instance name for a map record.
    """
    return "{}_{}".format(
        sanitizeTagName(mapItem.get("name")),
        mapItem.get("revision")
    )


def selectMostRecentMap(mapItems):
    """
    Select the map with the newest last_modified timestamp.
    """
    from Otto_API.Maps.Normalize import selectMostRecentMap as _selectMostRecentMap
    return _selectMostRecentMap(mapItems)


def buildMapTagValues(basePath, mapItem):
    """
    Build the tag value map for a map record.
    """
    from Otto_API.Maps.Normalize import buildMapTagValues as _buildMapTagValues
    return _buildMapTagValues(basePath, mapItem)


def buildWorkflowTagValues(basePath, templateItem):
    """
    Build the tag value map for a workflow / mission template record.
    """
    from Otto_API.Workflows.Normalize import buildWorkflowTagValues as _buildWorkflowTagValues
    return _buildWorkflowTagValues(basePath, templateItem)


def normalizeContainerRecord(containerRecord):
    """
    Normalize an OTTO container record for Fleet/Containers sync.
    """
    from Otto_API.Containers.Normalize import normalizeContainerRecord as _normalizeContainerRecord
    return _normalizeContainerRecord(containerRecord)


def sanitizeTagName(text):
    """Convert mission/tag names into a safe Ignition tag name."""
    if text is None:
        return "None"
    sanitized = re.sub(r"[^A-Za-z0-9_-]", "_", str(text))
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "None"
