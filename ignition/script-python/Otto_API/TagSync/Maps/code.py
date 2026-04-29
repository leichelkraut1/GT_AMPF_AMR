from Otto_API.Common.SyncHelpers import sanitizeTagName
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagIO import writeTagValue
from Otto_API.Common.TagIO import writeTagValueAsync
from Otto_API.Common.TagPaths import getFleetMapsPath
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.SyncHelpers import writeObservedTagDict


def buildMapInstanceName(mapItem):
    """
    Build the Ignition instance name for a map record.
    """
    return "{}_{}".format(
        sanitizeTagName(mapItem.get("name")),
        mapItem.get("revision")
    )


def buildMapTagValues(basePath, mapItem):
    """
    Build the tag value map for a map record.
    """
    instanceName = buildMapInstanceName(mapItem)
    instancePath = basePath + "/" + instanceName

    return instanceName, {
        instancePath + "/ID": mapItem.get("id"),
        instancePath + "/Last_Modified": mapItem.get("last_modified"),
        instancePath + "/Created": mapItem.get("created"),
        instancePath + "/Name": mapItem.get("name"),
        instancePath + "/Description": mapItem.get("description"),
        instancePath + "/Project": mapItem.get("project"),
        instancePath + "/Tag": mapItem.get("tag"),
        instancePath + "/Cached": mapItem.get("cached"),
        instancePath + "/Disabled": mapItem.get("disabled"),
        instancePath + "/User_ID": mapItem.get("user_id"),
        instancePath + "/Author": mapItem.get("author"),
        instancePath + "/Revision": mapItem.get("revision"),
        instancePath + "/Tag_Index": mapItem.get("tag_index"),
        instancePath + "/Source_Map": mapItem.get("source_map")
    }


def applyMapSync(mapItems, responseText, activeMapId, logger):
    """
    Apply map records to Fleet/Maps and keep ActiveMapID in sync.
    """
    basePath = getFleetMapsPath()
    writeTagValue(basePath + "/updateResponse", responseText)
    writeTagValueAsync(basePath + "/jsonString", responseText)

    activeMapTag = basePath + "/ActiveMapID"
    apiMaps = []
    writes = []

    if activeMapId is not None:
        writeTagValue(activeMapTag, activeMapId)
        writes.append((activeMapTag, activeMapId))

    for mapItem in list(mapItems or []):
        instanceName, tagDict = buildMapTagValues(basePath, mapItem)
        apiMaps.append(instanceName)
        instancePath = basePath + "/" + instanceName

        exists = tagExists(instancePath)

        if not exists:
            ensureUdtInstancePath(instancePath, "api_Map")
            logger.info("Otto API - Created new map tag instance: " + instanceName)

        writeObservedTagDict(tagDict, "Otto_API.TagSync.Maps map sync", logger)
        writes.extend(tagDict.items())

    cleanupStaleUdtInstances(
        basePath,
        apiMaps,
        logger,
        "Otto API - Removed stale map tag instance: ",
        skipNames=["ActiveMapID"],
    )

    return buildSyncResult(
        True,
        "info",
        "Maps updated for {} instance(s)".format(len(apiMaps)),
        records=mapItems,
        writes=writes,
        value=activeMapId
    )
