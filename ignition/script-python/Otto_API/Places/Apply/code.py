from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagIO import writeTagValueAsync
from Otto_API.Common.TagPaths import getFleetPlacesPath
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.SyncHelpers import writeObservedTagDict
from Otto_API.Places.Normalize import buildPlaceRecipeWrites
from Otto_API.Places.Normalize import normalizePlaceRecord


def _duplicateInstanceNames(normalizedPlaces):
    seenNames = set()
    duplicateNames = []

    for normalizedPlace in list(normalizedPlaces or []):
        instanceName = str(normalizedPlace.get("instance_name") or "").strip()
        if not instanceName:
            continue
        if instanceName in seenNames and instanceName not in duplicateNames:
            duplicateNames.append(instanceName)
            continue
        seenNames.add(instanceName)

    return duplicateNames


def applyPlaceSync(placeRecords, responseText, logger):
    """
    Apply normalized place data to Fleet/Places and remove stale instances.
    """
    basePath = getFleetPlacesPath()
    writeTagValueAsync(basePath + "/jsonString", responseText)

    normalizedPlaces = []
    for place in list(placeRecords or []):
        normalizedPlace = normalizePlaceRecord(place)
        if normalizedPlace is None:
            continue
        normalizedPlaces.append(normalizedPlace)

    duplicateNames = _duplicateInstanceNames(normalizedPlaces)
    if duplicateNames:
        message = "Duplicate sanitized place names returned by OTTO: {}".format(
            ", ".join(sorted(duplicateNames))
        )
        logger.error("Otto API - " + message)
        cleanupStaleUdtInstances(
            basePath,
            [],
            logger,
            "Otto API - Removed place tag instance due to duplicate sanitized place names: ",
        )
        return buildSyncResult(
            False,
            "error",
            message,
            records=placeRecords,
            writes=[],
            duplicate_instance_names=list(sorted(duplicateNames)),
        )

    apiPlaces = []
    writes = []

    for normalizedPlace in normalizedPlaces:
        instanceName = normalizedPlace["instance_name"]
        apiPlaces.append(instanceName)
        instancePath = basePath + "/" + instanceName

        exists = tagExists(instancePath)

        if not exists:
            ensureUdtInstancePath(instancePath, "api_Place")
            logger.info("Otto API - Created new place tag instance: " + instanceName)

        tagDict = {}
        for suffix, value in normalizedPlace["tag_values"].items():
            tagDict[instancePath + suffix] = value

        writeObservedTagDict(tagDict, "Otto_API.Places.Apply place sync", logger)
        writes.extend(tagDict.items())

        recipeValueWrites, recipeBoolWrites = buildPlaceRecipeWrites(
            instancePath,
            normalizedPlace["recipes"]
        )
        if recipeBoolWrites:
            writeObservedTagDict(recipeBoolWrites, "Otto_API.Places.Apply place recipe bool sync", logger)
            writes.extend(recipeBoolWrites.items())

        if recipeValueWrites:
            writeObservedTagDict(recipeValueWrites, "Otto_API.Places.Apply place recipe value sync", logger)
            writes.extend(recipeValueWrites.items())

    cleanupStaleUdtInstances(
        basePath,
        apiPlaces,
        logger,
        "Otto API - Removed stale place tag instance: ",
    )

    return buildSyncResult(
        True,
        "info",
        "Places updated for {} instance(s)".format(len(apiPlaces)),
        records=placeRecords,
        writes=writes
    )
