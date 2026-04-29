from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagIO import writeTagValueAsync
from Otto_API.Common.TagPaths import getFleetPlacesPath
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.SyncHelpers import sanitizeTagName
from Otto_API.Common.SyncHelpers import writeObservedTagDict


def buildPlaceInstanceName(placeRecord):
    """
    Build a safe Ignition instance name for a place record.
    """
    placeName = placeRecord.get("name")

    if not placeName and not placeRecord.get("id"):
        return None

    return sanitizeTagName(placeName or "Place")


def normalizePlaceRecord(placeRecord):
    """
    Normalize a place record and skip TEMPLATE entries.
    """
    if placeRecord.get("place_type") == "TEMPLATE":
        return None

    instanceName = buildPlaceInstanceName(placeRecord)
    if not instanceName:
        return None

    recipes = placeRecord.get("recipes", {})
    if not isinstance(recipes, dict):
        recipes = {}

    return {
        "instance_name": instanceName,
        "recipes": recipes,
        "tag_values": {
            "/Container_Types_Supported": placeRecord.get("container_types_supported"),
            "/Created": placeRecord.get("created"),
            "/Description": placeRecord.get("description"),
            "/Enabled": placeRecord.get("enabled"),
            "/Exit_Recipe": placeRecord.get("exit_recipe"),
            "/Feature_Queue": placeRecord.get("feature_queue"),
            "/ID": placeRecord.get("id"),
            "/Metadata": placeRecord.get("metadata"),
            "/Name": placeRecord.get("name"),
            "/Ownership_Queue": placeRecord.get("ownership_queue"),
            "/Place_Groups": placeRecord.get("place_groups"),
            "/Place_Type": placeRecord.get("place_type"),
            "/Primary_Marker_ID": placeRecord.get("primary_marker_id"),
            "/Primary_Marker_Intent": placeRecord.get("primary_marker_intent"),
            "/Source_ID": placeRecord.get("source_id"),
            "/Zone": placeRecord.get("zone"),
        }
    }


def buildPlaceRecipeWrites(instancePath, recipes):
    """
    Build value and enabled writes for place recipes.
    """
    valueWrites = {}
    boolWrites = {}

    for recipeName, recipeValue in dict(recipes or {}).items():
        safeRecipeName = sanitizeTagName(recipeName)
        valueWrites["{}/recipes/{}/Value".format(instancePath, safeRecipeName)] = recipeValue
        boolWrites["{}/recipes/{}/Able".format(instancePath, safeRecipeName)] = (
            1 if recipeValue is not None else 0
        )

    return valueWrites, boolWrites


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

        writeObservedTagDict(tagDict, "Otto_API.TagSync.Places place sync", logger)
        writes.extend(tagDict.items())

        recipeValueWrites, recipeBoolWrites = buildPlaceRecipeWrites(
            instancePath,
            normalizedPlace["recipes"]
        )
        if recipeBoolWrites:
            writeObservedTagDict(
                recipeBoolWrites,
                "Otto_API.TagSync.Places place recipe bool sync",
                logger
            )
            writes.extend(recipeBoolWrites.items())

        if recipeValueWrites:
            writeObservedTagDict(
                recipeValueWrites,
                "Otto_API.TagSync.Places place recipe value sync",
                logger
            )
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
