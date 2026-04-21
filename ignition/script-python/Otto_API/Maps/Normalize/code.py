from Otto_API.Fleet.ContentSync import sanitizeTagName


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
    items = list(mapItems or [])
    if not items:
        return None

    return sorted(
        items,
        key=lambda item: str(item.get("last_modified", "1970-01-01T00:00:00Z")),
        reverse=True
    )[0]


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


def extractLiveMapReference(payload):
    """
    Extract the active map reference/id from the live_map endpoint payload.
    """
    if isinstance(payload, dict):
        if payload.get("reference"):
            return payload.get("reference")

        results = payload.get("results", [])
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict) and item.get("reference"):
                    return item.get("reference")
        return None

    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and item.get("reference"):
                return item.get("reference")

    return None
