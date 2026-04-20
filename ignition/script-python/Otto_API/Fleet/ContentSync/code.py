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
    instanceName = robotRecord.get("name")
    if not instanceName:
        return None, {}

    instancePath = basePath + "/" + instanceName
    return instanceName, {
        instancePath + "/Hostname": robotRecord.get("hostname"),
        instancePath + "/ID": robotRecord.get("id"),
        instancePath + "/SerialNum": robotRecord.get("serial_number"),
    }


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


def buildPlaceInstanceName(placeRecord):
    """
    Build a safe unique Ignition instance name for a place record.
    """
    placeName = placeRecord.get("name")
    placeId = placeRecord.get("id")

    if not placeName and not placeId:
        return None

    safeName = sanitizeTagName(placeName or "Place")
    safeId = compactTagSuffix(placeId)

    if safeId:
        return "{}_{}".format(safeName, safeId)
    return safeName


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


def buildWorkflowTagValues(basePath, templateItem):
    """
    Build the tag value map for a workflow / mission template record.
    """
    instanceName = templateItem.get("name")
    if not instanceName:
        return None, {}

    instancePath = basePath + "/" + instanceName
    return instanceName, {
        instancePath + "/ID": templateItem.get("id"),
        instancePath + "/Description": templateItem.get("description", ""),
        instancePath + "/Priority": templateItem.get("priority", 0),
        instancePath + "/NominalDuration": templateItem.get("nominal_duration"),
        instancePath + "/MaxDuration": templateItem.get("max_duration"),
        instancePath + "/RobotTeam": templateItem.get("robot_team"),
        instancePath + "/OverridePrompts": templateItem.get("override_prompts_json"),
        instancePath + "/jsonString": json.dumps(templateItem)
    }


def normalizeContainerRecord(containerRecord):
    """
    Normalize an OTTO container record for Fleet/Containers sync.
    """
    containerId = containerRecord.get("id")
    if containerId is None or not str(containerId).strip():
        return None

    return {
        "instance_name": str(containerId).strip(),
        "tag_values": {
            "/ID": containerRecord.get("id"),
            "/ContainerType": containerRecord.get("container_type"),
            "/Created": containerRecord.get("created"),
            "/Description": containerRecord.get("description"),
            "/Empty": containerRecord.get("empty"),
            "/Name": containerRecord.get("name"),
            "/Place": containerRecord.get("place"),
            "/ReservedAt": containerRecord.get("reserved_at"),
            "/ReservedBy": containerRecord.get("reserved_by"),
            "/Robot": containerRecord.get("robot"),
            "/State": containerRecord.get("state"),
            "/SystemCreated": containerRecord.get("system_created"),
            "/jsonString": json.dumps(containerRecord),
        }
    }


def sanitizeTagName(text):
    """Convert mission/tag names into a safe Ignition tag name."""
    if text is None:
        return "None"
    sanitized = re.sub(r"[^A-Za-z0-9_-]", "_", str(text))
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "None"
