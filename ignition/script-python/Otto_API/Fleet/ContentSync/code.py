import json


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

    instanceName = placeRecord.get("name")
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
        valueWrites["{}/recipes/{}/Value".format(instancePath, recipeName)] = recipeValue
        boolWrites["{}/recipes/{}/Able".format(instancePath, recipeName)] = (
            1 if recipeValue is not None else 0
        )

    return valueWrites, boolWrites


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


def sanitizeTagName(text):
    """Convert mission/tag names into a safe Ignition tag name."""
    if text is None:
        return "None"
    return (
        str(text)
        .replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
        .replace(".", "_")
    )
