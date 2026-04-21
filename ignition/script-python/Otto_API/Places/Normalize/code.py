from Otto_API.Common.SyncHelpers import compactTagSuffix
from Otto_API.Common.SyncHelpers import sanitizeTagName


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
