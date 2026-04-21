import json


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
