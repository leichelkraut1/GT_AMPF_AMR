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
