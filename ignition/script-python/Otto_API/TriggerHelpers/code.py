def buildMissionTriggerPath(triggerBase, triggerName):
    """
    Build a mission trigger path from a base folder and trigger name.
    """
    return str(triggerBase).rstrip("/") + "/" + str(triggerName)


def buildCreateMissionTriggerPath(triggerBase, workflowId, robotId):
    """
    Build a mission-create trigger path for a workflow/robot pair.
    """
    return buildMissionTriggerPath(
        triggerBase,
        "create_WF{}_RV{}".format(workflowId, robotId)
    )


def buildTriggerPath(triggerBase, workflowId, robotId):
    """
    Backward-compatible wrapper for the old create-trigger helper name.
    Prefer buildCreateMissionTriggerPath(...) for new code.
    """
    return buildCreateMissionTriggerPath(triggerBase, workflowId, robotId)


def _splitTagPath(path):
    if "/" in path:
        return path.rsplit("/", 1)

    if "]" in path:
        providerPath, childName = path.split("]", 1)
        return providerPath + "]", childName

    raise ValueError("Unsupported tag path: {}".format(path))


def _ensureFolder(path):
    parentPath, name = _splitTagPath(path)
    system.tag.configure(
        parentPath,
        [{"name": name, "tagType": "Folder"}],
        "a"
    )


def _memoryTagDef(name, dataType, value=None):
    tagDef = {
        "name": name,
        "tagType": "AtomicTag",
        "valueSource": "memory",
        "dataType": dataType,
    }
    if value is not None:
        tagDef["value"] = value
    return tagDef


def _ensureBooleanTag(path, initialValue=False):
    parentPath, name = _splitTagPath(path)
    system.tag.configure(
        parentPath,
        [_memoryTagDef(name, "Boolean", bool(initialValue))],
        "a"
    )
    system.tag.writeBlocking([path], [bool(initialValue)])


def ensureMissionTriggerTags(workflowIds=None, robotIds=None):
    """
    Ensure the mission trigger folder structure and Boolean memory tags exist.
    """
    workflowIds = list(workflowIds or [1, 2, 3, 4])
    robotIds = list(robotIds or [1, 2, 3, 4])

    triggersBase = "[Otto_FleetManager]Triggers"
    missionsBase = triggersBase + "/Missions"
    createBase = missionsBase + "/Create"
    finalizeBase = missionsBase + "/Finalize"
    cancelBase = missionsBase + "/Cancel"
    systemUpdatesBase = triggersBase + "/SystemUpdates"

    _ensureFolder(triggersBase)
    _ensureFolder(missionsBase)
    _ensureFolder(createBase)
    _ensureFolder(finalizeBase)
    _ensureFolder(cancelBase)
    _ensureFolder(systemUpdatesBase)

    createdPaths = []

    for workflowId in workflowIds:
        for robotId in robotIds:
            triggerPath = buildCreateMissionTriggerPath(createBase, workflowId, robotId)
            _ensureBooleanTag(triggerPath, False)
            createdPaths.append(triggerPath)

    for robotId in robotIds:
        finalizePath = buildMissionTriggerPath(finalizeBase, "finalize_RV" + str(robotId))
        cancelPath = buildMissionTriggerPath(cancelBase, "cancel_RV" + str(robotId))
        _ensureBooleanTag(finalizePath, False)
        _ensureBooleanTag(cancelPath, False)
        createdPaths.append(finalizePath)
        createdPaths.append(cancelPath)

    cancelAllPath = buildMissionTriggerPath(cancelBase, "cancelAllActiveMissions")
    cancelAllFailedPath = buildMissionTriggerPath(cancelBase, "cancelAllFailedMissions")
    updateTriggersPath = buildMissionTriggerPath(systemUpdatesBase, "updateTriggers")
    _ensureBooleanTag(cancelAllPath, False)
    _ensureBooleanTag(cancelAllFailedPath, False)
    _ensureBooleanTag(updateTriggersPath, False)
    createdPaths.append(cancelAllPath)
    createdPaths.append(cancelAllFailedPath)
    createdPaths.append(updateTriggersPath)

    return createdPaths


def extractRobotIdFromMissionName(missionName, robotIds):
    """
    Extract an RV token from a mission name using the provided robot id list.
    """
    missionName = str(missionName or "")
    for robotId in list(robotIds or []):
        token = "RV" + str(robotId)
        if token in missionName:
            return robotId
    return None
