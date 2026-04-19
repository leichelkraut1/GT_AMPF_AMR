from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureMemoryTag


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

    ensureFolder(triggersBase)
    ensureFolder(missionsBase)
    ensureFolder(createBase)
    ensureFolder(finalizeBase)
    ensureFolder(cancelBase)
    ensureFolder(systemUpdatesBase)

    createdPaths = []

    for workflowId in workflowIds:
        for robotId in robotIds:
            triggerPath = buildCreateMissionTriggerPath(createBase, workflowId, robotId)
            ensureMemoryTag(triggerPath, "Boolean", False)
            createdPaths.append(triggerPath)

    for robotId in robotIds:
        finalizePath = buildMissionTriggerPath(finalizeBase, "finalize_RV" + str(robotId))
        cancelPath = buildMissionTriggerPath(cancelBase, "cancel_RV" + str(robotId))
        ensureMemoryTag(finalizePath, "Boolean", False)
        ensureMemoryTag(cancelPath, "Boolean", False)
        createdPaths.append(finalizePath)
        createdPaths.append(cancelPath)

    cancelAllPath = buildMissionTriggerPath(cancelBase, "cancelAllActiveMissions")
    cancelAllFailedPath = buildMissionTriggerPath(cancelBase, "cancelAllFailedMissions")
    updateTriggersPath = buildMissionTriggerPath(systemUpdatesBase, "updateTriggers")
    ensureMemoryTag(cancelAllPath, "Boolean", False)
    ensureMemoryTag(cancelAllFailedPath, "Boolean", False)
    ensureMemoryTag(updateTriggersPath, "Boolean", False)
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
