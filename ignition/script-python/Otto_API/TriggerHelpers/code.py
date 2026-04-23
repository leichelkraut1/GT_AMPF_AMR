from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagPaths import getFleetContainersPath
from Otto_API.Common.TagPaths import getFleetTriggersPath
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureMemoryTag


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


def buildContainerTriggerPath(triggerBase, triggerName):
    """
    Build a container trigger path from a base folder and trigger name.
    """
    return buildMissionTriggerPath(triggerBase, triggerName)


def getContainerTriggerBasePath():
    """
    Return the base tag path for container test triggers.
    """
    return getFleetTriggersPath() + "/Containers"


def getContainerTriggerConfigPath(tagName):
    """
    Return the full tag path for one container trigger config tag.
    """
    return getContainerTriggerBasePath() + "/" + str(tagName)


def buildContainerTemplatePath(templateName):
    """Resolve a configured container template name into the Fleet/Containers template tag path."""
    templateName = str(templateName or "").strip()
    return getFleetContainersPath() + "/Templates/" + templateName


def readContainerTriggerTemplatePath():
    """
    Read the configured container template tag name and return its full tag path.
    """
    return buildContainerTemplatePath(readRequiredTagValue(
        getContainerTriggerConfigPath("ContainerTemplate"),
        "Container trigger template name"
    ))


def readContainerTriggerContainerId():
    """
    Read the configured container id used by container test triggers.
    """
    return readRequiredTagValue(
        getContainerTriggerConfigPath("ContainerID"),
        "Container trigger container id"
    )


def readContainerTriggerPlaceId():
    """
    Read the configured place id used by container test triggers.
    """
    return readRequiredTagValue(
        getContainerTriggerConfigPath("PlaceID"),
        "Container trigger place id"
    )


def ensureContainerTriggerTags():
    """
    Ensure the container trigger folder structure and Boolean memory tags exist.
    """
    triggersBase = getFleetTriggersPath()
    containersBase = triggersBase + "/Containers"

    ensureFolder(triggersBase)
    ensureFolder(containersBase)

    createdPaths = []
    for tagName, value in [
        ("ContainerTemplate", ""),
        ("ContainerID", ""),
        ("PlaceID", ""),
    ]:
        configPath = getContainerTriggerConfigPath(tagName)
        ensureMemoryTag(configPath, "String", value)
        createdPaths.append(configPath)

    for triggerName in [
        "CreateContainer1",
        "UpdateContainer1ToPlace1",
        "DeleteContainer1",
        "DeleteAtPlace1",
        "DeleteAllContainers",
    ]:
        triggerPath = buildContainerTriggerPath(containersBase, triggerName)
        ensureMemoryTag(triggerPath, "Boolean", False)
        createdPaths.append(triggerPath)

    return createdPaths


def ensureMissionTriggerTags(workflowIds=None, robotIds=None):
    """
    Ensure the mission trigger folder structure and Boolean memory tags exist.
    """
    workflowIds = list(workflowIds or [1, 2, 3, 4])
    robotIds = list(robotIds or [1, 2, 3, 4])

    triggersBase = getFleetTriggersPath()
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
    provisionControllerTagsPath = buildMissionTriggerPath(systemUpdatesBase, "ProvisionControllerTags")
    syncPlcFleetTagsPath = buildMissionTriggerPath(systemUpdatesBase, "UpdateAndCleanPLCTags")
    ensureMemoryTag(cancelAllPath, "Boolean", False)
    ensureMemoryTag(cancelAllFailedPath, "Boolean", False)
    ensureMemoryTag(updateTriggersPath, "Boolean", False)
    ensureMemoryTag(provisionControllerTagsPath, "Boolean", False)
    ensureMemoryTag(syncPlcFleetTagsPath, "Boolean", False)
    createdPaths.append(cancelAllPath)
    createdPaths.append(cancelAllFailedPath)
    createdPaths.append(updateTriggersPath)
    createdPaths.append(provisionControllerTagsPath)
    createdPaths.append(syncPlcFleetTagsPath)

    createdPaths.extend(ensureContainerTriggerTags())

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
