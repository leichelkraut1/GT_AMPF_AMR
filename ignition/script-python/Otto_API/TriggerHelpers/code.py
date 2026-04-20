from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureMemoryTag
from Otto_API.Common.TagHelpers import ensureUdtInstancePath
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import getFleetTriggersPath
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import writeRequiredTagValues


CONTAINER1_ID = "SOMEID"
PLACE1_ID = "b54fab69-eae4-48a2-9e45-470c7da66ed2"
CONTAINER1_TEMPLATE_PATH = "[Otto_FleetManager]Fleet/Containers/Templates/Container1"


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


def buildContainerTemplatePath(containerTemplateName):
    """
    Build the full template UDT path from the configured template name.
    """
    return getFleetContainersPath() + "/Templates/" + str(containerTemplateName or "")


def readContainerTriggerTemplatePath():
    """
    Read the configured container template tag name and return the full template path.
    """
    templateName = readRequiredTagValue(
        getContainerTriggerConfigPath("ContainerTemplate"),
        "Container trigger template name"
    )
    return buildContainerTemplatePath(templateName)


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


def ensureContainerTestTemplate():
    """
    Ensure the fixed test template container instance exists for container triggers.
    This template is only used as a source for create calls, so seed only the
    base fields that createContainer actually reads.
    """
    ensureFolder(getFleetContainersPath())
    ensureFolder(getFleetContainersPath() + "/Templates")
    ensureUdtInstancePath(CONTAINER1_TEMPLATE_PATH, "api_Container")
    writeRequiredTagValues(
        [
            CONTAINER1_TEMPLATE_PATH + "/ContainerType",
            CONTAINER1_TEMPLATE_PATH + "/Description",
            CONTAINER1_TEMPLATE_PATH + "/Empty",
            CONTAINER1_TEMPLATE_PATH + "/Name",
        ],
        [
            "OTTO100_CART",
            "",
            False,
            "",
        ],
        labels=[
            "Container 1 template type",
            "Container 1 template description",
            "Container 1 template empty",
            "Container 1 template name",
        ],
    )
    return CONTAINER1_TEMPLATE_PATH


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
        ("ContainerTemplate", "Container1"),
        ("ContainerID", CONTAINER1_ID),
        ("PlaceID", PLACE1_ID),
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

    ensureContainerTestTemplate()
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
    ensureMemoryTag(cancelAllPath, "Boolean", False)
    ensureMemoryTag(cancelAllFailedPath, "Boolean", False)
    ensureMemoryTag(updateTriggersPath, "Boolean", False)
    createdPaths.append(cancelAllPath)
    createdPaths.append(cancelAllFailedPath)
    createdPaths.append(updateTriggersPath)

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
