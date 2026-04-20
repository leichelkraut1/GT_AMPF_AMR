from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureMemoryTag
from Otto_API.Common.TagHelpers import ensureUdtInstancePath
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import getFleetTriggersPath
from Otto_API.Common.TagHelpers import writeRequiredTagValues


CONTAINER1_ID = "SOMEID"
PLACE1_ID = "e1f2fcfe-caad-45fa-ab3d-db9bed61dfba"
CONTAINER1_TEMPLATE_PATH = "[Otto_FleetManager]Fleet/Containers/TestTemplates/Container1"


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


def ensureContainerTestTemplate():
    """
    Ensure the fixed test template container instance exists for container triggers.
    """
    ensureFolder(getFleetContainersPath())
    ensureFolder(getFleetContainersPath() + "/TestTemplates")
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
    for triggerName in [
        "CreateContainer1",
        "UpdateContainer1ToPlace1",
        "DeleteContainer1",
        "DeleteAtPlace1",
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
