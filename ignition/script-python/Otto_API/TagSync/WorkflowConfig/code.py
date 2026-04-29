from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.TagPaths import getFleetWorkflowsPath
from Otto_API.Common.TagPaths import getWorkflowConfigPath
from Otto_API.Common.TagProvisioning import ensureBaseFleetConfigTags
from Otto_API.Common.TagProvisioning import ensureMemoryTag
from Otto_API.Models.Fleet import getWorkflowConfigHeaders
from Otto_API.Models.Fleet import getWorkflowConfigRows
from Otto_API.Models.Fleet import groupWorkflowRows
from Otto_API.Models.Fleet import iterWorkflowConfigRows
from Otto_API.Models.Fleet import MISSION_NAME_MAX_LENGTH
from Otto_API.Models.Fleet import normalizeWorkflowNumber
from Otto_API.Models.Fleet import ROBOT_NAMES
from Otto_API.Models.Fleet import sanitizeMissionNameToken
from Otto_API.Models.Fleet import shortRobotToken


def _defaultWorkflowConfigDataset():
    return system.dataset.toDataSet(
        getWorkflowConfigHeaders(),
        getWorkflowConfigRows(),
    )


def ensureWorkflowConfigTag():
    """Provision the shared workflow config tag from OTTO-owned defaults."""
    ensureBaseFleetConfigTags()
    ensureMemoryTag(
        getWorkflowConfigPath(),
        "DataSet",
        _defaultWorkflowConfigDataset(),
    )


def _readWorkflowDatasetValue():
    datasetValue = readOptionalTagValue(getWorkflowConfigPath(), None)
    if hasattr(datasetValue, "getRowCount"):
        return datasetValue

    ensureWorkflowConfigTag()
    datasetValue = readOptionalTagValue(getWorkflowConfigPath(), None)
    if hasattr(datasetValue, "getRowCount"):
        return datasetValue

    return _defaultWorkflowConfigDataset()


def getWorkflowDefs():
    return groupWorkflowRows(iterWorkflowConfigRows(_readWorkflowDatasetValue()))


def getWorkflowDef(workflowNumber):
    workflowNumber = normalizeWorkflowNumber(workflowNumber)
    if workflowNumber is None:
        return None
    return getWorkflowDefs().get(workflowNumber)


def isWorkflowAllowedForRobot(workflowNumber, robotName):
    workflowDef = getWorkflowDef(workflowNumber)
    if workflowDef is None:
        return False
    return robotName in workflowDef.get("allowed_robots", [])


def workflowTemplateTagPath(workflowNumber):
    workflowDef = getWorkflowDef(workflowNumber)
    if workflowDef is None:
        return None
    return getFleetWorkflowsPath() + "/{}/jsonString".format(
        workflowDef["template_name"]
    )


def robotIdTagPath(robotName):
    return getFleetRobotsPath() + "/{}/ID".format(robotName)


def buildMissionName(workflowNumber, robotName):
    workflowDef = getWorkflowDef(workflowNumber)
    if workflowDef is None:
        return None

    workflowToken = "WF{}".format(workflowDef["workflow_number"])
    robotToken = sanitizeMissionNameToken(shortRobotToken(robotName))
    missionLabelToken = sanitizeMissionNameToken(workflowDef["mission_label"])

    reservedLength = len(workflowToken) + len(robotToken) + 2
    maxLabelLength = max(8, MISSION_NAME_MAX_LENGTH - reservedLength)
    if len(missionLabelToken) > maxLabelLength:
        missionLabelToken = missionLabelToken[:maxLabelLength].rstrip("_")

    missionName = "{}_{}_{}".format(
        workflowToken,
        missionLabelToken,
        robotToken,
    )
    return missionName[:MISSION_NAME_MAX_LENGTH].rstrip("_")
