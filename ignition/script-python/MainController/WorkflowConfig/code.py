import re

from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getWorkflowConfigPath
from Otto_API.Common.TagHelpers import getFleetWorkflowsPath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import ensureFleetConfigTags
from MainController.WorkflowConfigSeed import getWorkflowConfigHeaders
from MainController.WorkflowConfigSeed import getWorkflowConfigRows


ROBOT_NAMES = [
    "AMPF_AMR_RV1",
    "AMPF_AMR_RV2",
    "AMPF_AMR_RV3",
    "AMPF_AMR_RV4",
    "AMPF_AMR_RV5",
]

MISSION_NAME_MAX_LENGTH = 64
MISSION_NAME_TOKEN_RE = re.compile(r"[^A-Za-z0-9]+")


def normalizeWorkflowNumber(value):
    """Normalize PLC/workflow inputs so 0, blank, and invalid values all collapse to None."""
    try:
        number = int(value)
    except Exception:
        return None

    if number <= 0:
        return None
    return number


def _defaultWorkflowConfigDataset():
    return system.dataset.toDataSet(
        getWorkflowConfigHeaders(),
        getWorkflowConfigRows(),
    )


def _iterWorkflowConfigRows(datasetValue):
    if not hasattr(datasetValue, "getRowCount"):
        return []

    headers = list(datasetValue.getColumnNames() or [])
    rows = []
    for rowIndex in range(datasetValue.getRowCount()):
        row = {}
        for header in headers:
            row[str(header)] = datasetValue.getValueAt(rowIndex, header)
        rows.append(row)
    return rows


def _groupWorkflowRows(rows):
    grouped = {}

    for row in list(rows or []):
        workflowNumber = normalizeWorkflowNumber(row.get("WorkflowNumber"))
        robotName = str(row.get("RobotName") or "").strip()
        if workflowNumber is None or not robotName:
            continue

        workflowDef = grouped.get(workflowNumber)
        if workflowDef is None:
            workflowDef = {
                "workflow_number": workflowNumber,
                "allowed_robots": [],
                "mission_label": str(row.get("MissionLabel") or "").strip(),
                "template_name": str(row.get("TemplateName") or "").strip(),
                "mission_type": str(row.get("MissionType") or "").strip(),
            }
            grouped[workflowNumber] = workflowDef

        if robotName not in workflowDef["allowed_robots"]:
            workflowDef["allowed_robots"].append(robotName)

    for workflowDef in list(grouped.values()):
        workflowDef["allowed_robots"] = sorted(list(workflowDef["allowed_robots"]))

    return grouped


def _readWorkflowDatasetValue():
    datasetValue = readOptionalTagValue(getWorkflowConfigPath(), None)
    if hasattr(datasetValue, "getRowCount"):
        return datasetValue

    ensureFleetConfigTags()
    datasetValue = readOptionalTagValue(getWorkflowConfigPath(), None)
    if hasattr(datasetValue, "getRowCount"):
        return datasetValue

    return _defaultWorkflowConfigDataset()


def getWorkflowDefs():
    return _groupWorkflowRows(_iterWorkflowConfigRows(_readWorkflowDatasetValue()))


def getWorkflowDef(workflowNumber):
    """Return the configured workflow definition for a workflow number."""
    workflowNumber = normalizeWorkflowNumber(workflowNumber)
    if workflowNumber is None:
        return None
    return getWorkflowDefs().get(workflowNumber)


def isWorkflowAllowedForRobot(workflowNumber, robotName):
    """Validate that a workflow is configured for the requested robot."""
    workflowDef = getWorkflowDef(workflowNumber)
    if workflowDef is None:
        return False
    return robotName in workflowDef.get("allowed_robots", [])


def workflowTemplateTagPath(workflowNumber):
    """Resolve the Fleet workflow template tag used for mission creation."""
    workflowDef = getWorkflowDef(workflowNumber)
    if workflowDef is None:
        return None
    return getFleetWorkflowsPath() + "/{}/jsonString".format(
        workflowDef["template_name"]
    )


def robotIdTagPath(robotName):
    """Resolve the Fleet robot-id tag consumed by OTTO mission create calls."""
    return getFleetRobotsPath() + "/{}/ID".format(robotName)


def _sanitizeMissionNameToken(value):
    token = MISSION_NAME_TOKEN_RE.sub("_", str(value or "").strip())
    token = token.strip("_")
    return token or "Unknown"


def _shortRobotToken(robotName):
    text = str(robotName or "").strip()
    if "_" in text:
        return text.split("_")[-1]
    return text or "Robot"


def buildMissionName(workflowNumber, robotName):
    """
    Build a compact mission name that stays under OTTO validation limits.

    The name must keep the leading workflow number for later parsing, while still
    carrying enough place context to be useful in logs and the UI.
    """
    workflowDef = getWorkflowDef(workflowNumber)
    if workflowDef is None:
        return None

    workflowToken = "WF{}".format(workflowDef["workflow_number"])
    robotToken = _sanitizeMissionNameToken(_shortRobotToken(robotName))
    missionLabelToken = _sanitizeMissionNameToken(workflowDef["mission_label"])

    reservedLength = len(workflowToken) + len(robotToken) + 2
    maxLabelLength = max(8, MISSION_NAME_MAX_LENGTH - reservedLength)
    if len(missionLabelToken) > maxLabelLength:
        missionLabelToken = missionLabelToken[:maxLabelLength].rstrip("_")

    missionName = "{}_{}_{}".format(
        workflowToken,
        missionLabelToken,
        robotToken
    )
    return missionName[:MISSION_NAME_MAX_LENGTH].rstrip("_")
