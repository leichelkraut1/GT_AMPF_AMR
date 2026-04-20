from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getFleetWorkflowsPath


WORKFLOW_DEFS = {
    201: {
        "workflow_number": 201,
        "allowed_robots": ["AMPF_AMR_RV4", "AMPF_AMR_RV5"],
        "place_name": "EP02_TrackCart",
        "template_name": "WF201_TrakPickup",
        "mission_type": "Cart Pickup",
        "exclusive": True,
    },
    202: {
        "workflow_number": 202,
        "allowed_robots": ["AMPF_AMR_RV4", "AMPF_AMR_RV5"],
        "place_name": "EP02_TrackCart",
        "template_name": "WF202_TrakDropoff",
        "mission_type": "Cart Dropoff",
        "exclusive": True,
    },
    300: {
        "workflow_number": 300,
        "allowed_robots": ["AMPF_AMR_RV1"],
        "place_name": "EP03_Diamondsaw",
        "template_name": "WF300_DiamondSawDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    400: {
        "workflow_number": 400,
        "allowed_robots": ["AMPF_AMR_RV1"],
        "place_name": "EP04_MTS_Cell",
        "template_name": "WF400_MTSDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    500: {
        "workflow_number": 500,
        "allowed_robots": ["AMPF_AMR_RV1"],
        "place_name": "EP05_HeatTreatments",
        "template_name": "WF500_HeatTreatDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    601: {
        "workflow_number": 601,
        "allowed_robots": ["AMPF_AMR_RV1"],
        "place_name": "EP06-1_WetlabHandoff,outer",
        "template_name": "WF601_WetLabOuterDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    602: {
        "workflow_number": 602,
        "allowed_robots": ["AMPF_AMR_RV2"],
        "place_name": "EP06-2_WetLabHandoff,Inner",
        "template_name": "WF602_WetLabInnerDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    900: {
        "workflow_number": 900,
        "allowed_robots": ["AMPF_AMR_RV2"],
        "place_name": "EP09_RobometSystems",
        "template_name": "WF900_RobometDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    1201: {
        "workflow_number": 1201,
        "allowed_robots": ["AMPF_AMR_RV1"],
        "place_name": "EP12-1 MetLabHandoff,outer",
        "template_name": "WF1201_MetLabOuterDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    1202: {
        "workflow_number": 1202,
        "allowed_robots": ["AMPF_AMR_RV3"],
        "place_name": "EP12-2 MetLabHandoff,Inner",
        "template_name": "WF1202_MetLabInnerDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    1300: {
        "workflow_number": 1300,
        "allowed_robots": ["AMPF_AMR_RV3"],
        "place_name": "EP13_RigakuXPC/XRF Dock",
        "template_name": "WF1300_RigakuDock",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    1600: {
        "workflow_number": 1600,
        "allowed_robots": ["AMPF_AMR_RV3"],
        "place_name": "EP16_FinalPuckStorage",
        "template_name": "WF1600_FinalPuckStorage",
        "mission_type": "Robot Service",
        "exclusive": True,
    },
    1901: {
        "workflow_number": 1901,
        "allowed_robots": ["AMPF_AMR_RV4", "AMPF_AMR_RV5"],
        "place_name": "EP19_OptomecCart",
        "template_name": "WF1901_OptomecPickup",
        "mission_type": "CartPickup",
        "exclusive": True,
    },
    1902: {
        "workflow_number": 1902,
        "allowed_robots": ["AMPF_AMR_RV4", "AMPF_AMR_RV5"],
        "place_name": "EP19_OptomecCart",
        "template_name": "WF1902_OptomecDropoff",
        "mission_type": "CartDropoff",
        "exclusive": True,
    },
}

ROBOT_NAMES = [
    "AMPF_AMR_RV1",
    "AMPF_AMR_RV2",
    "AMPF_AMR_RV3",
    "AMPF_AMR_RV4",
    "AMPF_AMR_RV5",
]


def normalizeWorkflowNumber(value):
    """Normalize PLC/workflow inputs so 0, blank, and invalid values all collapse to None."""
    try:
        number = int(value)
    except Exception:
        return None

    if number <= 0:
        return None
    return number


def getWorkflowDef(workflowNumber):
    """Return the static workflow definition for a workflow number."""
    workflowNumber = normalizeWorkflowNumber(workflowNumber)
    if workflowNumber is None:
        return None
    return WORKFLOW_DEFS.get(workflowNumber)


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


def buildMissionName(workflowNumber, robotName):
    """Build the mission name format that lets MainController recover the workflow number later."""
    workflowDef = getWorkflowDef(workflowNumber)
    if workflowDef is None:
        return None
    return "WF{}_{} with {}".format(
        workflowDef["workflow_number"],
        workflowDef["place_name"],
        robotName
    )
