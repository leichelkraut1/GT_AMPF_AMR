WORKFLOW_CONFIG_HEADERS = [
    "WorkflowNumber",
    "RobotName",
    "MissionLabel",
    "TemplateName",
    "MissionType",
]

WORKFLOW_CONFIG_ROWS = [
    [900, "AMPF_AMR_RV2", "EP09_RobometSystems", "WF900_RobometDock", "Robot Service"],
    [602, "AMPF_AMR_RV2", "EP06-2_WetLabHandoff,Inner", "WF602_WetLabInnerDock", "Robot Service"],
    [300, "AMPF_AMR_RV1", "EP03_Diamondsaw", "WF300_DiamondSawDock", "Robot Service"],
    [400, "AMPF_AMR_RV1", "EP04_MTS_Cell", "WF400_MTSDock", "Robot Service"],
    [201, "AMPF_AMR_RV4", "EP02_TrakCart", "WF201_TrakPickup", "Cart Pickup"],
    [201, "AMPF_AMR_RV5", "EP02_TrakCart", "WF201_TrakPickup", "Cart Pickup"],
    [202, "AMPF_AMR_RV4", "EP02_TrakCart", "WF202_TrakDropoff", "Cart Dropoff"],
    [202, "AMPF_AMR_RV5", "EP02_TrakCart", "WF202_TrakDropoff", "Cart Dropoff"],
    [500, "AMPF_AMR_RV1", "EP05_HeatTreatments", "WF500_HeatTreatDock", "Robot Service"],
    [601, "AMPF_AMR_RV1", "EP06-1_WetlabHandoff,outer", "WF601_WetLabOuterDock", "Robot Service"],
    [1201, "AMPF_AMR_RV1", "EP12-1 MetLabHandoff,outer", "WF1201_MetLabOuterDock", "Robot Service"],
    [1202, "AMPF_AMR_RV3", "EP12-2 MetLabHandoff,Inner", "WF1202_MetLabInnerDock", "Robot Service"],
    [1300, "AMPF_AMR_RV3", "EP13_RigakuXPC/XRF Dock", "WF1300_RigakuDock", "Robot Service"],
    [1600, "AMPF_AMR_RV3", "EP16_FinalPuckStorage", "WF1600_FinalPuckStorage", "Robot Service"],
    [1901, "AMPF_AMR_RV4", "EP19_OptomecCart", "WF1901_OptomecPickup", "CartPickup"],
    [1901, "AMPF_AMR_RV5", "EP19_OptomecCart", "WF1901_OptomecPickup", "CartPickup"],
    [1902, "AMPF_AMR_RV4", "EP19_OptomecCart", "WF1902_OptomecDropoff", "CartDropoff"],
    [1902, "AMPF_AMR_RV5", "EP19_OptomecCart", "WF1902_OptomecDropoff", "CartDropoff"],
    [1910, "AMPF_AMR_RV4", "EP19_OptomecCart to EP02_TrakCart", "WF1910_OptoToTrak", "Cart Pickup then dropoff"],
    [1910, "AMPF_AMR_RV5", "EP19_OptomecCart to EP02_TrakCart", "WF1910_OptoToTrak", "Cart Pickup then dropoff"],
    [210, "AMPF_AMR_RV4", "EP02_TrakCart to EP19_OptomecCart", "WF210_TrakToOpto", "Cart Pickup then dropoff"],
    [210, "AMPF_AMR_RV5", "EP02_TrakCart to EP19_OptomecCart", "WF210_TrakToOpto", "Cart Pickup then dropoff"],
]


def getWorkflowConfigHeaders():
    return list(WORKFLOW_CONFIG_HEADERS)


def getWorkflowConfigRows():
    return [list(row) for row in list(WORKFLOW_CONFIG_ROWS)]
