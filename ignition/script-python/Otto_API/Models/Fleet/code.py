import re

from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import coerceBool
from Otto_API.Common.RecordHelpers import coerceFloatOrNone
from Otto_API.Common.RecordHelpers import coerceIntOrNone
from Otto_API.Common.RecordHelpers import coerceText
from Otto_API.Common.RecordHelpers import coerceUpperText


ROBOT_NAMES = [
    "AMPF_AMR_RV1",
    "AMPF_AMR_RV2",
    "AMPF_AMR_RV3",
    "AMPF_AMR_RV4",
    "AMPF_AMR_RV5",
]

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

MISSION_NAME_MAX_LENGTH = 64
MISSION_NAME_TOKEN_RE = re.compile(r"[^A-Za-z0-9]+")


def getWorkflowConfigHeaders():
    return list(WORKFLOW_CONFIG_HEADERS)


def getWorkflowConfigRows():
    return [list(row) for row in list(WORKFLOW_CONFIG_ROWS)]


def normalizeWorkflowNumber(value):
    """Normalize PLC/workflow inputs so 0, blank, and invalid values all collapse to None."""
    try:
        number = int(value)
    except Exception:
        return None

    if number <= 0:
        return None
    return number


def iterWorkflowConfigRows(datasetValue):
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


def groupWorkflowRows(rows):
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


def sanitizeMissionNameToken(value):
    token = MISSION_NAME_TOKEN_RE.sub("_", str(value or "").strip())
    token = token.strip("_")
    return token or "Unknown"


def shortRobotToken(robotName):
    text = str(robotName or "").strip()
    if "_" in text:
        return text.split("_")[-1]
    return text or "Robot"


def _normalizedActivityStates(allowedActivityStates):
    normalized = []
    seen = set()
    for value in list(allowedActivityStates or []):
        text = coerceUpperText(value, None)
        if text is None or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


class RobotReadinessContext(MappingRecordBase):
    FIELDS = (
        "min_charge",
        "charging_delay_ms",
        "mission_last_update_ts",
        "mission_last_update_success",
        "allowed_activity_states",
    )

    def __init__(
        self,
        minCharge,
        chargingDelayMs=None,
        missionLastUpdateTs=None,
        missionLastUpdateSuccess=None,
        allowedActivityStates=None,
    ):
        self.min_charge = coerceFloatOrNone(minCharge)
        self.charging_delay_ms = coerceIntOrNone(chargingDelayMs)
        self.mission_last_update_ts = coerceText(missionLastUpdateTs, None)
        self.mission_last_update_success = coerceBool(missionLastUpdateSuccess, False)
        self.allowed_activity_states = _normalizedActivityStates(allowedActivityStates)

    @classmethod
    def fromDict(cls, record):
        if isinstance(record, cls):
            return record
        record = dict(record or {})
        return cls(
            record.get("min_charge"),
            record.get("charging_delay_ms"),
            record.get("mission_last_update_ts"),
            record.get("mission_last_update_success"),
            record.get("allowed_activity_states"),
        )


class RobotReadinessResult(MappingRecordBase):
    FIELDS = (
        "robot_name",
        "available",
        "reason",
        "system_state",
        "activity_state",
        "charge_level",
        "min_charge",
        "active_mission_count",
        "failed_mission_count",
        "charging_tof",
        "charging_ts",
        "charging_delay_ms",
        "mission_last_update_ts",
        "mission_last_update_success",
    )

    def __init__(
        self,
        robotName,
        available,
        reason,
        systemState=None,
        activityState=None,
        chargeLevel=None,
        minCharge=None,
        activeMissionCount=None,
        failedMissionCount=None,
        chargingTof=None,
        chargingTs=None,
        chargingDelayMs=None,
        missionLastUpdateTs=None,
        missionLastUpdateSuccess=None,
    ):
        self.robot_name = coerceText(robotName)
        self.available = coerceBool(available, False)
        self.reason = coerceText(reason)
        self.system_state = coerceText(systemState, None)
        self.activity_state = coerceText(activityState, None)
        self.charge_level = coerceFloatOrNone(chargeLevel)
        self.min_charge = coerceFloatOrNone(minCharge)
        self.active_mission_count = coerceIntOrNone(activeMissionCount)
        self.failed_mission_count = coerceIntOrNone(failedMissionCount)
        self.charging_tof = coerceBool(chargingTof, False)
        self.charging_ts = coerceIntOrNone(chargingTs)
        self.charging_delay_ms = coerceIntOrNone(chargingDelayMs)
        self.mission_last_update_ts = coerceText(missionLastUpdateTs, None)
        self.mission_last_update_success = coerceBool(missionLastUpdateSuccess, False)

    @classmethod
    def fromDict(cls, record):
        if isinstance(record, cls):
            return record
        record = dict(record or {})
        return cls(
            record.get("robot_name"),
            record.get("available"),
            record.get("reason"),
            record.get("system_state"),
            record.get("activity_state"),
            record.get("charge_level"),
            record.get("min_charge"),
            record.get("active_mission_count"),
            record.get("failed_mission_count"),
            record.get("charging_tof"),
            record.get("charging_ts"),
            record.get("charging_delay_ms"),
            record.get("mission_last_update_ts"),
            record.get("mission_last_update_success"),
        )

    @classmethod
    def fromSnapshot(cls, snapshot, context, available, reason):
        return cls(
            snapshot.robot_name,
            available,
            reason,
            snapshot.system_state,
            snapshot.activity_state,
            snapshot.charge_level,
            context.min_charge,
            snapshot.active_mission_count,
            snapshot.failed_mission_count,
            snapshot.charging_tof,
            snapshot.charging_ts,
            context.charging_delay_ms,
            context.mission_last_update_ts,
            context.mission_last_update_success,
        )

    def isReady(self):
        return bool(self.available)

    def notReadyReason(self):
        if self.isReady():
            return ""
        return str(self.reason or "")
