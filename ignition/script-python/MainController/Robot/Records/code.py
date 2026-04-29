from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import coerceBool
from Otto_API.Common.RecordHelpers import coerceInt
from Otto_API.Common.RecordHelpers import coerceText
from Otto_API.Models.Missions import MissionRecord

from MainController.State.RobotStore import normalizeRobotState
from MainController.WorkflowConfig import normalizeWorkflowNumber


def _coerceWorkflowNumber(value):
    return normalizeWorkflowNumber(value) or 0


def _coerceOptionalWorkflowNumber(value):
    return normalizeWorkflowNumber(value)


def _coerceFloat(value):
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _coerceUpperText(value):
    return str(coerceText(value) or "").upper()


def _coerceTimeoutMs(value, defaultValue=30000):
    try:
        return max(0, int(value if value is not None else defaultValue))
    except Exception:
        return int(defaultValue or 0)


def _sourceDict(value):
    if hasattr(value, "toDict"):
        return value.toDict()
    return dict(value or {})


def _coerceOptionalMissionRecord(missionRecord):
    if missionRecord is None:
        return None
    return MissionRecord.fromDict(missionRecord)


def _missionRecordToDict(missionRecord):
    if missionRecord is None:
        return None
    return MissionRecord.fromDict(missionRecord).toDict()


class _SpecRecord(MappingRecordBase):
    FIELD_SPECS = ()

    def __init__(self, rawValues=None):
        rawValues = _sourceDict(rawValues)
        for fieldName, defaultValue, coercer in list(self.FIELD_SPECS):
            setattr(self, fieldName, coercer(rawValues.get(fieldName, defaultValue)))

    @classmethod
    def fromDict(cls, rawValues):
        return cls(rawValues)


class RobotPlcInputs(_SpecRecord):
    requested_workflow_number = 0
    finalize_ok = False
    healthy = True
    fault_reason = ""

    FIELDS = (
        "requested_workflow_number",
        "finalize_ok",
        "healthy",
        "fault_reason",
    )
    FIELD_SPECS = (
        ("requested_workflow_number", 0, _coerceWorkflowNumber),
        ("finalize_ok", False, coerceBool),
        ("healthy", True, lambda value: coerceBool(value, True)),
        ("fault_reason", "", coerceText),
    )


class RobotMirrorInputs(_SpecRecord):
    available_for_work = False
    active_mission_count = 0
    charge_level = 0.0
    system_state = ""
    sub_system_state = ""
    activity_state = ""
    place_id = ""
    place_name = ""
    container_present = False
    container_id = ""
    mission_starved = False
    mission_ready_for_attachment = False

    FIELDS = (
        "available_for_work",
        "active_mission_count",
        "charge_level",
        "system_state",
        "sub_system_state",
        "activity_state",
        "place_id",
        "place_name",
        "container_present",
        "container_id",
        "mission_starved",
        "mission_ready_for_attachment",
    )
    FIELD_SPECS = (
        ("available_for_work", False, coerceBool),
        ("active_mission_count", 0, coerceInt),
        ("charge_level", 0.0, _coerceFloat),
        ("system_state", "", coerceText),
        ("sub_system_state", "", coerceText),
        ("activity_state", "", coerceText),
        ("place_id", "", coerceText),
        ("place_name", "", coerceText),
        ("container_present", False, coerceBool),
        ("container_id", "", coerceText),
        ("mission_starved", False, coerceBool),
        ("mission_ready_for_attachment", False, coerceBool),
    )


class RobotControllerState(MappingRecordBase):
    FIELDS = (
        "force_robot_ready",
        "disable_ignition_control",
        "request_latched",
        "selected_workflow_number",
        "state",
        "mission_created",
        "mission_needs_finalized",
        "pending_create_start_epoch_ms",
        "last_command_ts",
        "last_result",
        "last_command_id",
        "next_action_allowed_epoch_ms",
        "last_attempt_action",
        "retry_count",
        "last_logged_signature",
        "last_computed_log_signature",
        "last_log_decision",
    )

    def __init__(self, rawState=None):
        state = normalizeRobotState(rawState)
        for fieldName in self.FIELDS:
            setattr(self, fieldName, state[fieldName])

    @classmethod
    def fromDict(cls, state):
        return cls(state)


class ActiveMissionSummary(_SpecRecord):
    count = 0
    missions = ()
    current_mission = None
    current_mission_status = ""
    current_mission_id = ""
    current_mission_path = ""
    mission_name = ""
    workflow_number = None

    FIELDS = (
        "count",
        "missions",
        "current_mission",
        "current_mission_status",
        "current_mission_id",
        "current_mission_path",
        "mission_name",
        "workflow_number",
    )
    FIELD_SPECS = (
        ("count", 0, coerceInt),
        ("missions", [], MissionRecord.listFromDicts),
        ("current_mission", None, _coerceOptionalMissionRecord),
        ("current_mission_status", "", _coerceUpperText),
        ("current_mission_id", "", coerceText),
        ("current_mission_path", "", coerceText),
        ("mission_name", "", coerceText),
        ("workflow_number", None, _coerceOptionalWorkflowNumber),
    )

    @classmethod
    def fromDict(cls, activeSummary):
        activeSummary = _sourceDict(activeSummary)
        if "count" not in activeSummary:
            activeSummary["count"] = len(list(activeSummary.get("missions") or []))
        return cls(activeSummary)

    def toDict(self):
        data = self._fieldDict()
        data["missions"] = [_missionRecordToDict(missionRecord) for missionRecord in self.missions]
        data["current_mission"] = _missionRecordToDict(self.current_mission)
        return data


class RobotCycleSnapshot(MappingRecordBase):
    FIELDS = (
        "robot_name",
        "plc_tag_name",
        "reserved_workflows",
        "now_epoch_ms",
        "create_mission",
        "finalize_mission_id",
        "cancel_mission_ids",
        "plc_inputs",
        "mirror_inputs",
        "current_state",
        "active_summary",
        "active_workflow_number",
        "selected_workflow_number",
        "controller_available_for_work",
        "pending_create_timeout_ms",
    )

    def __init__(
        self,
        robotName,
        plcTagName="",
        reservedWorkflows=None,
        nowEpochMs=0,
        createMission=None,
        finalizeMissionId=None,
        cancelMissionIds=None,
        plcInputs=None,
        mirrorInputs=None,
        currentState=None,
        activeSummary=None,
        activeWorkflowNumber=None,
        selectedWorkflowNumber=None,
        controllerAvailableForWork=False,
        pendingCreateTimeoutMs=30000
    ):
        self.robot_name = coerceText(robotName)
        self.plc_tag_name = coerceText(plcTagName)
        self.reserved_workflows = reservedWorkflows if reservedWorkflows is not None else {}
        self.now_epoch_ms = coerceInt(nowEpochMs)
        self.create_mission = createMission
        self.finalize_mission_id = finalizeMissionId
        self.cancel_mission_ids = cancelMissionIds
        self.plc_inputs = _coerceRobotPlcInputs(plcInputs)
        self.mirror_inputs = _coerceRobotMirrorInputs(mirrorInputs)
        self.current_state = _coerceRobotControllerState(currentState)
        self.active_summary = _coerceActiveMissionSummary(activeSummary)
        self.active_workflow_number = normalizeWorkflowNumber(
            self.active_summary.workflow_number
            if activeWorkflowNumber is None
            else activeWorkflowNumber
        )
        self.selected_workflow_number = _coerceWorkflowNumber(
            self.plc_inputs.requested_workflow_number
            if selectedWorkflowNumber is None
            else selectedWorkflowNumber
        )
        self.controller_available_for_work = coerceBool(controllerAvailableForWork)
        self.pending_create_timeout_ms = _coerceTimeoutMs(pendingCreateTimeoutMs)

    @classmethod
    def fromDict(cls, snapshot):
        snapshot = _sourceDict(snapshot)
        return cls(
            snapshot.get("robot_name", ""),
            snapshot.get("plc_tag_name", ""),
            snapshot.get("reserved_workflows"),
            snapshot.get("now_epoch_ms", 0),
            snapshot.get("create_mission"),
            snapshot.get("finalize_mission_id"),
            snapshot.get("cancel_mission_ids"),
            snapshot.get("plc_inputs"),
            snapshot.get("mirror_inputs"),
            snapshot.get("current_state"),
            snapshot.get("active_summary"),
            snapshot.get("active_workflow_number"),
            snapshot.get("selected_workflow_number"),
            snapshot.get("controller_available_for_work", False),
            snapshot.get("pending_create_timeout_ms", 30000),
        )

    def toDict(self):
        data = self._fieldDict()
        data["plc_inputs"] = self.plc_inputs.toDict()
        data["plc_healthy"] = self.plc_inputs.healthy
        data["mirror_inputs"] = self.mirror_inputs.toDict()
        data["current_state"] = self.current_state.toDict()
        data["active_summary"] = self.active_summary.toDict()
        return data

    def cloneWith(self, **overrides):
        values = self._fieldDict()
        values.update(dict(overrides or {}))
        return RobotCycleSnapshot.fromDict(values)


def _coerceRobotPlcInputs(plcInputs):
    if isinstance(plcInputs, RobotPlcInputs):
        return plcInputs
    return RobotPlcInputs.fromDict(plcInputs)


def _coerceRobotMirrorInputs(mirrorInputs):
    if isinstance(mirrorInputs, RobotMirrorInputs):
        return mirrorInputs
    return RobotMirrorInputs.fromDict(mirrorInputs)


def _coerceRobotControllerState(currentState):
    if isinstance(currentState, RobotControllerState):
        return currentState
    return RobotControllerState.fromDict(currentState)


def _coerceActiveMissionSummary(activeSummary):
    if isinstance(activeSummary, ActiveMissionSummary):
        return activeSummary
    return ActiveMissionSummary.fromDict(activeSummary)


def _coerceRobotCycleSnapshot(snapshot):
    if isinstance(snapshot, RobotCycleSnapshot):
        return snapshot
    return RobotCycleSnapshot.fromDict(snapshot)
