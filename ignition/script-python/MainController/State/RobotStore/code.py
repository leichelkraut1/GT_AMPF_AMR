from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import writeRequiredTagValues

from MainController.State.Coerce import toBool
from MainController.State.Paths import internalStatePaths
from MainController.WorkflowConfig import normalizeWorkflowNumber


INTERNAL_STATE_FIELD_NAMES = [
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
]


def _normalizeControllerStateName(stateName):
    """Collapse legacy detailed state labels into the compact four-state model."""
    stateName = str(stateName or "idle")
    if stateName in ["idle", "mission_requested", "mission_active", "fault"]:
        return stateName
    if stateName.startswith("clear_"):
        return "mission_active"
    if stateName in [
        "cancel_requested",
        "switch_cancel_requested",
        "request_invalid",
        "request_conflict",
        "waiting_available",
        "create_backoff",
        "failed",
        "plc_comm_fault",
    ]:
        return "fault"
    return "fault"


def defaultRobotState():
    """Canonical internal state for one robot runner."""
    return {
        "force_robot_ready": False,
        "disable_ignition_control": False,
        "request_latched": False,
        "selected_workflow_number": 0,
        "state": "idle",
        "mission_created": False,
        "mission_needs_finalized": False,
        "pending_create_start_epoch_ms": 0,
        "last_command_ts": "",
        "last_result": "",
        "last_command_id": "",
        "next_action_allowed_epoch_ms": 0,
        "last_attempt_action": "",
        "retry_count": 0,
        "last_logged_signature": "",
        "last_computed_log_signature": "",
        "last_log_decision": "",
    }


def normalizeRobotState(rawState):
    """Normalize persisted tag values back into the controller's expected state shape."""
    rawState = dict(rawState or {})
    state = defaultRobotState()
    state["force_robot_ready"] = toBool(rawState.get("force_robot_ready"))
    state["disable_ignition_control"] = toBool(rawState.get("disable_ignition_control"))
    state["request_latched"] = toBool(rawState.get("request_latched"))
    state["selected_workflow_number"] = normalizeWorkflowNumber(
        rawState.get("selected_workflow_number")
    ) or 0
    state["state"] = _normalizeControllerStateName(rawState.get("state"))
    state["mission_created"] = toBool(rawState.get("mission_created"))
    state["mission_needs_finalized"] = toBool(rawState.get("mission_needs_finalized"))
    state["pending_create_start_epoch_ms"] = int(rawState.get("pending_create_start_epoch_ms") or 0)
    state["last_command_ts"] = str(rawState.get("last_command_ts") or "")
    state["last_result"] = str(rawState.get("last_result") or "")
    state["last_command_id"] = str(rawState.get("last_command_id") or "")
    state["next_action_allowed_epoch_ms"] = int(rawState.get("next_action_allowed_epoch_ms") or 0)
    state["last_attempt_action"] = str(rawState.get("last_attempt_action") or "")
    state["retry_count"] = int(rawState.get("retry_count") or 0)
    state["last_logged_signature"] = str(rawState.get("last_logged_signature") or "")
    state["last_computed_log_signature"] = str(rawState.get("last_computed_log_signature") or "")
    state["last_log_decision"] = str(rawState.get("last_log_decision") or "")
    return state


def readRobotState(robotName):
    """Read one robot's internal state from tags."""
    paths = internalStatePaths(robotName)
    values = readTagValues([paths[fieldName] for fieldName in INTERNAL_STATE_FIELD_NAMES])

    rawState = {}
    for key, qualifiedValue in zip(INTERNAL_STATE_FIELD_NAMES, values):
        rawState[key] = qualifiedValue.value if qualifiedValue.quality.isGood() else None
    return normalizeRobotState(rawState)


def writeRobotState(robotName, state):
    """Persist only the provided robot-state fields after normalizing against the current state."""
    incomingState = dict(state or {})
    if not incomingState:
        return

    mergedState = dict(readRobotState(robotName) or {})
    mergedState.update(incomingState)
    normalizedState = normalizeRobotState(mergedState)
    paths = internalStatePaths(robotName)
    writePaths = []
    writeValues = []
    for fieldName in INTERNAL_STATE_FIELD_NAMES:
        if fieldName not in incomingState:
            continue
        writePaths.append(paths[fieldName])
        writeValues.append(normalizedState[fieldName])

    if writePaths:
        writeRequiredTagValues(
            writePaths,
            writeValues,
            labels=["MainController robot state"] * len(writePaths)
        )
