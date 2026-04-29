from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import writeRequiredTagValues

from MainController.State.Coerce import toBool
from MainController.State.Paths import internalStatePaths
from Otto_API.Models.Fleet import normalizeWorkflowNumber


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


def _normalizeInt(value):
    return int(value or 0)


def _normalizeString(value):
    return str(value or "")


def _normalizeSelectedWorkflowNumber(value):
    return normalizeWorkflowNumber(value) or 0


ROBOT_STATE_FIELD_SPECS = [
    {"name": "force_robot_ready", "default": False, "normalize": toBool},
    {"name": "disable_ignition_control", "default": False, "normalize": toBool},
    {"name": "request_latched", "default": False, "normalize": toBool},
    {"name": "selected_workflow_number", "default": 0, "normalize": _normalizeSelectedWorkflowNumber},
    {"name": "state", "default": "idle", "normalize": _normalizeControllerStateName},
    {"name": "mission_created", "default": False, "normalize": toBool},
    {"name": "mission_needs_finalized", "default": False, "normalize": toBool},
    {"name": "pending_create_start_epoch_ms", "default": 0, "normalize": _normalizeInt},
    {"name": "last_command_ts", "default": "", "normalize": _normalizeString},
    {"name": "last_result", "default": "", "normalize": _normalizeString},
    {"name": "last_command_id", "default": "", "normalize": _normalizeString},
    {"name": "next_action_allowed_epoch_ms", "default": 0, "normalize": _normalizeInt},
    {"name": "last_attempt_action", "default": "", "normalize": _normalizeString},
    {"name": "retry_count", "default": 0, "normalize": _normalizeInt},
    {"name": "last_logged_signature", "default": "", "normalize": _normalizeString},
    {"name": "last_computed_log_signature", "default": "", "normalize": _normalizeString},
    {"name": "last_log_decision", "default": "", "normalize": _normalizeString},
]

INTERNAL_STATE_FIELD_NAMES = [
    spec["name"] for spec in ROBOT_STATE_FIELD_SPECS
]


def defaultRobotState():
    """Canonical internal state for one robot runner."""
    return {
        spec["name"]: spec["default"]
        for spec in ROBOT_STATE_FIELD_SPECS
    }


def normalizeRobotState(rawState):
    """Normalize persisted tag values back into the controller's expected state shape."""
    rawState = dict(rawState or {})
    state = {}
    for spec in ROBOT_STATE_FIELD_SPECS:
        fieldName = spec["name"]
        state[fieldName] = spec["normalize"](rawState.get(fieldName))
    return state


def readRobotState(robotName):
    """Read one robot's internal state from tags."""
    paths = internalStatePaths(robotName)
    values = readTagValues([paths[fieldName] for fieldName in INTERNAL_STATE_FIELD_NAMES])

    rawState = {}
    for key, qualifiedValue in zip(INTERNAL_STATE_FIELD_NAMES, values):
        rawState[key] = qualifiedValue.value if qualifiedValue.quality.isGood() else None
    return normalizeRobotState(rawState)


def writeRobotState(robotName, state, currentState=None):
    """Persist only the provided robot-state fields after normalizing against the current state."""
    incomingState = dict(state or {})
    if not incomingState:
        return

    mergedState = dict(currentState or readRobotState(robotName) or {})
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
