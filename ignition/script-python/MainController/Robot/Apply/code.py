from Otto_API.Common.RuntimeHistory import appendRuntimeDatasetRow
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_HEADERS
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_MAX_ROWS
from Otto_API.Common.RuntimeHistory import timestampString
from MainController.State.PlcStore import writePlcHealthOutputs
from MainController.State.PlcStore import writePlcOutputs
from MainController.State.Results import buildCycleResult
from MainController.State.RobotStore import readRobotState
from MainController.State.RobotStore import writeRobotState
from MainController.WorkflowConfig import normalizeWorkflowNumber


def _buildCommandLogSignature(
    robotName,
    requestedWorkflowNumber,
    activeWorkflowNumber,
    action,
    level,
    stateName,
):
    return "|".join([
        str(robotName or ""),
        str(normalizeWorkflowNumber(requestedWorkflowNumber) or 0),
        str(normalizeWorkflowNumber(activeWorkflowNumber) or 0),
        str(action or ""),
        str(level or ""),
        str(stateName or ""),
    ])


def _recordCommandHistory(snapshot, outcome):
    """Log non-idle controller decisions to the runtime history dataset."""
    action = str(outcome.get("action") or "")
    if action == "idle":
        return

    robotName = str(outcome.get("robot_name") or snapshot["robot_name"])
    requestedWorkflowNumber = normalizeWorkflowNumber(
        outcome.get("selected_workflow_number")
    ) or 0
    activeWorkflowNumber = normalizeWorkflowNumber(
        outcome.get("active_workflow_number")
    ) or 0
    level = str(outcome.get("level") or "")
    stateName = str(outcome.get("state") or "")
    message = str(outcome.get("message") or "")
    signature = _buildCommandLogSignature(
        robotName,
        requestedWorkflowNumber,
        activeWorkflowNumber,
        action,
        level,
        stateName,
    )
    currentState = readRobotState(robotName)
    if currentState.get("last_logged_signature") == signature:
        writeRobotState(
            robotName,
            {
                "last_computed_log_signature": signature,
                "last_log_decision": "skip_duplicate",
            }
        )
        return

    appendRuntimeDatasetRow(
        "command_history",
        COMMAND_HISTORY_HEADERS,
        [
            timestampString(snapshot["now_epoch_ms"]),
            robotName,
            requestedWorkflowNumber,
            activeWorkflowNumber,
            action,
            level,
            stateName,
            message,
        ],
        maxRows=COMMAND_HISTORY_MAX_ROWS,
    )
    writeRobotState(
        robotName,
        {
            "last_logged_signature": signature,
            "last_computed_log_signature": signature,
            "last_log_decision": "append",
        }
    )


def applyRobotOutcome(snapshot, outcome):
    """Persist a robot-cycle outcome to state, PLC outputs, history, and result payloads."""
    robotName = snapshot["robot_name"]
    stateUpdates = dict(outcome.get("state_updates") or {})
    action = str(outcome.get("action") or "")

    if stateUpdates:
        writeRobotState(robotName, stateUpdates)

    if outcome.get("plc_health_outputs") is not None:
        plcHealth = dict(outcome.get("plc_health_outputs") or {})
        writePlcHealthOutputs(
            robotName,
            fleetFault=plcHealth.get("fleetFault", False),
            plcCommFault=plcHealth.get("plcCommFault", False),
            controlHealthy=plcHealth.get("controlHealthy", True),
        )
    elif outcome.get("plc_outputs") is not None:
        writePlcOutputs(robotName, outcome.get("plc_outputs"))

    payload = dict(outcome.get("data") or {})
    if outcome.get("command_result") is not None:
        payload["command_result"] = outcome.get("command_result")
    if outcome.get("mission_ops") is not None:
        payload["mission_ops"] = outcome.get("mission_ops")
    payload["requested_workflow_number"] = outcome.get("selected_workflow_number")
    payload["active_workflow_number"] = outcome.get("active_workflow_number")

    result = buildCycleResult(
        outcome.get("ok", False),
        outcome.get("level", "info"),
        outcome.get("message", ""),
        robotName=robotName,
        state=str(outcome.get("state") or ""),
        action=action,
        data=payload,
    )
    _recordCommandHistory(snapshot, outcome)
    return result
