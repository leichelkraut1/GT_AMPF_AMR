from Otto_API.Common.RuntimeHistory import appendRuntimeDatasetRow
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_HEADERS
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_MAX_ROWS
from Otto_API.Common.RuntimeHistory import timestampString
from MainController.State.PlcStore import writePlcHealthOutputs
from MainController.State.PlcStore import writePlcOutputs
from MainController.State.Results import buildCycleResult
from MainController.State.RobotStore import readRobotState
from MainController.State.RobotStore import writeRobotState
from MainController.Robot.Records import _coerceRobotCycleSnapshot
from MainController.WorkflowConfig import normalizeWorkflowNumber


def _plcSyncResult(ok, level="info", message="PLC robot sync healthy"):
    return {
        "ok": bool(ok),
        "level": str(level or "info"),
        "message": str(message or ""),
    }


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


def _recordCommandHistory(snapshot, outcome, currentState=None):
    """Log non-idle controller decisions to the runtime history dataset."""
    action = str(outcome.get("action") or "")
    if action == "idle":
        return

    robotName = str(outcome.get("robot_name") or snapshot.robot_name)
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
    currentState = dict(currentState or readRobotState(robotName) or {})
    if currentState.get("last_logged_signature") == signature:
        writeRobotState(
            robotName,
            {
                "last_computed_log_signature": signature,
                "last_log_decision": "skip_duplicate",
            },
            currentState=currentState,
        )
        return

    appendRuntimeDatasetRow(
        "command_history",
        COMMAND_HISTORY_HEADERS,
        [
            timestampString(snapshot.now_epoch_ms),
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
        },
        currentState=currentState,
    )


def applyRobotOutcome(snapshot, outcome):
    """Persist a robot-cycle outcome to state, PLC outputs, history, and result payloads."""
    snapshot = _coerceRobotCycleSnapshot(snapshot)
    robotName = snapshot.robot_name
    stateUpdates = dict(outcome.get("state_updates") or {})
    action = str(outcome.get("action") or "")

    if stateUpdates:
        writeRobotState(robotName, stateUpdates, currentState=snapshot.current_state.toDict())
    currentState = snapshot.current_state.toDict()
    currentState.update(stateUpdates)

    syncResult = _plcSyncResult(True)
    if outcome.get("plc_health_outputs") is not None:
        plcHealth = dict(outcome.get("plc_health_outputs") or {})
        try:
            writePlcHealthOutputs(
                snapshot.plc_tag_name,
                fleetFault=plcHealth.get("fleetFault", False),
                plcCommFault=plcHealth.get("plcCommFault", False),
                controlHealthy=plcHealth.get("controlHealthy", True),
            )
        except Exception as exc:
            syncResult = _plcSyncResult(
                False,
                "warn",
                "Robot [{}] PLC health sync failed: {}".format(robotName, str(exc)),
            )
    elif outcome.get("plc_outputs") is not None:
        try:
            writePlcOutputs(snapshot.plc_tag_name, outcome.get("plc_outputs"))
        except Exception as exc:
            syncResult = _plcSyncResult(
                False,
                "warn",
                "Robot [{}] PLC sync failed: {}".format(robotName, str(exc)),
            )

    payload = dict(outcome.get("data") or {})
    if outcome.get("command_result") is not None:
        payload["command_result"] = outcome.get("command_result")
    if outcome.get("mission_ops") is not None:
        payload["mission_ops"] = outcome.get("mission_ops")
    payload["plc_sync_result"] = syncResult
    payload["requested_workflow_number"] = outcome.get("selected_workflow_number")
    payload["active_workflow_number"] = outcome.get("active_workflow_number")

    resultOk = bool(outcome.get("ok", False))
    resultLevel = str(outcome.get("level", "info") or "info")
    resultMessage = str(outcome.get("message", "") or "")
    if not syncResult["ok"]:
        resultOk = False
        if resultLevel == "info":
            resultLevel = "warn"
        if syncResult["message"]:
            resultMessage = (
                "{}; {}".format(resultMessage, syncResult["message"])
                if resultMessage
                else syncResult["message"]
            )

    result = buildCycleResult(
        resultOk,
        resultLevel,
        resultMessage,
        robotName=robotName,
        state=str(outcome.get("state") or ""),
        action=action,
        data=payload,
    )
    _recordCommandHistory(snapshot, outcome, currentState=currentState)
    return result
