from MainController.MissionCommandHelpers import issueMissionCommands
from MainController.Robot.Actions import callCreateMission
from MainController.Robot.Records import _coerceRobotCycleSnapshot
from MainController.Robot.Reservations import reserveWorkflow


def emptyMissionCommandSummary():
    """Return the no-op mission clear summary shape used by controller decisions."""
    return {
        "finalized_count": 0,
        "canceled_count": 0,
        "skipped_count": 0,
        "failed_messages": [],
        "failed_levels": [],
        "issued_count": 0,
        "any_failures": False,
        "message": "",
    }


def issueClearMissionCommands(snapshot, missions, selectedWorkflowNumber, activeWorkflowNumber):
    """Issue finalize/cancel commands for active-mission cleanup."""
    if not missions:
        return emptyMissionCommandSummary()
    return issueMissionCommands(
        snapshot.robot_name,
        missions,
        selectedWorkflowNumber,
        activeWorkflowNumber,
        snapshot.now_epoch_ms,
        finalizeMissionId=snapshot.finalize_mission_id,
        cancelMissionIds=snapshot.cancel_mission_ids,
    )


def createWorkflowMission(snapshot, workflowNumber):
    """Create one workflow mission and reserve the workflow after a successful create."""
    result = callCreateMission(
        snapshot.robot_name,
        workflowNumber,
        createMission=snapshot.create_mission,
    )
    if result.get("ok"):
        reserveWorkflow(
            snapshot.reserved_workflows,
            workflowNumber,
            snapshot.robot_name,
        )
    return result


def _unsupportedCommandResult(commandRequest):
    return {
        "ok": False,
        "level": "error",
        "message": "Unsupported robot command request [{}]".format(
            str(dict(commandRequest or {}).get("type") or "")
        ),
    }


def executeRobotCommandRequests(snapshot, commandRequests):
    """Execute planned robot command requests and return results by request name."""
    snapshot = _coerceRobotCycleSnapshot(snapshot)
    results = {}
    for commandRequest in list(commandRequests or []):
        commandRequest = dict(commandRequest or {})
        requestName = str(commandRequest.get("name") or commandRequest.get("type") or "")
        requestType = str(commandRequest.get("type") or "")
        if requestType == "create_workflow_mission":
            result = createWorkflowMission(
                snapshot,
                commandRequest.get("workflow_number"),
            )
        elif requestType == "clear_missions":
            result = issueClearMissionCommands(
                snapshot,
                commandRequest.get("missions"),
                commandRequest.get("selected_workflow_number"),
                commandRequest.get("active_workflow_number"),
            )
        else:
            result = _unsupportedCommandResult(commandRequest)
        results[requestName] = result
    return results
