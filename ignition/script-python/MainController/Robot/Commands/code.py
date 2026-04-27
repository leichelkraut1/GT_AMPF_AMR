from MainController.MissionCommandHelpers import issueMissionCommands
from MainController.Robot.Actions import callCreateMission
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
        snapshot["robot_name"],
        missions,
        selectedWorkflowNumber,
        activeWorkflowNumber,
        snapshot["now_epoch_ms"],
        finalizeMissionId=snapshot["finalize_mission_id"],
        cancelMissionIds=snapshot["cancel_mission_ids"],
    )


def reserveActiveWorkflow(snapshot):
    """Reserve an already-active workflow for this robot in the shared cycle map."""
    if snapshot.get("active_workflow_number"):
        reserveWorkflow(
            snapshot["reserved_workflows"],
            snapshot["active_workflow_number"],
            snapshot["robot_name"],
        )


def createWorkflowMission(snapshot, workflowNumber):
    """Create one workflow mission and reserve the workflow after a successful create."""
    result = callCreateMission(
        snapshot["robot_name"],
        workflowNumber,
        createMission=snapshot["create_mission"],
    )
    if result.get("ok"):
        reserveWorkflow(
            snapshot["reserved_workflows"],
            workflowNumber,
            snapshot["robot_name"],
        )
    return result
