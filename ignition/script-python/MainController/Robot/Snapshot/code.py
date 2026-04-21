import time

from MainController.State.MissionStore import readActiveMissionSummary
from MainController.State.MissionStore import readRobotMirrorInputs
from MainController.State.PlcStore import readPlcInputs
from MainController.State.Provisioning import ensureRobotRunnerTags
from MainController.State.RobotStore import readRobotState


def readRobotCycleSnapshot(
    robotName,
    reservedWorkflows=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Read the full per-robot snapshot needed for one workflow-controller pass."""
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)
    if reservedWorkflows is None:
        reservedWorkflows = {}

    ensureRobotRunnerTags(robotName)

    plcInputs = readPlcInputs(robotName)
    mirrorInputs = readRobotMirrorInputs(robotName)
    currentState = readRobotState(robotName)
    activeSummary = readActiveMissionSummary(robotName)
    activeWorkflowNumber = activeSummary.get("workflow_number")
    selectedWorkflowNumber = plcInputs["requested_workflow_number"]
    controllerAvailableForWork = (
        bool(mirrorInputs.get("available_for_work"))
        or bool(currentState.get("force_robot_ready"))
    )

    return {
        "robot_name": robotName,
        "reserved_workflows": reservedWorkflows,
        "now_epoch_ms": int(nowEpochMs),
        "create_mission": createMission,
        "finalize_mission_id": finalizeMissionId,
        "cancel_mission_ids": cancelMissionIds,
        "plc_inputs": plcInputs,
        "plc_healthy": bool(plcInputs.get("healthy", True)),
        "mirror_inputs": mirrorInputs,
        "current_state": currentState,
        "active_summary": activeSummary,
        "active_workflow_number": activeWorkflowNumber,
        "selected_workflow_number": selectedWorkflowNumber,
        "controller_available_for_work": controllerAvailableForWork,
    }
