import time

from Otto_API.AttachmentPhaseHelpers import buildMissionControlFlags
from MainController.State.MissionStore import readActiveMissionSummary
from MainController.State.MissionStore import readRobotMirrorInputs
from MainController.State.PlcMappingStore import readPlcMappings
from MainController.State.PlcStore import readPlcInputs
from MainController.State.RobotStore import readRobotState
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagPaths import getPendingCreateMissionTimeoutMsPath


_DEFAULT_PENDING_CREATE_TIMEOUT_MS = 30000


def _resolveRobotPlcTagName(robotName, plcMappingState):
    """Resolve one robot's PLC row name from the already-read mapping state."""
    return str(dict(plcMappingState.get("robot_name_to_plc_tag") or {}).get(robotName) or "")


def _pendingCreateTimeoutMs():
    rawValue = readOptionalTagValue(
        getPendingCreateMissionTimeoutMsPath(),
        _DEFAULT_PENDING_CREATE_TIMEOUT_MS,
    )
    try:
        return max(0, int(rawValue or 0))
    except Exception:
        return _DEFAULT_PENDING_CREATE_TIMEOUT_MS


def readRobotCycleSnapshot(
    robotName,
    plcMappingState=None,
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
    if plcMappingState is None:
        plcMappingState = readPlcMappings()

    plcTagName = _resolveRobotPlcTagName(robotName, plcMappingState)
    if not plcTagName:
        mappingFaultReason = "plc_robot_mapping_missing"
        if not plcMappingState.get("robot_dataset_ok", True):
            mappingFaultReason = "plc_robot_mapping_unreadable"
    else:
        mappingFaultReason = ""

    plcInputs = readPlcInputs(plcTagName, faultReason=mappingFaultReason)
    mirrorInputs = dict(readRobotMirrorInputs(robotName) or {})
    currentState = readRobotState(robotName)
    activeSummary = readActiveMissionSummary(robotName)
    missionFlags = buildMissionControlFlags(
        str(activeSummary.get("current_mission_status") or "").upper() == "STARVED"
    )
    mirrorInputs["mission_starved"] = missionFlags["mission_starved"]
    mirrorInputs["mission_ready_for_attachment"] = missionFlags["ready_for_attachment"]
    activeWorkflowNumber = activeSummary.get("workflow_number")
    selectedWorkflowNumber = plcInputs["requested_workflow_number"]
    controllerAvailableForWork = (
        bool(mirrorInputs.get("available_for_work"))
        or bool(currentState.get("force_robot_ready"))
    )

    return {
        "robot_name": robotName,
        "plc_tag_name": plcTagName,
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
        "pending_create_timeout_ms": _pendingCreateTimeoutMs(),
    }


def readRobotCycleSnapshots(
    robotNames,
    plcMappingState=None,
    reservedWorkflows=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Read a controller-cycle batch with shared PLC mapping, time, and reservations."""
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)
    if reservedWorkflows is None:
        reservedWorkflows = {}
    if plcMappingState is None:
        plcMappingState = readPlcMappings()

    snapshots = []
    for robotName in list(robotNames or []):
        snapshots.append(
            readRobotCycleSnapshot(
                robotName,
                plcMappingState=plcMappingState,
                reservedWorkflows=reservedWorkflows,
                nowEpochMs=nowEpochMs,
                createMission=createMission,
                finalizeMissionId=finalizeMissionId,
                cancelMissionIds=cancelMissionIds,
            )
        )
    return snapshots
