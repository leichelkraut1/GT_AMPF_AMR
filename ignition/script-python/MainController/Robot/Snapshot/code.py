import time

from Otto_API.AttachmentPhase import buildMissionControlFlags
from MainController.State.MissionStore import readActiveMissionSummary
from MainController.State.MissionStore import readRobotMirrorInputs
from MainController.State.PlcMappingStore import readPlcMappings
from MainController.State.PlcStore import readPlcInputs
from MainController.State.RobotStore import readRobotState
from MainController.Robot.Records import RobotCycleSnapshot
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
    cancelMissionIds=None,
    pendingCreateTimeoutMs=None
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
    mirrorInputs = readRobotMirrorInputs(robotName)
    currentState = readRobotState(robotName)
    activeSummary = readActiveMissionSummary(robotName)
    missionFlags = buildMissionControlFlags(
        str(activeSummary.current_mission_status or "").upper() == "STARVED"
    )
    mirrorInputs = mirrorInputs.cloneWith(
        mission_starved=missionFlags["mission_starved"],
        mission_ready_for_attachment=missionFlags["ready_for_attachment"],
    )
    activeWorkflowNumber = activeSummary.workflow_number
    selectedWorkflowNumber = plcInputs["requested_workflow_number"]
    controllerAvailableForWork = (
        bool(mirrorInputs.available_for_work)
        or bool(currentState.get("force_robot_ready"))
    )
    if pendingCreateTimeoutMs is None:
        pendingCreateTimeoutMs = _pendingCreateTimeoutMs()

    return RobotCycleSnapshot(
        robotName,
        plcTagName,
        reservedWorkflows,
        int(nowEpochMs),
        createMission,
        finalizeMissionId,
        cancelMissionIds,
        plcInputs,
        mirrorInputs,
        currentState,
        activeSummary,
        activeWorkflowNumber,
        selectedWorkflowNumber,
        controllerAvailableForWork,
        pendingCreateTimeoutMs,
    )


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
    pendingCreateTimeoutMs = _pendingCreateTimeoutMs()

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
                pendingCreateTimeoutMs=pendingCreateTimeoutMs,
            )
        )
    return snapshots
