from MainController.Robot.Apply import applyRobotOutcome
from MainController.Robot.Commands import executeRobotCommandRequests
from MainController.Robot.Decision import planRobotWorkflowCycleSnapshot
from MainController.Robot.Decision import resolveRobotWorkflowDecision
from MainController.Robot.Records import _coerceRobotCycleSnapshot
from MainController.Robot.Reservations import buildReservedWorkflowsFromSnapshots
from MainController.Robot.Snapshot import readRobotCycleSnapshot
from MainController.State.Results import RobotCycleResult


def _attachLocalReservations(snapshot):
    """Ensure a single-robot cycle sees reservations implied by its own snapshot."""
    reservedWorkflows = snapshot.reserved_workflows
    if reservedWorkflows is None:
        reservedWorkflows = {}
        snapshot.reserved_workflows = reservedWorkflows

    localReservations = buildReservedWorkflowsFromSnapshots([snapshot])
    for workflowNumber, robotName in localReservations.items():
        if not reservedWorkflows.get(workflowNumber):
            reservedWorkflows[workflowNumber] = robotName
    return snapshot


def runRobotWorkflowCycleSnapshot(snapshot):
    """Evaluate and apply one already-read robot snapshot."""
    snapshot = _coerceRobotCycleSnapshot(snapshot)
    _attachLocalReservations(snapshot)
    plan = planRobotWorkflowCycleSnapshot(snapshot)
    commandResults = executeRobotCommandRequests(
        snapshot,
        plan.get("command_requests") or [],
    )
    outcome = resolveRobotWorkflowDecision(snapshot, plan, commandResults)
    return applyRobotOutcome(snapshot, outcome)


def runRobotWorkflowCycle(
    robotName,
    plcMappingState=None,
    reservedWorkflows=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Read one robot snapshot, decide the action, then apply the outcome."""
    snapshot = readRobotCycleSnapshot(
        robotName,
        plcMappingState=plcMappingState,
        reservedWorkflows=reservedWorkflows,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )
    result = runRobotWorkflowCycleSnapshot(snapshot)
    return RobotCycleResult.fromDict(result).toDict()
