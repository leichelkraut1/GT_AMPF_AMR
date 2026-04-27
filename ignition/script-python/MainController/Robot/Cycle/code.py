from MainController.Robot.Apply import applyRobotOutcome
from MainController.Robot.Decision import decideRobotWorkflowCycleSnapshot
from MainController.Robot.Snapshot import readRobotCycleSnapshot


def runRobotWorkflowCycleSnapshot(snapshot):
    """Evaluate and apply one already-read robot snapshot."""
    snapshot = dict(snapshot or {})
    outcome = decideRobotWorkflowCycleSnapshot(snapshot)
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
    return runRobotWorkflowCycleSnapshot(snapshot)
