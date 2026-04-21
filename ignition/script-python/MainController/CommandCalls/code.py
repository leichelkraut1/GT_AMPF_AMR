from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Containers import Get as ContainerGet
from Otto_API.System import Get as FleetGet
from Otto_API.Missions import MissionSorting
from Otto_API.Robots import Get as RobotGet

from MainController.CommandHelpers import buildWorkflowReservedMap
from MainController.CommandHelpers import ensureRobotRunnerTags
from MainController.CommandHelpers import ROBOT_NAMES
from MainController.CommandHelpers import writePlcHealthOutputs
from MainController.RobotCycle import runRobotWorkflowCycle


def runAllRobotWorkflowCycles(
    robotNames=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """
    Run one workflow-controller pass across every configured robot.

    Per-robot warn results are treated as soft failures here so the aggregate only
    goes hard-failed on real execution errors, not on expected control states like
    waiting, conflicts, or invalid requests.
    """
    if robotNames is None:
        robotNames = ROBOT_NAMES

    reservedWorkflows = buildWorkflowReservedMap(robotNames)
    results = []

    for robotName in list(robotNames or []):
        results.append(
            runRobotWorkflowCycle(
                robotName,
                reservedWorkflows=reservedWorkflows,
                nowEpochMs=nowEpochMs,
                createMission=createMission,
                finalizeMissionId=finalizeMissionId,
                cancelMissionIds=cancelMissionIds,
            )
        )

    ok = all(result.get("ok", False) or result.get("level") == "warn" for result in results)
    level = "info" if ok else "error"
    return buildOperationResult(
        ok,
        level,
        "Processed workflow cycles for {} robot(s)".format(len(results)),
        data={"results": results},
        results=results,
    )


def runMainControllerCycle(
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """
    Run the ordered controller phases for one main-loop cycle.

    The PLC workflow pass only runs after robot state and mission state have both
    been refreshed in the same cycle. Server status is read from the slower
    cached status timer rather than making a fresh HTTP call in this fast loop.
    """
    serverStatusResult = FleetGet.readCachedServerStatus()
    robotStateResult = RobotGet.updateRobotOperationalState()
    containerStateResult = ContainerGet.updateContainers()
    missionSortResult = MissionSorting.run()

    canEvaluatePlc = (
        robotStateResult.get("ok")
        and containerStateResult.get("ok")
        and missionSortResult.get("ok")
    )
    if canEvaluatePlc:
        workflowResult = runAllRobotWorkflowCycles(
            nowEpochMs=nowEpochMs,
            createMission=createMission,
            finalizeMissionId=finalizeMissionId,
            cancelMissionIds=cancelMissionIds,
        )
    else:
        for robotName in ROBOT_NAMES:
            ensureRobotRunnerTags(robotName)
            writePlcHealthOutputs(
                robotName,
                fleetFault=True,
                plcCommFault=False,
                controlHealthy=False,
            )
        workflowResult = buildOperationResult(
            False,
            "warn",
            "Skipped PLC workflow evaluation because robot or mission state is stale",
            data=None,
        )

    ok = canEvaluatePlc and workflowResult.get("ok", False)
    if not serverStatusResult.get("ok", False):
        level = "warn"
    else:
        level = "info" if ok else "warn"

    return buildOperationResult(
        ok,
        level,
        "MainController cycle completed",
        data={
            "server_status": serverStatusResult,
            "robot_state": robotStateResult,
            "container_state": containerStateResult,
            "mission_sorting": missionSortResult,
            "workflow_cycles": workflowResult,
        },
        server_status=serverStatusResult,
        robot_state=robotStateResult,
        container_state=containerStateResult,
        mission_sorting=missionSortResult,
        workflow_cycles=workflowResult,
    )
