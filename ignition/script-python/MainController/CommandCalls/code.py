from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Fleet import Get
from Otto_API.Missions import MissionSorting

from MainController.CommandHelpers import buildWorkflowReservedMap
from MainController.CommandHelpers import ensureRobotRunnerTags
from MainController.CommandHelpers import ROBOT_NAMES
from MainController.RobotCycle import runRobotWorkflowCycle


def runAllRobotWorkflowCycles(
    robotNames=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMission=None,
    cancelMission=None
):
    """Run one workflow-controller pass across every configured robot."""
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
                finalizeMission=finalizeMission,
                cancelMission=cancelMission,
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


def runMainControllerCycle(nowEpochMs=None, createMission=None, finalizeMission=None, cancelMission=None):
    """
    Run the ordered controller phases for one main-loop cycle.

    The PLC workflow pass only runs after fleet state and mission state have both
    been refreshed in the same cycle.
    """
    serverStatusResult = Get.getServerStatus()
    robotStateResult = Get.updateRobotOperationalState()
    missionSortResult = MissionSorting.run()

    canEvaluatePlc = robotStateResult.get("ok") and missionSortResult.get("ok")
    if canEvaluatePlc:
        workflowResult = runAllRobotWorkflowCycles(
            nowEpochMs=nowEpochMs,
            createMission=createMission,
            finalizeMission=finalizeMission,
            cancelMission=cancelMission,
        )
    else:
        for robotName in ROBOT_NAMES:
            ensureRobotRunnerTags(robotName)
            from MainController.CommandHelpers import writePlcHealthOutputs
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
            "mission_sorting": missionSortResult,
            "workflow_cycles": workflowResult,
        },
        server_status=serverStatusResult,
        robot_state=robotStateResult,
        mission_sorting=missionSortResult,
        workflow_cycles=workflowResult,
    )
