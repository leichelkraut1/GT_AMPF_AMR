from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Containers import Get as ContainerGet
from Otto_API.Missions import MissionSorting
from Otto_API.Robots import Get as RobotGet
from Otto_API.System import Get as SystemGet

from MainController.Robot.Cycle import runRobotWorkflowCycle
from MainController.State.ContainerMirror import mirrorPlcContainerOccupancy
from MainController.State.MissionStore import buildWorkflowReservedMap
from MainController.State.Paths import ROBOT_NAMES
from MainController.State.PlcStore import writePlcHealthOutputs
from MainController.State.Provisioning import ensureRobotRunnerTags
from MainController.State.RuntimeStore import writeRuntimeFields


def _phaseStatus(result):
    result = dict(result or {})
    if result.get("ok"):
        return "Healthy"

    level = str(result.get("level") or "").lower()
    if level == "error":
        return "Error"
    return "Warn"


def _phaseMessage(result):
    return str(dict(result or {}).get("message") or "")


def _controllerRuntimeFields(
    robotStateResult,
    containerStateResult,
    plcContainerMirrorResult,
    missionSortResult,
    workflowResult
):
    phaseResults = [
        ("robot_state", "Robot Sync", robotStateResult),
        ("container_state", "Container Sync", containerStateResult),
        ("plc_container_mirror", "PLC Container Mirror", plcContainerMirrorResult),
        ("mission_sorting", "Mission Sorting", missionSortResult),
        ("workflow_cycles", "Workflow Cycles", workflowResult),
    ]

    fields = {}
    unhealthyMessages = []
    for fieldPrefix, label, result in phaseResults:
        fields[fieldPrefix + "_status"] = _phaseStatus(result)
        fields[fieldPrefix + "_message"] = _phaseMessage(result)
        if not dict(result or {}).get("ok"):
            unhealthyMessages.append(
                "{}: {}".format(label, _phaseMessage(result) or "unhealthy")
            )

    fields["controller_fault_summary"] = "; ".join(unhealthyMessages) or "Healthy"
    return fields


def runAllRobotWorkflowCycles(
    robotNames=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
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
    """Run the ordered controller phases for one main-loop cycle."""
    serverStatusResult = SystemGet.readCachedServerStatus()
    robotStateResult = RobotGet.updateRobotOperationalState()
    containerStateResult = ContainerGet.updateContainers()
    if containerStateResult.get("ok"):
        plcContainerMirrorResult = mirrorPlcContainerOccupancy()
    else:
        plcContainerMirrorResult = buildOperationResult(
            False,
            "warn",
            "Skipped PLC container occupancy mirror because container state is stale",
            data={"rows": [], "writes": []},
            rows=[],
            writes=[],
        )
    missionSortResult = MissionSorting.run()

    canEvaluatePlc = (
        robotStateResult.get("ok")
        and containerStateResult.get("ok")
        and plcContainerMirrorResult.get("ok")
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
            "Skipped PLC workflow evaluation because robot, container, PLC mirror, or mission state is stale",
            data=None,
        )

    writeRuntimeFields(
        _controllerRuntimeFields(
            robotStateResult,
            containerStateResult,
            plcContainerMirrorResult,
            missionSortResult,
            workflowResult,
        )
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
            "plc_container_mirror": plcContainerMirrorResult,
            "mission_sorting": missionSortResult,
            "workflow_cycles": workflowResult,
        },
        server_status=serverStatusResult,
        robot_state=robotStateResult,
        container_state=containerStateResult,
        plc_container_mirror=plcContainerMirrorResult,
        mission_sorting=missionSortResult,
        workflow_cycles=workflowResult,
    )
