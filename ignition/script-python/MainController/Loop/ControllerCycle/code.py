from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Containers import Get as ContainerGet
from Otto_API.Missions import MissionSorting
from Otto_API.Robots import Get as RobotGet
from Otto_API.System import Get as SystemGet

from MainController.Robot.Cycle import runRobotWorkflowCycleSnapshot
from MainController.Robot.Snapshot import readRobotCycleSnapshot
from MainController.State.ContainerMirror import mirrorPlcContainerOccupancy
from MainController.State.Paths import ROBOT_NAMES
from MainController.State.PlcStore import writePlcHealthOutputs
from MainController.State.Provisioning import ensureRobotRunnerTags
from MainController.State.RuntimeStore import writeRuntimeFields
from MainController.WorkflowConfig import normalizeWorkflowNumber


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
    serverStatusResult,
    robotStateResult,
    containerStateResult,
    plcContainerMirrorResult,
    missionSortResult,
    workflowResult
):
    phaseResults = [
        ("server_status", "Server Status", serverStatusResult),
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


def _buildReservedWorkflowsFromSnapshots(snapshots):
    reserved = {}
    for snapshot in list(snapshots or []):
        activeWorkflowNumber = normalizeWorkflowNumber(
            dict(snapshot.get("active_summary") or {}).get("workflow_number")
        )
        if activeWorkflowNumber:
            reserved[activeWorkflowNumber] = snapshot["robot_name"]
            continue

        currentState = dict(snapshot.get("current_state") or {})
        selectedWorkflowNumber = normalizeWorkflowNumber(
            currentState.get("selected_workflow_number")
        )
        if not selectedWorkflowNumber:
            continue

        if (
            currentState.get("request_latched")
            or currentState.get("mission_needs_finalized")
            or currentState.get("mission_created")
        ):
            reserved[selectedWorkflowNumber] = snapshot["robot_name"]
    return reserved


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

    snapshots = []
    for robotName in list(robotNames or []):
        snapshots.append(
            readRobotCycleSnapshot(
                robotName,
                nowEpochMs=nowEpochMs,
                createMission=createMission,
                finalizeMissionId=finalizeMissionId,
                cancelMissionIds=cancelMissionIds,
            )
        )

    reservedWorkflows = _buildReservedWorkflowsFromSnapshots(snapshots)
    results = []
    for snapshot in snapshots:
        snapshot["reserved_workflows"] = reservedWorkflows
        results.append(
            runRobotWorkflowCycleSnapshot(snapshot)
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
    missionSortResult = MissionSorting.run()
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
            serverStatusResult,
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
