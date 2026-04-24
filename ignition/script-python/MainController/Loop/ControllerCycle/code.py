from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.RuntimeHistory import recordRuntimeIssues
from Otto_API.Containers import Get as ContainerGet
from Otto_API.Missions import MissionSorting
from Otto_API.Robots import Get as RobotGet
from Otto_API.System import Get as SystemGet

from MainController.Robot.Cycle import runRobotWorkflowCycleSnapshot
from MainController.Robot.Snapshot import readRobotCycleSnapshot
from MainController.State.ContainerMirror import mirrorPlcPlaces
from MainController.State.MissionMirror import writeMissionSortingRobotMirror
from MainController.State.PlcMappingStore import readPlcMappings
from MainController.State.Paths import ROBOT_NAMES
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
    plcPlaceSyncResult,
    plcRobotSyncResult,
    missionSortResult,
    workflowResult
):
    phaseResults = [
        ("server_status", "Server Status", serverStatusResult),
        ("robot_state", "Robot Sync", robotStateResult),
        ("container_state", "Container Sync", containerStateResult),
        ("plc_robot_fleet_sync", "PLC Robot Fleet Sync", plcRobotSyncResult),
        ("plc_place_fleet_sync", "PLC Place Fleet Sync", plcPlaceSyncResult),
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


def _log():
    return system.util.getLogger("MainController.Loop.ControllerCycle")


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


def _mergeResults(label, *results):
    """Merge same-shape phase or sync results into one summary result."""
    normalizedResults = []
    for result in list(results or []):
        if result is None:
            continue
        normalizedResults.append(dict(result or {}))

    if not normalizedResults:
        return buildOperationResult(
            True,
            "info",
            "{} healthy".format(label),
            data={"results": []},
            results=[],
        )

    unhealthy = [result for result in normalizedResults if not result.get("ok", False)]
    if not unhealthy:
        return buildOperationResult(
            True,
            "info",
            "{} healthy".format(label),
            data={"results": normalizedResults},
            results=normalizedResults,
        )

    level = "error" if any(str(result.get("level") or "").lower() == "error" for result in unhealthy) else "warn"
    message = "; ".join([
        str(result.get("message") or "unhealthy")
        for result in unhealthy
    ]) or "{} degraded".format(label)
    return buildOperationResult(
        False,
        level,
        message,
        data={"results": normalizedResults},
        results=normalizedResults,
    )


def _robotCycleExceptionResult(robotName, exc):
    message = "Robot [{}] workflow cycle failed: {}".format(robotName, str(exc))
    _log().error(message)
    return buildOperationResult(
        False,
        "error",
        message,
        data={
            "robot_name": robotName,
            "state": "fault",
            "action": "robot_cycle_exception",
        },
        robot_name=robotName,
        state="fault",
        action="robot_cycle_exception",
    )


def _missionMirrorResult(missionSortResult):
    robotSummaryByFolder = dict(
        dict(missionSortResult.get("data") or {}).get("robot_summary_by_folder")
        or missionSortResult.get("robot_summary_by_folder")
        or {}
    )
    try:
        writeMissionSortingRobotMirror(robotSummaryByFolder)
        return buildOperationResult(
            True,
            "info",
            "Mission summary mirror healthy",
            data={"robot_summary_by_folder": robotSummaryByFolder},
            issues=[],
        )
    except Exception as exc:
        message = "Mission summary mirror failed: {}".format(str(exc))
        return buildOperationResult(
            False,
            "warn",
            message,
            data={"robot_summary_by_folder": robotSummaryByFolder},
            issues=[
                buildRuntimeIssue(
                    "mission_sorting.summary_mirror_failed",
                    "MainController.Loop.ControllerCycle",
                    "warn",
                    message,
                )
            ],
        )


def _plcPlaceSyncResult(containerStateResult, plcMappingState):
    if containerStateResult.get("ok"):
        return mirrorPlcPlaces(plcMappingState=plcMappingState)
    return buildOperationResult(
        False,
        "warn",
        "Skipped PLC place sync because container state is stale",
        data={"rows": [], "writes": []},
        rows=[],
        writes=[],
    )


def _workflowCycleResults(
    plcMappingState,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None,
):
    """Run robot workflow cycles and normalize the aggregated PLC robot sync result."""
    workflowResult = runAllRobotWorkflowCycles(
        plcMappingState=plcMappingState,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )
    plcRobotSyncResult = dict(
        workflowResult.get("plc_robot_sync")
        or buildOperationResult(True, "info", "PLC robot sync healthy")
    )
    return workflowResult, plcRobotSyncResult


def _mainCycleResults(
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None,
):
    """Run the ordered shared phases and return the normalized cycle result bundle."""
    serverStatusResult = SystemGet.readCachedServerStatus()
    missionSortResult = MissionSorting.run()
    missionSummaryResult = _missionMirrorResult(missionSortResult)
    robotStateResult = RobotGet.updateRobotOperationalState()
    containerStateResult = ContainerGet.updateContainers()
    plcMappingState = readPlcMappings()
    plcPlaceSyncResult = _plcPlaceSyncResult(containerStateResult, plcMappingState)
    workflowResult, plcRobotSyncResult = _workflowCycleResults(
        plcMappingState,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )
    return {
        "server_status": serverStatusResult,
        "mission_sorting": missionSortResult,
        "mission_summary_mirror": missionSummaryResult,
        "robot_state": robotStateResult,
        "container_state": containerStateResult,
        "plc_mapping": plcMappingState,
        "plc_place_sync": plcPlaceSyncResult,
        "plc_robot_sync": plcRobotSyncResult,
        "workflow_cycles": workflowResult,
    }


def _mainCycleOk(results):
    return all([
        results["server_status"].get("ok", False),
        results["mission_sorting"].get("ok", False),
        results["mission_summary_mirror"].get("ok", False),
        results["robot_state"].get("ok", False),
        results["container_state"].get("ok", False),
        results["plc_place_sync"].get("ok", False),
        results["plc_robot_sync"].get("ok", False),
        results["workflow_cycles"].get("ok", False),
    ])


def runAllRobotWorkflowCycles(
    robotNames=None,
    plcMappingState=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Run one workflow-controller pass across every configured robot."""
    if robotNames is None:
        robotNames = ROBOT_NAMES

    snapshots = [
        readRobotCycleSnapshot(
            robotName,
            plcMappingState=plcMappingState,
            nowEpochMs=nowEpochMs,
            createMission=createMission,
            finalizeMissionId=finalizeMissionId,
            cancelMissionIds=cancelMissionIds,
        )
        for robotName in list(robotNames or [])
    ]

    reservedWorkflows = _buildReservedWorkflowsFromSnapshots(snapshots)
    results = []
    plcSyncResults = []
    for snapshot in snapshots:
        snapshot["reserved_workflows"] = reservedWorkflows
        try:
            cycleResult = runRobotWorkflowCycleSnapshot(snapshot)
        except Exception as exc:
            cycleResult = _robotCycleExceptionResult(snapshot["robot_name"], exc)
        results.append(cycleResult)
        syncResult = dict(dict(cycleResult.get("data") or {}).get("plc_sync_result") or {})
        if syncResult:
            plcSyncResults.append(syncResult)

    ok = all(result.get("ok", False) or result.get("level") == "warn" for result in results)
    level = "info" if ok else "error"
    plcRobotSyncResult = _mergeResults("PLC robot sync", *plcSyncResults)
    return buildOperationResult(
        ok,
        level,
        "Processed workflow cycles for {} robot(s)".format(len(results)),
        data={"results": results},
        results=results,
        plc_robot_sync=plcRobotSyncResult,
    )


def runMainControllerCycle(
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Run the ordered controller phases for one main-loop cycle."""
    results = _mainCycleResults(
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )
    recordRuntimeIssues(
        [
            results.get("server_status"),
            results.get("mission_sorting"),
            results.get("mission_summary_mirror"),
            results.get("robot_state"),
            results.get("plc_mapping"),
            results.get("plc_place_sync"),
        ],
        logger=_log(),
    )
    missionPhaseResult = _mergeResults(
        "Mission sorting",
        results["mission_sorting"],
        results["mission_summary_mirror"],
    )

    writeRuntimeFields(
        _controllerRuntimeFields(
            results["server_status"],
            results["robot_state"],
            results["container_state"],
            results["plc_place_sync"],
            results["plc_robot_sync"],
            missionPhaseResult,
            results["workflow_cycles"],
        )
    )

    ok = _mainCycleOk(results)
    level = "info" if ok else "warn"

    return buildOperationResult(
        ok,
        level,
        "MainController cycle completed",
        data=dict(results),
        **results
    )
