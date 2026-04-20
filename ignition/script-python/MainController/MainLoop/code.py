import time

from MainController import CommandCalls
from MainController import CommandHelpers
from MainController.WorkflowConfig import ROBOT_NAMES


def runRobotWorkflowCycle(
    robotName,
    reservedWorkflows=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Compatibility passthrough to the real workflow-cycle implementation."""
    return CommandCalls.runRobotWorkflowCycle(
        robotName,
        reservedWorkflows=reservedWorkflows,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )


def runAllRobotWorkflowCycles(
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Run one cycle for all configured robots."""
    return CommandCalls.runAllRobotWorkflowCycles(
        robotNames=ROBOT_NAMES,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )


def runMainControllerCycle(
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """
    Top-level timer entrypoint with overlap protection and runtime timing telemetry.

    The actual business logic lives in CommandCalls; this wrapper owns loop timing
    and makes sure we do not stack overlapping cycles.
    """
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    CommandHelpers.ensureRuntimeTags()
    runtimeState = CommandHelpers.readRuntimeState()
    if runtimeState["loop_is_running"]:
        overlapCount = runtimeState["loop_overlap_count"] + 1
        CommandHelpers.writeRuntimeFields({
            "loop_overlap_count": overlapCount,
            "loop_last_result": "overlap_skipped",
        })
        system.util.getLogger("MainController_MainLoop").warn(
            "Skipping MainController cycle because the previous loop is still running"
        )
        return {
            "ok": False,
            "level": "warn",
            "message": "Skipped MainController cycle because the previous loop is still running",
            "data": {"overlap_count": overlapCount},
            "overlap_count": overlapCount,
        }

    startEpochMs = int(nowEpochMs)
    # Mark the loop as running before any controller work starts so the next timer
    # tick can detect overlap immediately.
    CommandHelpers.writeRuntimeFields({
        "loop_is_running": True,
        "loop_last_start_ts": CommandHelpers.timestampString(startEpochMs),
        "loop_last_result": "running",
    })

    result = None
    try:
        result = CommandCalls.runMainControllerCycle(
            nowEpochMs=startEpochMs,
            createMission=createMission,
            finalizeMissionId=finalizeMissionId,
            cancelMissionIds=cancelMissionIds,
        )
        return result
    finally:
        endEpochMs = int(time.time() * 1000)
        durationMs = max(0, endEpochMs - startEpochMs)
        lastResult = "unknown"
        if result is not None:
            lastResult = str(result.get("level", "info")) + ":" + str(result.get("message", ""))
        CommandHelpers.writeRuntimeFields({
            "loop_is_running": False,
            "loop_last_end_ts": CommandHelpers.timestampString(endEpochMs),
            "loop_last_duration_ms": durationMs,
            "loop_last_result": lastResult,
        })
