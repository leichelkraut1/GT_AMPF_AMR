import time

from MainController import CommandCalls
from MainController import CommandHelpers
from MainController.WorkflowConfig import ROBOT_NAMES


def runRobotWorkflowCycle(
    robotName,
    reservedWorkflows=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMission=None
):
    return CommandCalls.runRobotWorkflowCycle(
        robotName,
        reservedWorkflows=reservedWorkflows,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMission=finalizeMission,
    )


def runAllRobotWorkflowCycles(nowEpochMs=None, createMission=None, finalizeMission=None):
    return CommandCalls.runAllRobotWorkflowCycles(
        robotNames=ROBOT_NAMES,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMission=finalizeMission,
    )


def runMainControllerCycle(nowEpochMs=None, createMission=None, finalizeMission=None):
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
            finalizeMission=finalizeMission,
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
