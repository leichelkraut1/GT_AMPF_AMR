import time

from MainController.Loop import ControllerCycle
from MainController.State.Coerce import toBool
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.RuntimeHistory import runtimePaths
from Otto_API.Common.RuntimeHistory import writeRuntimeFields
from Otto_API.Common.TagIO import readTagValues

STALE_LOOP_RESET_MS = 30000


def _writeRuntimeFields(fieldValues):
    writeRuntimeFields(
        fieldValues,
        required=True,
        label="MainController runtime state",
    )


def _readRuntimeState():
    """Read only the runtime fields needed for overlap protection."""
    paths = runtimePaths()
    values = readTagValues([
        paths["loop_is_running"],
        paths["loop_last_start_ts"],
        paths["loop_retry_after_ts"],
        paths["loop_overlap_count"],
    ])
    return {
        "loop_is_running": toBool(values[0].value if values[0].quality.isGood() else False),
        "loop_last_start_ts": str(values[1].value or "") if values[1].quality.isGood() else "",
        "loop_retry_after_ts": str(values[2].value or "") if values[2].quality.isGood() else "",
        "loop_overlap_count": int(values[3].value or 0) if values[3].quality.isGood() else 0,
    }


def _parseRuntimeTimestampToEpochMillis(timestampText):
    raw = str(timestampText or "").strip()
    if not raw:
        raise ValueError("Missing runtime timestamp")
    return int(time.mktime(time.strptime(raw, "%Y-%m-%d %H:%M:%S")) * 1000)


def _isStaleRunningLoop(runtimeState, nowEpochMs):
    if not bool(dict(runtimeState or {}).get("loop_is_running")):
        return False

    retryAfterText = str(dict(runtimeState or {}).get("loop_retry_after_ts") or "").strip()
    if retryAfterText:
        try:
            retryAfterEpochMs = _parseRuntimeTimestampToEpochMillis(retryAfterText)
            return int(nowEpochMs) >= retryAfterEpochMs
        except Exception:
            pass

    lastStartText = str(dict(runtimeState or {}).get("loop_last_start_ts") or "").strip()
    if not lastStartText:
        return True
    try:
        startEpochMs = _parseRuntimeTimestampToEpochMillis(lastStartText)
    except Exception:
        return True

    return max(0, int(nowEpochMs) - startEpochMs) >= STALE_LOOP_RESET_MS


def _resetStaleRunningLoop(runtimeState, nowEpochMs):
    endEpochMs = int(nowEpochMs)
    lastStartText = str(dict(runtimeState or {}).get("loop_last_start_ts") or "").strip()
    durationMs = None
    if lastStartText:
        try:
            durationMs = max(0, endEpochMs - _parseRuntimeTimestampToEpochMillis(lastStartText))
        except Exception:
            durationMs = None
    if durationMs is None:
        durationMs = STALE_LOOP_RESET_MS

    _writeRuntimeFields({
        "loop_is_running": False,
        "loop_retry_after_ts": "",
        "loop_last_end_ts": timestampString(endEpochMs),
        "loop_last_duration_ms": durationMs,
        "loop_last_result": "stale_reset",
    })
    system.util.getLogger("MainController_MainLoop").warn(
        "Reset stale MainController LoopIsRunning latch before starting a new cycle"
    )


def runMainControllerCycle(
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """
    Top-level timer entrypoint with overlap protection and runtime timing telemetry.

    The actual business logic lives in ControllerCycle; this wrapper owns loop timing
    and makes sure we do not stack overlapping cycles.
    """
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    runtimeState = _readRuntimeState()
    if _isStaleRunningLoop(runtimeState, nowEpochMs):
        _resetStaleRunningLoop(runtimeState, nowEpochMs)
        runtimeState = dict(runtimeState or {})
        runtimeState["loop_is_running"] = False

    if runtimeState["loop_is_running"]:
        rawOverlapCount = runtimeState.get("loop_overlap_count")
        overlapCount = rawOverlapCount + 1 if isinstance(rawOverlapCount, int) else 1
        _writeRuntimeFields({
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
    _writeRuntimeFields({
        "loop_is_running": True,
        "loop_last_start_ts": timestampString(startEpochMs),
        "loop_retry_after_ts": timestampString(startEpochMs + STALE_LOOP_RESET_MS),
        "loop_last_result": "running",
    })

    result = None
    try:
        result = ControllerCycle.runMainControllerCycle(
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
        _writeRuntimeFields({
            "loop_is_running": False,
            "loop_retry_after_ts": "",
            "loop_last_end_ts": timestampString(endEpochMs),
            "loop_last_duration_ms": durationMs,
            "loop_last_result": lastResult,
        })
