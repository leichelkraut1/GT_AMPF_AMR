import time

from MainController.State.RuntimeStore import writeRuntimeFields
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.RuntimeHistory import recordRuntimeIssues
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Interlocks.Sync import updateInterlocks


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Runtime")


def _statusFromResult(result):
    result = dict(result or {})
    if result.get("ok"):
        return "Healthy"

    level = str(result.get("level") or "").lower()
    if level == "error":
        return "Error"
    return "Warn"


def _messageFromResult(result):
    return str(dict(result or {}).get("message") or "")


def runInterlockSyncCycle(nowEpochMs=None):
    """
    Run one interlock sync pass while updating shared MainControl/Runtime telemetry.
    """
    logger = _log()
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    startEpochMs = int(nowEpochMs)
    result = None

    try:
        writeRuntimeFields({
            "interlock_sync_is_running": True,
            "interlock_sync_last_start_ts": timestampString(startEpochMs),
        })
    except Exception as exc:
        logger.error("Interlock sync runtime start telemetry failed: {}".format(str(exc)))

    try:
        result = updateInterlocks()
    except Exception as exc:
        message = "Interlock sync runtime wrapper failed: {}".format(str(exc))
        logger.error(message)
        result = buildOperationResult(
            False,
            "error",
            message,
            data={},
            issues=[
                buildRuntimeIssue(
                    "interlocks.runtime.wrapper_exception",
                    "Otto_API.Interlocks.Runtime",
                    "error",
                    message,
                )
            ],
        )
        return result
    finally:
        endEpochMs = int(time.time() * 1000)
        durationMs = max(0, endEpochMs - startEpochMs)
        status = _statusFromResult(result)
        message = _messageFromResult(result)
        lastResult = "unknown"
        if result is not None:
            lastResult = str(result.get("level", "info")) + ":" + str(result.get("message", ""))

        recordRuntimeIssues([result], nowEpochMs=endEpochMs, logger=logger)

        try:
            writeRuntimeFields({
                "interlock_sync_is_running": False,
                "interlock_sync_last_end_ts": timestampString(endEpochMs),
                "interlock_sync_last_duration_ms": durationMs,
                "interlock_sync_last_result": lastResult,
                "interlock_sync_status": status,
                "interlock_sync_message": message,
            })
        except Exception as exc:
            logger.error("Interlock sync runtime end telemetry failed: {}".format(str(exc)))

    return result
