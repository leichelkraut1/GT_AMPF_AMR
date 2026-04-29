import time

from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.RuntimeHistory import recordRuntimeIssues
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.RuntimeHistory import writeRuntimeFields
from Otto_API.TagSync.Interlocks.Sync import updateInterlocks
from Otto_API.Models.Results import OperationalResult


def _log():
    return system.util.getLogger("Otto_API.TagSync.Interlocks.Runtime")


def _statusFromResult(result):
    if result is not None and result.ok:
        return "Healthy"

    level = "" if result is None else str(result.level or "").lower()
    if level == "error":
        return "Error"
    return "Warn"


def _messageFromResult(result):
    return "" if result is None else str(result.message or "")


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
        result = OperationalResult(
            False,
            "error",
            message,
            sharedFields={
                "issues": [
                    buildRuntimeIssue(
                        "interlocks.runtime.wrapper_exception",
                        "Otto_API.TagSync.Interlocks.Runtime",
                        "error",
                        message,
                    )
                ],
            },
        )
    finally:
        endEpochMs = int(time.time() * 1000)
        durationMs = max(0, endEpochMs - startEpochMs)
        status = _statusFromResult(result)
        message = _messageFromResult(result)
        lastResult = "unknown"
        if result is not None:
            lastResult = str(result.level or "info") + ":" + str(result.message or "")

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

    if result is None:
        result = OperationalResult(
            False,
            "error",
            "Interlock sync did not produce a result",
            sharedFields={
                "issues": [
                    buildRuntimeIssue(
                        "interlocks.runtime.missing_result",
                        "Otto_API.TagSync.Interlocks.Runtime",
                        "error",
                        "Interlock sync did not produce a result",
                    )
                ],
            },
        )

    return result.toDict()
