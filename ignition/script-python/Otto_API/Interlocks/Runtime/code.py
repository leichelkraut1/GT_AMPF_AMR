import time

from MainController.State.RuntimeStore import writeRuntimeFields
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.RuntimeHistory import INTERLOCK_SYNC_ISSUES_HEADERS
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


def _issueLevelText(levelText):
    normalized = str(levelText or "warn").strip().lower()
    if normalized == "error":
        return "Error"
    if normalized == "info":
        return "Info"
    return "Warn"


def _issuesDatasetFromResult(result):
    result = dict(result or {})
    rows = []

    for warning in list(result.get("warnings") or []):
        if isinstance(warning, dict):
            levelText = _issueLevelText(warning.get("level"))
            message = str(warning.get("message") or "").strip()
        else:
            levelText = "Warn"
            message = str(warning or "").strip()
        if not message:
            continue
        rows.append([levelText, message])

    if (not rows) and (not result.get("ok")):
        fallbackMessage = str(result.get("message") or "").strip()
        if fallbackMessage:
            rows.append([
                _issueLevelText(result.get("level")),
                fallbackMessage,
            ])

    return system.dataset.toDataSet(INTERLOCK_SYNC_ISSUES_HEADERS, rows)


def runInterlockSyncCycle(nowEpochMs=None):
    """
    Run one interlock sync pass while updating shared MainControl/Runtime telemetry.
    """
    logger = _log()
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    startEpochMs = int(nowEpochMs)
    writeRuntimeFields({
        "interlock_sync_is_running": True,
        "interlock_sync_last_start_ts": timestampString(startEpochMs),
    })

    result = None
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
        )
        return result
    finally:
        endEpochMs = int(time.time() * 1000)
        durationMs = max(0, endEpochMs - startEpochMs)
        status = _statusFromResult(result)
        message = _messageFromResult(result)
        issuesDataset = _issuesDatasetFromResult(result)
        lastResult = "unknown"
        if result is not None:
            lastResult = str(result.get("level", "info")) + ":" + str(result.get("message", ""))

        writeRuntimeFields({
            "interlock_sync_is_running": False,
            "interlock_sync_last_end_ts": timestampString(endEpochMs),
            "interlock_sync_last_duration_ms": durationMs,
            "interlock_sync_last_result": lastResult,
            "interlock_sync_status": status,
            "interlock_sync_message": message,
            "interlock_sync_issues": issuesDataset,
        })

    return result
