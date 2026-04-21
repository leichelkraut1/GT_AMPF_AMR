import time

from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureMemoryTag
from Otto_API.Common.TagHelpers import getMainControlRuntimePath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import writeTagValues


RUNTIME_BASE = getMainControlRuntimePath()

COMMAND_HISTORY_HEADERS = [
    "Ts",
    "Robot",
    "RequestedWorkflowNumber",
    "ActiveWorkflowNumber",
    "Action",
    "Level",
    "State",
    "Message",
]

MISSION_STATE_HISTORY_HEADERS = [
    "Ts",
    "Robot",
    "MissionId",
    "MissionName",
    "OldStatus",
    "NewStatus",
    "WorkflowNumber",
]

ROBOT_STATE_HISTORY_HEADERS = [
    "Ts",
    "Robot",
    "OldSystemState",
    "NewSystemState",
    "OldSubSystemState",
    "NewSubSystemState",
    "OldActivityState",
    "NewActivityState",
]

HTTP_HISTORY_HEADERS = [
    "Ts",
    "Method",
    "Url",
    "Request",
    "Response",
    "Ok",
    "DurationMs",
    "Error",
]

COMMAND_HISTORY_MAX_ROWS = 100
MISSION_STATE_HISTORY_MAX_ROWS = 100
ROBOT_STATE_HISTORY_MAX_ROWS = 100
HTTP_HISTORY_MAX_ROWS = 500


def runtimePaths():
    """Shared runtime telemetry and history tags for the main loop and OTTO history helpers."""
    return {
        "base": RUNTIME_BASE,
        "loop_is_running": RUNTIME_BASE + "/LoopIsRunning",
        "loop_last_start_ts": RUNTIME_BASE + "/LoopLastStartTs",
        "loop_last_end_ts": RUNTIME_BASE + "/LoopLastEndTs",
        "loop_last_duration_ms": RUNTIME_BASE + "/LoopLastDurationMs",
        "loop_last_result": RUNTIME_BASE + "/LoopLastResult",
        "loop_overlap_count": RUNTIME_BASE + "/LoopOverlapCount",
        "command_history": RUNTIME_BASE + "/CommandHistory",
        "mission_state_history": RUNTIME_BASE + "/MissionStateHistory",
        "robot_state_history": RUNTIME_BASE + "/RobotStateHistory",
        "http_history": RUNTIME_BASE + "/HttpHistory",
        "http_get_history": RUNTIME_BASE + "/HttpGetHistory",
        "http_post_history": RUNTIME_BASE + "/HttpPostHistory",
    }


def ensureRuntimeTags():
    """Create runtime telemetry and history dataset tags for the main loop."""
    paths = runtimePaths()
    ensureFolder(RUNTIME_BASE)
    ensureMemoryTag(paths["loop_is_running"], "Boolean", False)
    ensureMemoryTag(paths["loop_last_start_ts"], "String", "")
    ensureMemoryTag(paths["loop_last_end_ts"], "String", "")
    ensureMemoryTag(paths["loop_last_duration_ms"], "Int4", 0)
    ensureMemoryTag(paths["loop_last_result"], "String", "")
    ensureMemoryTag(paths["loop_overlap_count"], "Int4", 0)
    ensureMemoryTag(
        paths["command_history"],
        "DataSet",
        system.dataset.toDataSet(COMMAND_HISTORY_HEADERS, [])
    )
    ensureMemoryTag(
        paths["mission_state_history"],
        "DataSet",
        system.dataset.toDataSet(MISSION_STATE_HISTORY_HEADERS, [])
    )
    ensureMemoryTag(
        paths["robot_state_history"],
        "DataSet",
        system.dataset.toDataSet(ROBOT_STATE_HISTORY_HEADERS, [])
    )
    ensureMemoryTag(
        paths["http_history"],
        "DataSet",
        system.dataset.toDataSet(HTTP_HISTORY_HEADERS, [])
    )
    ensureMemoryTag(
        paths["http_get_history"],
        "DataSet",
        system.dataset.toDataSet(HTTP_HISTORY_HEADERS, [])
    )
    ensureMemoryTag(
        paths["http_post_history"],
        "DataSet",
        system.dataset.toDataSet(HTTP_HISTORY_HEADERS, [])
    )


def timestampString(nowEpochMs=None):
    """Return a stable local timestamp string for tag writes and history rows."""
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)
    return time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(float(nowEpochMs) / 1000.0)
    )


def appendRuntimeDatasetRow(
    fieldName,
    headers,
    rowValues,
    maxRows=500
):
    """Append one row to a runtime history dataset and cap it to the most recent rows."""
    ensureRuntimeTags()
    paths = runtimePaths()
    datasetPath = paths.get(fieldName)
    if not datasetPath:
        return

    currentValue = readOptionalTagValue(
        datasetPath,
        system.dataset.toDataSet(headers, [])
    )
    if not currentValue:
        currentValue = system.dataset.toDataSet(headers, [])

    updated = system.dataset.addRow(currentValue, rowValues)
    if hasattr(updated, "getRowCount") and updated.getRowCount() > maxRows:
        rows = []
        startIndex = max(0, updated.getRowCount() - maxRows)
        for rowIndex in range(startIndex, updated.getRowCount()):
            rows.append([
                updated.getValueAt(rowIndex, header)
                for header in list(headers or [])
            ])
        updated = system.dataset.toDataSet(headers, rows)

    writeTagValues([datasetPath], [updated])


def readRuntimeDataset(fieldName, headers):
    """Read a runtime dataset tag, returning an empty dataset when missing."""
    ensureRuntimeTags()
    datasetPath = runtimePaths().get(fieldName)
    if not datasetPath:
        return system.dataset.toDataSet(headers, [])

    datasetValue = readOptionalTagValue(
        datasetPath,
        system.dataset.toDataSet(headers, [])
    )
    if not datasetValue:
        return system.dataset.toDataSet(headers, [])
    return datasetValue


def buildLatestMissionStateHistoryStatusMap():
    """Build {MissionId: LatestNewStatus} from the runtime mission history dataset."""
    datasetValue = readRuntimeDataset(
        "mission_state_history",
        MISSION_STATE_HISTORY_HEADERS
    )
    if not hasattr(datasetValue, "getRowCount"):
        return {}

    latestByMissionId = {}
    for rowIndex in range(datasetValue.getRowCount() - 1, -1, -1):
        missionId = str(datasetValue.getValueAt(rowIndex, "MissionId") or "")
        if not missionId or missionId in latestByMissionId:
            continue
        latestByMissionId[missionId] = str(datasetValue.getValueAt(rowIndex, "NewStatus") or "")
    return latestByMissionId


def buildRobotStateLogSignature(
    robotName,
    oldSystemState,
    newSystemState,
    oldSubSystemState,
    newSubSystemState,
    oldActivityState,
    newActivityState
):
    """Build the signature used to suppress duplicate robot-state history rows."""
    return "|".join([
        str(robotName or ""),
        str(oldSystemState or ""),
        str(newSystemState or ""),
        str(oldSubSystemState or ""),
        str(newSubSystemState or ""),
        str(oldActivityState or ""),
        str(newActivityState or ""),
    ])


def appendRobotStateHistoryRow(
    nowTimestamp,
    robotName,
    oldSystemState,
    newSystemState,
    oldSubSystemState,
    newSubSystemState,
    oldActivityState,
    newActivityState
):
    """Append one robot operational-state transition row to runtime history."""
    appendRuntimeDatasetRow(
        "robot_state_history",
        ROBOT_STATE_HISTORY_HEADERS,
        [
            str(nowTimestamp or ""),
            str(robotName or ""),
            str(oldSystemState or ""),
            str(newSystemState or ""),
            str(oldSubSystemState or ""),
            str(newSubSystemState or ""),
            str(oldActivityState or ""),
            str(newActivityState or ""),
        ],
        maxRows=ROBOT_STATE_HISTORY_MAX_ROWS,
    )


def _historyText(value, maxLen=4000):
    text = "" if value is None else str(value)
    if len(text) <= maxLen:
        return text
    return text[: maxLen - 3] + "..."


def _httpHistoryFieldNames(method):
    normalizedMethod = str(method or "").upper()
    fieldNames = ["http_history"]
    if normalizedMethod == "GET":
        fieldNames.append("http_get_history")
    elif normalizedMethod == "POST":
        fieldNames.append("http_post_history")
    return fieldNames


def appendHttpHistoryRow(
    nowTimestamp,
    method,
    url,
    requestText,
    responseText,
    ok,
    durationMs,
    errorText=""
):
    """Append one HTTP request/response row to runtime history."""
    rowValues = [
        str(nowTimestamp or ""),
        str(method or ""),
        str(url or ""),
        _historyText(requestText),
        _historyText(responseText),
        bool(ok),
        int(durationMs or 0),
        _historyText(errorText),
    ]
    for fieldName in _httpHistoryFieldNames(method):
        appendRuntimeDatasetRow(
            fieldName,
            HTTP_HISTORY_HEADERS,
            rowValues,
            maxRows=HTTP_HISTORY_MAX_ROWS,
        )
