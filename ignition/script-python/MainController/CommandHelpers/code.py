import re
import time
import uuid

from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureMemoryTag
from Otto_API.Common.TagHelpers import getFleetMissionsPath
from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getMainControlInternalPath
from Otto_API.Common.TagHelpers import getMainControlRobotsPath
from Otto_API.Common.TagHelpers import getMainControlRuntimePath
from Otto_API.Common.TagHelpers import getPlcRootPath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import writeTagValues
from Otto_API.Missions.MissionTreeHelpers import browseMissionInstances

from MainController.WorkflowConfig import ROBOT_NAMES
from MainController.WorkflowConfig import normalizeWorkflowNumber


FLEET_ROBOTS_BASE = getFleetRobotsPath()
MAINCONTROL_ROBOTS_BASE = getMainControlRobotsPath()
MISSIONS_ACTIVE_BASE = getFleetMissionsPath() + "/Active"
PLC_BASE = getPlcRootPath()
INTERNAL_BASE = getMainControlInternalPath()
RUNTIME_BASE = getMainControlRuntimePath()

WORKFLOW_NAME_RE = re.compile(r"^WF(\d+)_")


def timestampString(nowEpochMs=None):
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)
    return time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(float(nowEpochMs) / 1000.0)
    )


def newCommandId(uuidFactory=None):
    if uuidFactory is None:
        uuidFactory = uuid.uuid4
    return str(uuidFactory())


def defaultRobotState():
    return {
        "request_latched": False,
        "selected_workflow_number": 0,
        "state": "idle",
        "mission_created": False,
        "mission_needs_finalized": False,
        "last_command_ts": "",
        "last_result": "",
        "last_command_id": "",
    }


def internalStatePaths(robotName):
    basePath = INTERNAL_BASE + "/" + robotName
    return {
        "base": basePath,
        "request_latched": basePath + "/RequestLatched",
        "selected_workflow_number": basePath + "/SelectedWorkflowNumber",
        "state": basePath + "/State",
        "mission_created": basePath + "/MissionCreated",
        "mission_needs_finalized": basePath + "/MissionNeedsFinalized",
        "last_command_ts": basePath + "/LastCommandTs",
        "last_result": basePath + "/LastResult",
        "last_command_id": basePath + "/LastCommandId",
    }


def plcPaths(robotName):
    basePath = PLC_BASE + "/" + robotName
    fromPlc = basePath + "/FromPLC"
    toPlc = basePath + "/ToPLC"
    return {
        "base": basePath,
        "from_plc": fromPlc,
        "to_plc": toPlc,
        "request_active": fromPlc + "/RequestActive",
        "requested_workflow_number": fromPlc + "/RequestedWorkflowNumber",
        "finalize_ok": fromPlc + "/FinalizeOk",
        "available_for_work": toPlc + "/AvailableForWork",
        "active_workflow_number": toPlc + "/ActiveWorkflowNumber",
        "mission_ready_for_attachment": toPlc + "/MissionReadyforAttachment",
        "mission_needs_finalized": toPlc + "/MissionNeedsFinalized",
        "request_conflict": toPlc + "/RequestConflict",
        "request_invalid": toPlc + "/RequestInvalid",
    }


def runtimePaths():
    return {
        "base": RUNTIME_BASE,
        "loop_is_running": RUNTIME_BASE + "/LoopIsRunning",
        "loop_last_start_ts": RUNTIME_BASE + "/LoopLastStartTs",
        "loop_last_end_ts": RUNTIME_BASE + "/LoopLastEndTs",
        "loop_last_duration_ms": RUNTIME_BASE + "/LoopLastDurationMs",
        "loop_last_result": RUNTIME_BASE + "/LoopLastResult",
        "loop_overlap_count": RUNTIME_BASE + "/LoopOverlapCount",
    }


def ensureRobotRunnerTags(robotName):
    internalPaths = internalStatePaths(robotName)
    plcTagPaths = plcPaths(robotName)

    ensureMainControlRobotTags(robotName)
    ensureFolder(INTERNAL_BASE)
    ensureFolder(internalPaths["base"])
    ensureFolder(PLC_BASE)
    ensureFolder(plcTagPaths["base"])
    ensureFolder(plcTagPaths["from_plc"])
    ensureFolder(plcTagPaths["to_plc"])

    ensureMemoryTag(internalPaths["request_latched"], "Boolean", False)
    ensureMemoryTag(internalPaths["selected_workflow_number"], "Int4", 0)
    ensureMemoryTag(internalPaths["state"], "String", "idle")
    ensureMemoryTag(internalPaths["mission_created"], "Boolean", False)
    ensureMemoryTag(internalPaths["mission_needs_finalized"], "Boolean", False)
    ensureMemoryTag(internalPaths["last_command_ts"], "String", "")
    ensureMemoryTag(internalPaths["last_result"], "String", "")
    ensureMemoryTag(internalPaths["last_command_id"], "String", "")

    ensureMemoryTag(plcTagPaths["request_active"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["requested_workflow_number"], "Int4", 0)
    ensureMemoryTag(plcTagPaths["finalize_ok"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["available_for_work"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["active_workflow_number"], "Int4", 0)
    ensureMemoryTag(plcTagPaths["mission_ready_for_attachment"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["mission_needs_finalized"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["request_conflict"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["request_invalid"], "Boolean", False)


def ensureMainControlRobotTags(robotName):
    robotPath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
    ensureFolder(MAINCONTROL_ROBOTS_BASE)
    ensureFolder(robotPath)
    ensureMemoryTag(robotPath + "/MissionReadyforAttachment", "Boolean", False)
    ensureMemoryTag(robotPath + "/MissionIdForAttacment", "String", "")
    ensureMemoryTag(robotPath + "/MissionNameForAttachment", "String", "")


def ensureRuntimeTags():
    paths = runtimePaths()
    ensureFolder(RUNTIME_BASE)
    ensureMemoryTag(paths["loop_is_running"], "Boolean", False)
    ensureMemoryTag(paths["loop_last_start_ts"], "String", "")
    ensureMemoryTag(paths["loop_last_end_ts"], "String", "")
    ensureMemoryTag(paths["loop_last_duration_ms"], "Int4", 0)
    ensureMemoryTag(paths["loop_last_result"], "String", "")
    ensureMemoryTag(paths["loop_overlap_count"], "Int4", 0)


def readRuntimeState():
    paths = runtimePaths()
    values = readTagValues([
        paths["loop_is_running"],
        paths["loop_overlap_count"],
    ])
    return {
        "loop_is_running": _toBool(values[0].value if values[0].quality.isGood() else False),
        "loop_overlap_count": int(values[1].value or 0) if values[1].quality.isGood() else 0,
    }


def writeRuntimeFields(fieldValues):
    paths = runtimePaths()
    writePaths = []
    writeValues = []
    for fieldName, value in list(dict(fieldValues or {}).items()):
        path = paths.get(fieldName)
        if not path:
            continue
        writePaths.append(path)
        writeValues.append(value)
    if writePaths:
        writeTagValues(writePaths, writeValues)


def _toBool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ["true", "1", "yes", "on"]


def normalizeRobotState(rawState):
    rawState = dict(rawState or {})
    state = defaultRobotState()
    state["request_latched"] = _toBool(rawState.get("request_latched"))
    state["selected_workflow_number"] = normalizeWorkflowNumber(
        rawState.get("selected_workflow_number")
    ) or 0
    state["state"] = str(rawState.get("state") or "idle")
    state["mission_created"] = _toBool(rawState.get("mission_created"))
    state["mission_needs_finalized"] = _toBool(rawState.get("mission_needs_finalized"))
    state["last_command_ts"] = str(rawState.get("last_command_ts") or "")
    state["last_result"] = str(rawState.get("last_result") or "")
    state["last_command_id"] = str(rawState.get("last_command_id") or "")
    return state


def readRobotState(robotName):
    paths = internalStatePaths(robotName)
    values = readTagValues([
        paths["request_latched"],
        paths["selected_workflow_number"],
        paths["state"],
        paths["mission_created"],
        paths["mission_needs_finalized"],
        paths["last_command_ts"],
        paths["last_result"],
        paths["last_command_id"],
    ])

    rawState = {}
    keys = [
        "request_latched",
        "selected_workflow_number",
        "state",
        "mission_created",
        "mission_needs_finalized",
        "last_command_ts",
        "last_result",
        "last_command_id",
    ]
    for key, qualifiedValue in zip(keys, values):
        rawState[key] = qualifiedValue.value if qualifiedValue.quality.isGood() else None
    return normalizeRobotState(rawState)


def writeRobotState(robotName, state):
    state = normalizeRobotState(state)
    paths = internalStatePaths(robotName)
    writeTagValues(
        [
            paths["request_latched"],
            paths["selected_workflow_number"],
            paths["state"],
            paths["mission_created"],
            paths["mission_needs_finalized"],
            paths["last_command_ts"],
            paths["last_result"],
            paths["last_command_id"],
        ],
        [
            state["request_latched"],
            state["selected_workflow_number"],
            state["state"],
            state["mission_created"],
            state["mission_needs_finalized"],
            state["last_command_ts"],
            state["last_result"],
            state["last_command_id"],
        ]
    )


def readPlcInputs(robotName):
    paths = plcPaths(robotName)
    return {
        "request_active": _toBool(readOptionalTagValue(paths["request_active"], False)),
        "requested_workflow_number": normalizeWorkflowNumber(
            readOptionalTagValue(paths["requested_workflow_number"], 0)
        ) or 0,
        "finalize_ok": _toBool(readOptionalTagValue(paths["finalize_ok"], False)),
    }


def writePlcOutputs(robotName, outputs):
    paths = plcPaths(robotName)
    outputs = dict(outputs or {})
    writeTagValues(
        [
            paths["available_for_work"],
            paths["active_workflow_number"],
            paths["mission_ready_for_attachment"],
            paths["mission_needs_finalized"],
            paths["request_conflict"],
            paths["request_invalid"],
        ],
        [
            _toBool(outputs.get("available_for_work")),
            normalizeWorkflowNumber(outputs.get("active_workflow_number")) or 0,
            _toBool(outputs.get("mission_ready_for_attachment")),
            _toBool(outputs.get("mission_needs_finalized")),
            _toBool(outputs.get("request_conflict")),
            _toBool(outputs.get("request_invalid")),
        ]
    )


def readRobotMirrorInputs(robotName):
    robotPath = FLEET_ROBOTS_BASE + "/" + robotName
    mainControlRobotPath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
    return {
        "available_for_work": _toBool(readOptionalTagValue(robotPath + "/AvailableForWork", False)),
        "mission_ready_for_attachment": _toBool(
            readOptionalTagValue(
                mainControlRobotPath + "/MissionReadyforAttachment",
                readOptionalTagValue(robotPath + "/MissionReadyforAttachment", False)
            )
        ),
    }


def parseActiveWorkflowNumberFromMissionName(missionName):
    text = str(missionName or "").strip()
    if not text:
        return None

    match = WORKFLOW_NAME_RE.match(text)
    if not match:
        return None

    return normalizeWorkflowNumber(match.group(1))


def readActiveMissionSummary(robotName):
    rootPath = MISSIONS_ACTIVE_BASE + "/" + robotName
    missionInstances = browseMissionInstances(rootPath)
    if not missionInstances:
        return {
            "count": 0,
            "mission_name": "",
            "workflow_number": None,
        }

    namePaths = [fullPath + "/Name" for fullPath, _ in missionInstances]
    nameResults = readTagValues(namePaths)

    missionNames = []
    for qualifiedValue in nameResults:
        if not qualifiedValue.quality.isGood():
            continue
        value = qualifiedValue.value
        if value is None:
            continue
        text = str(value).strip()
        if text:
            missionNames.append(text)

    missionName = missionNames[0] if missionNames else ""
    return {
        "count": len(missionInstances),
        "mission_name": missionName,
        "workflow_number": parseActiveWorkflowNumberFromMissionName(missionName),
    }


def buildWorkflowReservedMap(robotNames):
    reserved = {}
    for robotName in list(robotNames or ROBOT_NAMES):
        activeSummary = readActiveMissionSummary(robotName)
        workflowNumber = activeSummary.get("workflow_number")
        if workflowNumber is not None:
            reserved[workflowNumber] = robotName
            continue

        state = readRobotState(robotName)
        selectedWorkflow = normalizeWorkflowNumber(
            state.get("selected_workflow_number")
        )
        if not selectedWorkflow:
            continue
        if state.get("request_latched") or state.get("mission_needs_finalized") or state.get("mission_created"):
            reserved[selectedWorkflow] = robotName
    return reserved


def buildCycleResult(ok, level, message, robotName=None, state=None, action=None, data=None):
    payload = {
        "robot_name": robotName,
        "state": state,
        "action": action,
    }
    if data:
        payload.update(data)

    return buildOperationResult(
        ok,
        level,
        message,
        data=payload,
        robot_name=robotName,
        state=state,
        action=action,
    )
