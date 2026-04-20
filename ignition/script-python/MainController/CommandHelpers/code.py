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
    """Return a stable local timestamp string for tag writes and history rows."""
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)
    return time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(float(nowEpochMs) / 1000.0)
    )


def newCommandId(uuidFactory=None):
    """Build a controller-owned command id for tracing create/finalize/cancel actions."""
    if uuidFactory is None:
        uuidFactory = uuid.uuid4
    return str(uuidFactory())


def defaultRobotState():
    """Canonical internal state for one robot runner."""
    return {
        "force_robot_ready": False,
        "request_latched": False,
        "selected_workflow_number": 0,
        "state": "idle",
        "mission_created": False,
        "mission_needs_finalized": False,
        "last_command_ts": "",
        "last_result": "",
        "last_command_id": "",
        "last_logged_signature": "",
        "last_computed_log_signature": "",
        "last_log_decision": "",
    }


def internalStatePaths(robotName):
    """Centralize MainControl/Internal paths so the runner only has one tag contract to maintain."""
    basePath = INTERNAL_BASE + "/" + robotName
    return {
        "base": basePath,
        "force_robot_ready": basePath + "/ForceRobotReady",
        "request_latched": basePath + "/RequestLatched",
        "selected_workflow_number": basePath + "/SelectedWorkflowNumber",
        "state": basePath + "/State",
        "mission_created": basePath + "/MissionCreated",
        "mission_needs_finalized": basePath + "/MissionNeedsFinalized",
        "last_command_ts": basePath + "/LastCommandTs",
        "last_result": basePath + "/LastResult",
        "last_command_id": basePath + "/LastCommandId",
        "last_logged_signature": basePath + "/LastLoggedSignature",
        "last_computed_log_signature": basePath + "/LastComputedLogSignature",
        "last_log_decision": basePath + "/LastLogDecision",
    }


def plcPaths(robotName):
    """Return the PLC-facing input/output contract for a robot."""
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
        "request_received": toPlc + "/RequestReceived",
        "request_success": toPlc + "/RequestSuccess",
        "request_robot_not_ready": toPlc + "/RequestRobotNotReady",
        "fleet_fault": toPlc + "/FleetFault",
        "plc_comm_fault": toPlc + "/PlcCommFault",
        "control_healthy": toPlc + "/ControlHealthy",
        "request_conflict": toPlc + "/RequestConflict",
        "request_invalid": toPlc + "/RequestInvalid",
    }


def runtimePaths():
    """Shared runtime telemetry and history tags for the top-level loop."""
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
    }


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


def ensureRobotRunnerTags(robotName):
    """Provision the per-robot controller state and PLC interface tags on demand."""
    internalPaths = internalStatePaths(robotName)
    plcTagPaths = plcPaths(robotName)

    ensureMainControlRobotTags(robotName)
    ensureFolder(INTERNAL_BASE)
    ensureFolder(internalPaths["base"])
    ensureFolder(PLC_BASE)
    ensureFolder(plcTagPaths["base"])
    ensureFolder(plcTagPaths["from_plc"])
    ensureFolder(plcTagPaths["to_plc"])

    ensureMemoryTag(internalPaths["force_robot_ready"], "Boolean", False)
    ensureMemoryTag(internalPaths["request_latched"], "Boolean", False)
    ensureMemoryTag(internalPaths["selected_workflow_number"], "Int4", 0)
    ensureMemoryTag(internalPaths["state"], "String", "idle")
    ensureMemoryTag(internalPaths["mission_created"], "Boolean", False)
    ensureMemoryTag(internalPaths["mission_needs_finalized"], "Boolean", False)
    ensureMemoryTag(internalPaths["last_command_ts"], "String", "")
    ensureMemoryTag(internalPaths["last_result"], "String", "")
    ensureMemoryTag(internalPaths["last_command_id"], "String", "")
    ensureMemoryTag(internalPaths["last_logged_signature"], "String", "")
    ensureMemoryTag(internalPaths["last_computed_log_signature"], "String", "")
    ensureMemoryTag(internalPaths["last_log_decision"], "String", "")

    ensureMemoryTag(plcTagPaths["request_active"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["requested_workflow_number"], "Int4", 0)
    ensureMemoryTag(plcTagPaths["finalize_ok"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["available_for_work"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["active_workflow_number"], "Int4", 0)
    ensureMemoryTag(plcTagPaths["mission_ready_for_attachment"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["mission_needs_finalized"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["request_received"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["request_success"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["request_robot_not_ready"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["fleet_fault"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["plc_comm_fault"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["control_healthy"], "Boolean", True)
    ensureMemoryTag(plcTagPaths["request_conflict"], "Boolean", False)
    ensureMemoryTag(plcTagPaths["request_invalid"], "Boolean", False)


def ensureMainControlRobotTags(robotName):
    """Create the robot-scoped derived tags consumed by MainControl and mirrored to PLC."""
    robotPath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
    ensureFolder(MAINCONTROL_ROBOTS_BASE)
    ensureFolder(robotPath)
    ensureMemoryTag(robotPath + "/MissionReadyforAttachment", "Boolean", False)
    ensureMemoryTag(robotPath + "/MissionIdForAttacment", "String", "")
    ensureMemoryTag(robotPath + "/MissionNameForAttachment", "String", "")


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


def readRuntimeState():
    """Read only the runtime fields needed for overlap protection."""
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
    """Write a partial set of runtime telemetry fields by logical name."""
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


def appendRuntimeDatasetRow(
    fieldName,
    headers,
    rowValues,
    maxRows=500
):
    """
    Append one row to a runtime history dataset and cap it to the most recent rows.

    Command-history dedupe is handled in robot internal state; this helper only appends.
    """
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
        rows = updated.to_rows()[-maxRows:]
        updated = system.dataset.toDataSet(headers, rows)

    writeTagValues([datasetPath], [updated])


def _toBool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ["true", "1", "yes", "on"]


def normalizeRobotState(rawState):
    """Normalize persisted tag values back into the controller's expected state shape."""
    rawState = dict(rawState or {})
    state = defaultRobotState()
    state["force_robot_ready"] = _toBool(rawState.get("force_robot_ready"))
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
    state["last_logged_signature"] = str(rawState.get("last_logged_signature") or "")
    state["last_computed_log_signature"] = str(rawState.get("last_computed_log_signature") or "")
    state["last_log_decision"] = str(rawState.get("last_log_decision") or "")
    return state


def buildCommandLogSignature(
    robotName,
    requestedWorkflowNumber,
    activeWorkflowNumber,
    action,
    level,
    stateName,
    message
):
    """
    Build the per-robot signature used to suppress duplicate command-history rows.

    This intentionally tracks the controller decision shape, not the full log message,
    so scan-to-scan rewrites of the same state do not keep creating new history rows.
    """
    return "|".join([
        str(robotName or ""),
        str(normalizeWorkflowNumber(requestedWorkflowNumber) or 0),
        str(normalizeWorkflowNumber(activeWorkflowNumber) or 0),
        str(action or ""),
        str(level or ""),
        str(stateName or ""),
    ])


def readRobotState(robotName):
    """Read one robot's internal state from tags."""
    paths = internalStatePaths(robotName)
    values = readTagValues([
        paths["force_robot_ready"],
        paths["request_latched"],
        paths["selected_workflow_number"],
        paths["state"],
        paths["mission_created"],
        paths["mission_needs_finalized"],
        paths["last_command_ts"],
        paths["last_result"],
        paths["last_command_id"],
        paths["last_logged_signature"],
        paths["last_computed_log_signature"],
        paths["last_log_decision"],
    ])

    rawState = {}
    keys = [
        "force_robot_ready",
        "request_latched",
        "selected_workflow_number",
        "state",
        "mission_created",
        "mission_needs_finalized",
        "last_command_ts",
        "last_result",
        "last_command_id",
        "last_logged_signature",
        "last_computed_log_signature",
        "last_log_decision",
    ]
    for key, qualifiedValue in zip(keys, values):
        rawState[key] = qualifiedValue.value if qualifiedValue.quality.isGood() else None
    return normalizeRobotState(rawState)


def writeRobotState(robotName, state):
    """Persist robot state while preserving fields not being updated in this cycle."""
    mergedState = dict(readRobotState(robotName) or {})
    mergedState.update(dict(state or {}))
    state = normalizeRobotState(mergedState)
    paths = internalStatePaths(robotName)
    writeTagValues(
        [
            paths["force_robot_ready"],
            paths["request_latched"],
            paths["selected_workflow_number"],
            paths["state"],
            paths["mission_created"],
            paths["mission_needs_finalized"],
            paths["last_command_ts"],
            paths["last_result"],
            paths["last_command_id"],
            paths["last_logged_signature"],
            paths["last_computed_log_signature"],
            paths["last_log_decision"],
        ],
        [
            state["force_robot_ready"],
            state["request_latched"],
            state["selected_workflow_number"],
            state["state"],
            state["mission_created"],
            state["mission_needs_finalized"],
            state["last_command_ts"],
            state["last_result"],
            state["last_command_id"],
            state["last_logged_signature"],
            state["last_computed_log_signature"],
            state["last_log_decision"],
        ]
    )


def readPlcInputs(robotName):
    """
    Read PLC demand and handshake inputs.

    RequestedWorkflowNumber is the real request signal. RequestActive is kept as a
    compatibility input while the PLC side settles into the workflow-number model.
    """
    paths = plcPaths(robotName)
    readResults = readTagValues([
        paths["request_active"],
        paths["requested_workflow_number"],
        paths["finalize_ok"],
    ])
    qualities = [qualifiedValue.quality.isGood() for qualifiedValue in readResults]
    requestedWorkflowNumber = normalizeWorkflowNumber(
        readResults[1].value if qualities[1] else 0
    ) or 0
    compatibilityRequestActive = _toBool(
        readResults[0].value if qualities[0] else False
    )
    return {
        "request_active": bool(requestedWorkflowNumber) or compatibilityRequestActive,
        "requested_workflow_number": requestedWorkflowNumber,
        "finalize_ok": _toBool(readResults[2].value if qualities[2] else False),
        "healthy": all(qualities),
        "fault_reason": "" if all(qualities) else "plc_input_quality_bad",
    }


def writePlcOutputs(robotName, outputs):
    """Write the PLC-facing status bits that summarize the runner's decision for this cycle."""
    paths = plcPaths(robotName)
    outputs = dict(outputs or {})
    writeTagValues(
        [
            paths["available_for_work"],
            paths["active_workflow_number"],
            paths["mission_ready_for_attachment"],
            paths["mission_needs_finalized"],
            paths["request_received"],
            paths["request_success"],
            paths["request_robot_not_ready"],
            paths["fleet_fault"],
            paths["plc_comm_fault"],
            paths["control_healthy"],
            paths["request_conflict"],
            paths["request_invalid"],
        ],
        [
            _toBool(outputs.get("available_for_work")),
            normalizeWorkflowNumber(outputs.get("active_workflow_number")) or 0,
            _toBool(outputs.get("mission_ready_for_attachment")),
            _toBool(outputs.get("mission_needs_finalized")),
            _toBool(outputs.get("request_received")),
            _toBool(outputs.get("request_success")),
            _toBool(outputs.get("request_robot_not_ready")),
            _toBool(outputs.get("fleet_fault")),
            _toBool(outputs.get("plc_comm_fault")),
            _toBool(outputs.get("control_healthy", True)),
            _toBool(outputs.get("request_conflict")),
            _toBool(outputs.get("request_invalid")),
        ]
    )


def writePlcHealthOutputs(robotName, fleetFault=False, plcCommFault=False, controlHealthy=True):
    """Update only the health subset when the loop skips normal PLC evaluation."""
    paths = plcPaths(robotName)
    writeTagValues(
        [
            paths["fleet_fault"],
            paths["plc_comm_fault"],
            paths["control_healthy"],
        ],
        [
            _toBool(fleetFault),
            _toBool(plcCommFault),
            _toBool(controlHealthy),
        ]
    )


def readRobotMirrorInputs(robotName):
    """Read the fleet/main-control signals that feed PLC output mirroring and dispatch gating."""
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
    """Extract the requested workflow number from the active mission naming convention."""
    text = str(missionName or "").strip()
    if not text:
        return None

    match = WORKFLOW_NAME_RE.match(text)
    if not match:
        return None

    return normalizeWorkflowNumber(match.group(1))


def readActiveMissionSummary(robotName):
    """Summarize the single active mission view that MainController cares about for one robot."""
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
    """
    Build a workflow ownership map from both active missions and controller state.

    This lets the runner avoid double-assigning exclusive workflows even while a
    mission is still being created, finalized, or canceled.
    """
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
    """Wrap one robot-cycle decision in a consistent result payload."""
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
