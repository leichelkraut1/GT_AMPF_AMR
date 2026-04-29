from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readTagValues

from MainController.State.PlcMappingStore import readPlcMappings


RUNTIME_PHASE_ROWS = [
    (
        "Fleet Server API",
        "[Otto_FleetManager]MainControl/Runtime/ServerStatusStatus",
        "[Otto_FleetManager]MainControl/Runtime/ServerStatusMessage",
    ),
    (
        "Robot Sync",
        "[Otto_FleetManager]MainControl/Runtime/RobotStateStatus",
        "[Otto_FleetManager]MainControl/Runtime/RobotStateMessage",
    ),
    (
        "Container Sync",
        "[Otto_FleetManager]MainControl/Runtime/ContainerStateStatus",
        "[Otto_FleetManager]MainControl/Runtime/ContainerStateMessage",
    ),
    (
        "Interlock Sync",
        "[Otto_FleetManager]MainControl/Runtime/InterlockSyncStatus",
        "[Otto_FleetManager]MainControl/Runtime/InterlockSyncMessage",
    ),
    (
        "PLC Place Fleet Sync",
        "[Otto_FleetManager]MainControl/Runtime/PLCPlaceFleetSyncStatus",
        "[Otto_FleetManager]MainControl/Runtime/PLCPlaceFleetSyncMessage",
    ),
    (
        "PLC Robot Fleet Sync",
        "[Otto_FleetManager]MainControl/Runtime/PLCRobotFleetSyncStatus",
        "[Otto_FleetManager]MainControl/Runtime/PLCRobotFleetSyncMessage",
    ),
    (
        "Mission Sorting",
        "[Otto_FleetManager]MainControl/Runtime/MissionSortingStatus",
        "[Otto_FleetManager]MainControl/Runtime/MissionSortingMessage",
    ),
    (
        "Workflow Cycles",
        "[Otto_FleetManager]MainControl/Runtime/WorkflowCyclesStatus",
        "[Otto_FleetManager]MainControl/Runtime/WorkflowCyclesMessage",
    ),
]


def _healthColor(status):
    if status == "Error":
        return "#991b1b"
    if status == "Warn":
        return "#b45309"
    return "#166534"


def _phaseMessageForStatus(status, message):
    if message:
        return message
    if status == "Healthy":
        return "Healthy"
    if status == "Warn":
        return "Needs attention"
    if status == "Error":
        return "Faulted"
    return "No message"


def _loadFleetStatusModel(robotName="AMPF_AMR_RV1"):
    """Load one shared model for FleetStatus cards and tables."""
    robotName = normalizeTagValue(robotName) or "AMPF_AMR_RV1"
    mappingState = readPlcMappings()
    plcTagName = normalizeTagValue(
        (mappingState.get("robot_name_to_plc_tag") or {}).get(robotName)
    )

    paths = []
    for _, statusPath, messagePath in RUNTIME_PHASE_ROWS:
        paths.extend([statusPath, messagePath])
    paths.extend([
        "[Otto_FleetManager]MainControl/Runtime/LoopIsRunning",
        "[Otto_FleetManager]MainControl/Runtime/LoopLastResult",
    ])
    if plcTagName:
        paths.append("[Otto_FleetManager]PLC/Robots/{}/ToPLC/ControlHealthy".format(plcTagName))

    results = readTagValues(paths)
    phaseResults = results[:len(RUNTIME_PHASE_ROWS) * 2]
    loopRunningResult = results[len(RUNTIME_PHASE_ROWS) * 2]
    loopLastResult = results[len(RUNTIME_PHASE_ROWS) * 2 + 1]
    plcHealthyResult = results[len(RUNTIME_PHASE_ROWS) * 2 + 2] if plcTagName else None

    phaseRows = []
    statuses = []
    for index, (label, _, _) in enumerate(RUNTIME_PHASE_ROWS):
        statusResult = phaseResults[index * 2]
        messageResult = phaseResults[index * 2 + 1]
        status = normalizeTagValue(statusResult.value) if statusResult.quality.isGood() else "Unknown"
        message = normalizeTagValue(messageResult.value) if messageResult.quality.isGood() else ""
        status = status or "Unknown"
        phaseRows.append({
            "Subsystem": label,
            "Status": status,
            "Message": _phaseMessageForStatus(status, message),
        })
        statuses.append(status)

    plcHealthy = bool(plcHealthyResult and plcHealthyResult.quality.isGood() and bool(plcHealthyResult.value))
    loopRunning = loopRunningResult.quality.isGood() and bool(loopRunningResult.value)
    loopMessage = normalizeTagValue(loopLastResult.value) if loopLastResult.quality.isGood() else ""
    if loopMessage and "error" in loopMessage.lower():
        loopStatus = "Error"
    elif loopRunning:
        loopStatus = "Healthy"
    else:
        loopStatus = "Idle"
    if not loopMessage:
        loopMessage = "Loop currently running" if loopRunning else "Loop idle"

    return {
        "robot_name": robotName,
        "mapping_state": mappingState,
        "plc_tag_name": plcTagName,
        "phase_rows": phaseRows,
        "statuses": statuses,
        "plc_healthy": plcHealthy,
        "loop_row": {
            "Subsystem": "Main Loop",
            "Status": loopStatus,
            "Message": loopMessage,
        },
    }


def mainPlcCommsDisplay(robotName="AMPF_AMR_RV1"):
    """Return the FleetStatus text/color pair for the mapped main PLC comms card."""
    healthy = bool(_loadFleetStatusModel(robotName).get("plc_healthy"))
    return {
        "healthy": healthy,
        "text": "Healthy" if healthy else "Not Healthy",
        "color": "#166534" if healthy else "#991b1b",
    }


def controllerHealthDisplay():
    """Summarize the runtime phase-status tags for the FleetStatus controller-health card."""
    model = _loadFleetStatusModel()
    statuses = list(model.get("statuses") or [])
    if not bool(model.get("plc_healthy")):
        statuses.append("Error")

    loopStatus = str(dict(model.get("loop_row") or {}).get("Status") or "")
    if loopStatus == "Error":
        statuses.append("Error")
    elif loopStatus == "Warn":
        statuses.append("Warn")

    displayStatus = "Healthy"
    if any(status == "Error" for status in statuses):
        displayStatus = "Error"
    elif any(status == "Warn" for status in statuses):
        displayStatus = "Warn"

    return {
        "status": displayStatus,
        "text": "Attention" if displayStatus == "Error" else ("Degraded" if displayStatus == "Warn" else "Healthy"),
        "color": _healthColor(displayStatus),
    }


def phaseHealthRows(robotName="AMPF_AMR_RV1"):
    """Build the FleetStatus phase-health table rows from runtime tags plus mapped PLC comms."""
    model = _loadFleetStatusModel(robotName)
    rows = list(model.get("phase_rows") or [])
    plcHealthy = bool(model.get("plc_healthy"))
    rows.append({
        "Subsystem": "Main PLC Comms",
        "Status": "Healthy" if plcHealthy else "Error",
        "Message": (
            "RV1 mapped ControlHealthy is true"
            if plcHealthy
            else "RV1 mapped ControlHealthy is false, missing, or bad quality"
        ),
    })
    rows.append(dict(model.get("loop_row") or {}))
    return rows


def phaseHealthText(robotName="AMPF_AMR_RV1"):
    """Build a copy-friendly plain-text summary of subsystem health rows."""
    rows = list(phaseHealthRows(robotName) or [])
    if not rows:
        return ""
    return "\n".join([
        "{subsystem}\t{status}\t{message}".format(
            subsystem=str(row.get("Subsystem") or ""),
            status=str(row.get("Status") or ""),
            message=str(row.get("Message") or ""),
        )
        for row in rows
    ])
