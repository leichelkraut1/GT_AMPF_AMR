from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readOptionalTagValues
from Otto_API.Common.TagPaths import getFleetContainersPath
from Otto_API.Common.TagPaths import getFleetPlacesPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.TagPaths import getFleetSystemPath
from Otto_API.Common.TagPaths import getMainControlRobotsPath

from MainController.State.FleetStatusStore import controllerHealthDisplay
from MainController.State.FleetStatusStore import mainPlcCommsDisplay
from MainController.State.FleetStatusStore import phaseHealthRows
from MainController.State.Paths import plcRobotPaths
from MainController.State.Paths import runtimePaths
from MainController.State.PlcMappingStore import readPlcMappings
from MainController.WorkflowConfig import ROBOT_NAMES


def _statusTone(status):
    status = normalizeTagValue(status)
    if status == "Error":
        return "Error"
    if status == "Warn":
        return "Warn"
    if status == "Healthy":
        return "Healthy"
    if status == "Idle":
        return "Idle"
    return "Info"


def _rowValue(row, key, default=None):
    if isinstance(row, dict):
        return row.get(key, default)
    getter = getattr(row, "get", None)
    if getter is not None:
        try:
            return getter(key)
        except Exception:
            pass
    return getattr(row, key, default)


def _isUdtInstance(row):
    tagType = str(_rowValue(row, "tagType", "") or "").lower()
    return "udtinstance" in tagType


def _fullPath(row):
    return str(_rowValue(row, "fullPath", "") or "")


def _normalizeCount(value):
    try:
        return int(value)
    except Exception:
        return 0


def _normalizeRequestedWorkflow(value):
    try:
        number = int(value)
    except Exception:
        return None
    if number <= 0:
        return None
    return number


def _shortRobotName(robotName):
    robotName = normalizeTagValue(robotName)
    if robotName.startswith("AMPF_AMR_"):
        return robotName.replace("AMPF_AMR_", "", 1)
    return robotName or "-"


def notReadyReasonLookupDataset():
    return system.dataset.toDataSet(
        ["ReasonCode", "DisplayText"],
        [
            ["invalid_robot_id", "Robot ID is missing or invalid in Fleet data"],
            ["min_charge_missing", "Minimum charge setting is missing"],
            ["system_state_missing", "System state is unavailable"],
            ["activity_state_missing", "Activity state is unavailable"],
            ["charge_level_missing", "Charge level is unavailable"],
            ["mission_data_not_successful", "Mission data is stale or not successful"],
            ["mission_data_missing_timestamp", "Mission data timestamp is missing"],
            ["system_state_not_run", "Robot system is not in RUN"],
            ["activity_state_not_allowed", "Robot activity is not in an allowed idle state"],
            ["charge_below_minimum", "Battery is below the minimum charge threshold"],
            ["recently_charging", "Robot is still in charging cooldown"],
            ["active_mission_count_missing", "Active mission count is unavailable"],
            ["active_missions_present", "Robot still has an active mission assigned"],
            ["failed_mission_count_missing", "Failed mission count is unavailable"],
            ["failed_missions_present", "Robot has failed missions that need attention"],
        ],
    )


def summaryCards():
    runtime = runtimePaths()
    paths = [
        getFleetSystemPath() + "/ServerStatus",
        runtime["server_status_status"],
        runtime["server_status_message"],
        runtime["controller_fault_summary"],
        runtime["loop_is_running"],
        runtime["loop_last_result"],
        runtime["loop_last_duration_ms"],
        runtime["interlock_sync_status"],
        runtime["interlock_sync_message"],
    ]
    (
        serverStatus,
        serverRuntimeStatus,
        serverRuntimeMessage,
        controllerFaultSummary,
        loopIsRunning,
        loopLastResult,
        loopLastDurationMs,
        interlockStatus,
        interlockMessage,
    ) = readOptionalTagValues(
        paths,
        defaultValues=["", "Unknown", "", "", False, "", None, "Unknown", ""],
        allowEmptyString=True,
    )

    loopResultText = str(loopLastResult or "")
    if loopResultText and "error" in loopResultText.lower():
        loopValue = "Errored"
        loopTone = "Error"
    elif bool(loopIsRunning):
        loopValue = "Running"
        loopTone = "Healthy"
    else:
        loopValue = "Idle"
        loopTone = "Idle"

    loopDetail = ""
    if loopLastDurationMs is not None:
        loopDetail = "{} ms".format(str(loopLastDurationMs))
    elif loopResultText:
        loopDetail = loopResultText

    controllerDisplay = controllerHealthDisplay()
    plcDisplay = mainPlcCommsDisplay()

    return [
        {
            "title": "Fleet Server",
            "value": normalizeTagValue(serverStatus) or "-",
            "detail": normalizeTagValue(serverRuntimeMessage) or "Current Fleet server state",
            "tone": _statusTone(serverRuntimeStatus),
        },
        {
            "title": "Main PLC Comms",
            "value": str(plcDisplay.get("text") or "-"),
            "detail": "Mapped PLC control health",
            "tone": "Healthy" if bool(plcDisplay.get("healthy")) else "Error",
        },
        {
            "title": "Controller Health",
            "value": str(controllerDisplay.get("text") or "-"),
            "detail": normalizeTagValue(controllerFaultSummary) or "All tracked controller phases are healthy.",
            "tone": _statusTone(controllerDisplay.get("status")),
        },
        {
            "title": "Main Loop",
            "value": loopValue,
            "detail": loopDetail or "No recent loop detail",
            "tone": loopTone,
        },
        {
            "title": "Interlock Sync",
            "value": normalizeTagValue(interlockStatus) or "-",
            "detail": normalizeTagValue(interlockMessage) or "No interlock sync message",
            "tone": _statusTone(interlockStatus),
        },
    ]


def subsystemHealthCards():
    cards = []
    for row in list(phaseHealthRows() or []):
        subsystem = str(row.get("Subsystem") or "").strip()
        normalized = subsystem.lower()
        if normalized in {"main plc comms", "main loop"}:
            continue
        if "interlock" in normalized:
            continue
        cards.append({
            "subsystem": subsystem,
            "status": str(row.get("Status") or ""),
            "message": str(row.get("Message") or ""),
        })
    return cards


def robotCards():
    mappingState = dict(readPlcMappings() or {})
    robotNameToPlcTag = dict(mappingState.get("robot_name_to_plc_tag") or {})
    fleetRobotsBase = getFleetRobotsPath()
    mainControlRobotsBase = getMainControlRobotsPath()
    cards = []

    for robotName in list(ROBOT_NAMES or []):
        fleetBase = fleetRobotsBase + "/" + robotName
        mainBase = mainControlRobotsBase + "/" + robotName
        plcTagName = normalizeTagValue(robotNameToPlcTag.get(robotName))
        requestedWorkflowPath = None
        activeWorkflowPath = None
        finalizeOkPath = None
        if plcTagName:
            plcPaths = plcRobotPaths(plcTagName)
            requestedWorkflowPath = plcPaths.get("requested_workflow_number")
            activeWorkflowPath = plcPaths.get("active_workflow_number")
            finalizeOkPath = plcPaths.get("finalize_ok")

        paths = [
            fleetBase + "/AvailableForWork",
            fleetBase + "/NotReadyReason",
            fleetBase + "/SystemState",
            fleetBase + "/SubSystemState",
            fleetBase + "/ActivityState",
            fleetBase + "/ChargeLevel",
            fleetBase + "/PlaceName",
            fleetBase + "/ActiveMissionCount",
            mainBase + "/CurrentMissionName",
            mainBase + "/CurrentMissionStatus",
            mainBase + "/LastResult",
        ]
        defaults = [False, "", "", "", "", None, "", 0, "", "", ""]
        if requestedWorkflowPath:
            paths.append(requestedWorkflowPath)
            defaults.append(None)
        if activeWorkflowPath:
            paths.append(activeWorkflowPath)
            defaults.append(None)
        if finalizeOkPath:
            paths.append(finalizeOkPath)
            defaults.append(False)

        values = readOptionalTagValues(paths, defaultValues=defaults, allowEmptyString=True)
        availableForWork = bool(values[0])
        notReadyReason = normalizeTagValue(values[1])
        systemState = normalizeTagValue(values[2])
        subSystemState = normalizeTagValue(values[3])
        activityState = normalizeTagValue(values[4])
        chargeLevel = values[5]
        placeName = normalizeTagValue(values[6])
        activeMissionCount = _normalizeCount(values[7])
        currentMissionName = normalizeTagValue(values[8])
        currentMissionStatus = normalizeTagValue(values[9])
        missionControlStatus = normalizeTagValue(values[10])
        nextIndex = 11
        requestedWorkflowNumber = _normalizeRequestedWorkflow(values[nextIndex]) if len(values) > nextIndex else None
        nextIndex += 1
        activeWorkflowNumber = _normalizeRequestedWorkflow(values[nextIndex]) if len(values) > nextIndex else None
        nextIndex += 1
        readyToFinalize = bool(values[nextIndex]) if len(values) > nextIndex else False

        cards.append({
            "robotName": _shortRobotName(robotName),
            "availableForWork": availableForWork,
            "notReadyReason": notReadyReason,
            "systemState": systemState or "-",
            "subSystemState": subSystemState or "-",
            "activityState": activityState or "-",
            "chargeLevel": chargeLevel,
            "placeName": placeName or "-",
            "currentMissionName": currentMissionName or "-",
            "currentMissionStatus": currentMissionStatus or "-",
            "activeMissionCount": activeMissionCount,
            "requestedWorkflowNumber": requestedWorkflowNumber,
            "activeWorkflowNumber": activeWorkflowNumber,
            "missionControlStatus": missionControlStatus or "-",
            "readyToFinalize": readyToFinalize,
            "plcTagName": plcTagName,
        })

    return cards


def containerCards():
    placeRows = [row for row in browseTagResults(getFleetPlacesPath()) if _isUdtInstance(row)]
    robotRows = [row for row in browseTagResults(getFleetRobotsPath()) if _isUdtInstance(row)]
    containerRows = [row for row in browseTagResults(getFleetContainersPath()) if _isUdtInstance(row)]

    placePaths = []
    for row in placeRows:
        path = _fullPath(row)
        placePaths.extend([path + "/ID", path + "/Name"])
    placeValues = readOptionalTagValues(placePaths, defaultValues=[""] * len(placePaths), allowEmptyString=True)
    placeById = {}
    for index, _row in enumerate(placeRows):
        offset = index * 2
        placeId = normalizeTagValue(placeValues[offset]) if offset < len(placeValues) else ""
        placeName = normalizeTagValue(placeValues[offset + 1]) if offset + 1 < len(placeValues) else ""
        if placeId:
            placeById[placeId] = placeName or placeId

    robotPaths = []
    for row in robotRows:
        robotPaths.append(_fullPath(row) + "/ID")
    robotValues = readOptionalTagValues(robotPaths, defaultValues=[""] * len(robotPaths), allowEmptyString=True)
    robotById = {}
    for index, row in enumerate(robotRows):
        robotId = normalizeTagValue(robotValues[index]) if index < len(robotValues) else ""
        robotName = str(_rowValue(row, "name", "") or "")
        if robotId:
            robotById[robotId] = robotName or robotId

    containerPaths = []
    for row in containerRows:
        path = _fullPath(row)
        containerPaths.extend([
            path + "/ID",
            path + "/ContainerType",
            path + "/State",
            path + "/Place",
            path + "/Robot",
        ])
    containerValues = readOptionalTagValues(containerPaths, defaultValues=[""] * len(containerPaths), allowEmptyString=True)

    cards = []
    for index, _row in enumerate(containerRows):
        offset = index * 5
        containerId = normalizeTagValue(containerValues[offset]) if offset < len(containerValues) else ""
        containerType = normalizeTagValue(containerValues[offset + 1]) if offset + 1 < len(containerValues) else ""
        state = normalizeTagValue(containerValues[offset + 2]) if offset + 2 < len(containerValues) else ""
        placeId = normalizeTagValue(containerValues[offset + 3]) if offset + 3 < len(containerValues) else ""
        robotId = normalizeTagValue(containerValues[offset + 4]) if offset + 4 < len(containerValues) else ""

        if placeId and placeId in placeById:
            locationLabel = "Place: {}".format(placeById[placeId])
        elif placeId:
            locationLabel = "Place: {}".format(placeId)
        elif robotId and robotId in robotById:
            locationLabel = "Robot: {}".format(robotById[robotId])
        elif robotId:
            locationLabel = "Robot: {}".format(robotId)
        else:
            locationLabel = "Location: Unassigned"

        cards.append({
            "containerId": containerId or "-",
            "containerType": containerType or "-",
            "state": state or "-",
            "locationLabel": locationLabel,
        })

    cards.sort(key=lambda row: (str(row.get("containerType") or ""), str(row.get("containerId") or "")))
    return cards
