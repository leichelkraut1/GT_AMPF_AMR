import re

from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.TagPaths import getMainControlRobotsPath
from Otto_API.Common.TagPaths import getPlcRootPath
from Otto_API.Common.RuntimeHistory import RUNTIME_BASE

from Otto_API.Models.Fleet import ROBOT_NAMES


FLEET_ROBOTS_BASE = getFleetRobotsPath()
MAINCONTROL_ROBOTS_BASE = getMainControlRobotsPath()
MISSIONS_ACTIVE_BASE = getFleetMissionsPath() + "/Active"
PLC_BASE = getPlcRootPath()
PLC_ROBOTS_BASE = PLC_BASE + "/Robots"
PLC_PLACES_BASE = PLC_BASE + "/Places"
PLC_FLEET_MAPPING_BASE = PLC_BASE + "/FleetMapping"

WORKFLOW_NAME_RE = re.compile(r"^WF(\d+)_")
RETRY_DELAY_MS = 5000


def internalStatePaths(robotName):
    """Centralize MainControl/Robots paths so controller state and mission-derived flags live together."""
    basePath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
    return {
        "base": basePath,
        "force_robot_ready": basePath + "/ForceRobotReady",
        "disable_ignition_control": basePath + "/DisableIgnitionControl",
        "request_latched": basePath + "/RequestLatched",
        "selected_workflow_number": basePath + "/SelectedWorkflowNumber",
        "state": basePath + "/State",
        "mission_created": basePath + "/MissionCreated",
        "mission_needs_finalized": basePath + "/MissionNeedsFinalized",
        "pending_create_start_epoch_ms": basePath + "/PendingCreateStartEpochMs",
        "last_command_ts": basePath + "/LastCommandTs",
        "last_result": basePath + "/LastResult",
        "last_command_id": basePath + "/LastCommandId",
        "next_action_allowed_epoch_ms": basePath + "/NextActionAllowedEpochMs",
        "last_attempt_action": basePath + "/LastAttemptAction",
        "retry_count": basePath + "/RetryCount",
        "last_logged_signature": basePath + "/LastLoggedSignature",
        "last_computed_log_signature": basePath + "/LastComputedLogSignature",
        "last_log_decision": basePath + "/LastLogDecision",
    }


def plcRobotRowPath(plcTagName):
    return PLC_ROBOTS_BASE + "/" + str(plcTagName or "")


def plcRobotPaths(plcTagName):
    """
    Return the PLC-facing input/output contract for a mapped PLC robot row.

    The returned dict uses stable snake_case path keys such as "activity_state".
    Other modules, especially MainController.State.PlcStore, map logical output keys onto these
    path keys first; this helper is where those keys become concrete PLC tag paths such as
    "/ToPLC/ActivityState".
    """
    basePath = plcRobotRowPath(plcTagName)
    fromPlc = basePath + "/FromPLC"
    toPlc = basePath + "/ToPLC"
    return {
        "base": basePath,
        "from_plc": fromPlc,
        "to_plc": toPlc,
        "requested_workflow_number": fromPlc + "/RequestedWorkflowNumber",
        "finalize_ok": fromPlc + "/FinalizeOk",
        "available_for_work": toPlc + "/AvailableForWork",
        "active_mission_count": toPlc + "/ActiveMissionCount",
        "charge_level": toPlc + "/ChargeLevel",
        "system_state": toPlc + "/SystemState",
        "sub_system_state": toPlc + "/SubSystemState",
        "activity_state": toPlc + "/ActivityState",
        "place_id": toPlc + "/PlaceId",
        "place_name": toPlc + "/PlaceName",
        "container_present": toPlc + "/ContainerPresent",
        "container_id": toPlc + "/ContainerId",
        "active_workflow_number": toPlc + "/ActiveWorkflowNumber",
        "mission_starved": toPlc + "/MissionStarved",
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


def plcPaths(plcTagName):
    """Legacy alias that now expects an already-resolved PLC robot tag name."""
    return plcRobotPaths(plcTagName)


def plcPlacesPath():
    """Return the PLC folder that holds mapped place sync rows."""
    return PLC_PLACES_BASE


def plcPlaceRowPath(plcTagName):
    return PLC_PLACES_BASE + "/" + str(plcTagName or "")


def plcFleetMappingPath():
    return PLC_FLEET_MAPPING_BASE


def plcRobotTagNameMappingPath():
    return PLC_FLEET_MAPPING_BASE + "/RobotTagNameMapping"


def plcPlaceTagNameMappingPath():
    return PLC_FLEET_MAPPING_BASE + "/PlaceTagNameMapping"
