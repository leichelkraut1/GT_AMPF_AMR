import re

from Otto_API.Common.TagHelpers import getFleetMissionsPath
from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getMainControlRobotsPath
from Otto_API.Common.TagHelpers import getMainControlRuntimePath
from Otto_API.Common.TagHelpers import getPlcRootPath

from MainController.WorkflowConfig import ROBOT_NAMES


FLEET_ROBOTS_BASE = getFleetRobotsPath()
MAINCONTROL_ROBOTS_BASE = getMainControlRobotsPath()
MISSIONS_ACTIVE_BASE = getFleetMissionsPath() + "/Active"
PLC_BASE = getPlcRootPath()
PLC_CONTAINERS_BASE = PLC_BASE + "/Containers"
RUNTIME_BASE = getMainControlRuntimePath()

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


def plcPaths(robotName):
    """Return the PLC-facing input/output contract for a robot."""
    basePath = PLC_BASE + "/" + robotName
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


def plcContainersPath():
    """Return the PLC folder that holds manual container-location mirror rows."""
    return PLC_CONTAINERS_BASE


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
        "robot_state_history": RUNTIME_BASE + "/RobotStateHistory",
        "http_history": RUNTIME_BASE + "/HttpHistory",
        "http_get_history": RUNTIME_BASE + "/HttpGetHistory",
        "http_post_history": RUNTIME_BASE + "/HttpPostHistory",
    }
