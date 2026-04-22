from Otto_API.AttachmentPhaseHelpers import buildMissionControlFlags
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Missions.MissionActions import selectCurrentActiveMissionRecord
from Otto_API.Missions.MissionActions import sortActiveMissionRecords
from Otto_API.Missions.MissionTreeHelpers import browseMissionInstances

from MainController.State.Coerce import toBool
from MainController.State.Paths import FLEET_ROBOTS_BASE
from MainController.State.Paths import MAINCONTROL_ROBOTS_BASE
from MainController.State.Paths import MISSIONS_ACTIVE_BASE
from MainController.State.Paths import WORKFLOW_NAME_RE
from MainController.State.RobotStore import readRobotState
from MainController.WorkflowConfig import ROBOT_NAMES
from MainController.WorkflowConfig import normalizeWorkflowNumber


def readRobotMirrorInputs(robotName):
    """Read the fleet/main-control signals that feed PLC output mirroring and dispatch gating."""
    robotPath = FLEET_ROBOTS_BASE + "/" + robotName
    mainControlRobotPath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
    missionStarved = toBool(
        readOptionalTagValue(mainControlRobotPath + "/MissionStarved", False)
    )
    missionControlFlags = buildMissionControlFlags(missionStarved)
    return {
        "available_for_work": toBool(readOptionalTagValue(robotPath + "/AvailableForWork", False)),
        "active_mission_count": int(readOptionalTagValue(robotPath + "/ActiveMissionCount", 0) or 0),
        "charge_level": float(readOptionalTagValue(robotPath + "/ChargeLevel", 0.0) or 0.0),
        "system_state": str(readOptionalTagValue(robotPath + "/SystemState", "", allowEmptyString=True) or ""),
        "sub_system_state": str(readOptionalTagValue(robotPath + "/SubSystemState", "", allowEmptyString=True) or ""),
        "activity_state": str(readOptionalTagValue(robotPath + "/ActivityState", "", allowEmptyString=True) or ""),
        "mission_starved": missionControlFlags["mission_starved"],
        "mission_ready_for_attachment": missionControlFlags["ready_for_attachment"],
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
    """Summarize the robot's active mission set and choose one current mission deterministically."""
    rootPath = MISSIONS_ACTIVE_BASE + "/" + robotName
    missionInstances = browseMissionInstances(rootPath)
    if not missionInstances:
        return {
            "count": 0,
            "missions": [],
            "current_mission": None,
            "current_mission_status": "",
            "current_mission_id": "",
            "current_mission_path": "",
            "mission_name": "",
            "workflow_number": None,
        }

    readPaths = []
    for fullPath, _instanceName in missionInstances:
        readPaths.extend([
            fullPath + "/Name",
            fullPath + "/name",
            fullPath + "/Mission_Status",
            fullPath + "/mission_status",
            fullPath + "/ID",
            fullPath + "/id",
        ])
    readResults = readTagValues(readPaths)

    missions = []
    for index, missionRow in enumerate(missionInstances):
        fullPath = missionRow[0]
        offset = index * 6
        nameValue = None
        statusValue = None
        missionIdValue = None

        for candidate in [readResults[offset], readResults[offset + 1]]:
            if candidate.quality.isGood() and candidate.value is not None and str(candidate.value).strip():
                nameValue = str(candidate.value).strip()
                break
        for candidate in [readResults[offset + 2], readResults[offset + 3]]:
            if candidate.quality.isGood() and candidate.value is not None and str(candidate.value).strip():
                statusValue = str(candidate.value).strip().upper()
                break
        for candidate in [readResults[offset + 4], readResults[offset + 5]]:
            if candidate.quality.isGood() and candidate.value is not None and str(candidate.value).strip():
                missionIdValue = str(candidate.value).strip()
                break

        missions.append({
            "instance_path": fullPath,
            "path": fullPath,
            "mission_name": nameValue or "",
            "name": nameValue or "",
            "mission_status": statusValue or "",
            "workflow_number": parseActiveWorkflowNumberFromMissionName(nameValue),
            "id": missionIdValue or "",
        })

    missions = sortActiveMissionRecords(missions)
    currentMission = selectCurrentActiveMissionRecord(missions)
    missionName = ""
    workflowNumber = None
    currentStatus = ""
    currentMissionId = ""
    currentMissionPath = ""
    if currentMission:
        missionName = str(currentMission.get("mission_name") or currentMission.get("name") or "")
        workflowNumber = currentMission.get("workflow_number")
        currentStatus = str(currentMission.get("mission_status") or "")
        currentMissionId = str(currentMission.get("id") or "")
        currentMissionPath = str(currentMission.get("instance_path") or currentMission.get("path") or "")

    return {
        "count": len(missionInstances),
        "missions": missions,
        "current_mission": currentMission,
        "current_mission_status": currentStatus,
        "current_mission_id": currentMissionId,
        "current_mission_path": currentMissionPath,
        "mission_name": missionName,
        "workflow_number": workflowNumber,
    }


def buildWorkflowReservedMap(robotNames):
    """Build a workflow ownership map from both active missions and controller state."""
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
