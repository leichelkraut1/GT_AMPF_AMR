from Otto_API.Services.Missions import Operations as MissionOperations

from Otto_API.TagSync.WorkflowConfig import buildMissionName
from Otto_API.TagSync.WorkflowConfig import robotIdTagPath
from Otto_API.TagSync.WorkflowConfig import workflowTemplateTagPath


def callCreateMission(robotName, workflowNumber, createMission=None):
    """Create a mission for the selected workflow using the configured OTTO template."""
    if createMission is None:
        createMission = MissionOperations.createMission

    templateTagPath = workflowTemplateTagPath(workflowNumber)
    missionName = buildMissionName(workflowNumber, robotName)
    if not templateTagPath or not missionName:
        return {
            "ok": False,
            "level": "error",
            "message": "Workflow [{}] is not configured for robot [{}]".format(workflowNumber, robotName),
        }

    return createMission(
        templateTagPath=templateTagPath,
        robotTagPath=robotIdTagPath(robotName),
        missionName=missionName,
    )


def callFinalizeMissionId(missionId, finalizeMissionId=None):
    """Finalize one explicit mission id."""
    if finalizeMissionId is None:
        finalizeMissionId = MissionOperations.finalizeMissionId
    return finalizeMissionId(missionId)


def callCancelMissionIds(missionIds, cancelMissionIds=None):
    """Cancel an explicit list of mission ids."""
    if cancelMissionIds is None:
        cancelMissionIds = MissionOperations.cancelMissionIds
    return cancelMissionIds(missionIds)


def callMissionCommand(actionName, missionId, finalizeMissionId=None, cancelMissionIds=None):
    """Dispatch one explicit mission command through the mission-id OTTO wrappers."""
    actionName = str(actionName or "")
    if actionName == "finalize_mission":
        return callFinalizeMissionId(missionId, finalizeMissionId=finalizeMissionId)
    if actionName == "cancel_mission":
        return callCancelMissionIds([missionId], cancelMissionIds=cancelMissionIds)
    return {
        "ok": False,
        "level": "error",
        "message": "Unsupported mission command [{}]".format(actionName),
    }
