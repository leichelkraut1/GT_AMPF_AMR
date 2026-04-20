from Otto_API.Missions import Post

from MainController.WorkflowConfig import buildMissionName
from MainController.WorkflowConfig import robotIdTagPath
from MainController.WorkflowConfig import workflowTemplateTagPath


def callCreateMission(robotName, workflowNumber, createMission=None):
    """Create a mission for the selected workflow using the configured OTTO template."""
    if createMission is None:
        createMission = Post.createMission

    templateTagPath = workflowTemplateTagPath(workflowNumber)
    missionName = buildMissionName(workflowNumber, robotName)
    return createMission(
        templateTagPath=templateTagPath,
        robotTagPath=robotIdTagPath(robotName),
        missionName=missionName,
    )


def callFinalizeMission(robotName, finalizeMission=None):
    """Finalize the active mission for a robot."""
    if finalizeMission is None:
        finalizeMission = Post.finalizeMission
    return finalizeMission(robotName)


def callCancelMission(robotName, cancelMission=None):
    """Cancel the active mission for a robot."""
    if cancelMission is None:
        cancelMission = Post.cancelMission
    return cancelMission(robotName)
