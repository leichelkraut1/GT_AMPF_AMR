from Otto_API.Common.HttpLogPolicy import ensureHttpLogConfigTags
from Otto_API.Common.RuntimeHistory import ensureRuntimeTags as ensureSharedRuntimeTags
from Otto_API.Common.TagProvisioning import ensureBaseFleetConfigTags
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.TagSync.WorkflowConfig import ensureWorkflowConfigTag

from MainController.State.Paths import MAINCONTROL_ROBOTS_BASE
from MainController.State.Paths import PLC_BASE
from MainController.State.Paths import internalStatePaths
from MainController.State.Paths import plcPlacesPath
from MainController.State.Paths import ROBOT_NAMES


def ensureRobotRunnerTags(robotName):
    """Provision only the robot-scoped controller tags; PLC/Fleet sync rows are synced separately."""
    internalPaths = internalStatePaths(robotName)

    ensureMainControlRobotTags(robotName)
    ensureFolder(PLC_BASE)
    ensureUdtInstancePath(internalPaths["base"], "MainControl_Robot")


def ensureAllRobotRunnerTags(robotNames=None):
    """Provision every configured MainControl robot runner row explicitly."""
    robotNames = list(robotNames or ROBOT_NAMES or [])
    for robotName in robotNames:
        ensureRobotRunnerTags(robotName)


def ensureMainControlRobotTags(robotName):
    """Ensure the single robot-scoped MainControl UDT instance exists."""
    robotPath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
    ensureFolder(MAINCONTROL_ROBOTS_BASE)
    ensureUdtInstancePath(robotPath, "MainControl_Robot")


def ensureRuntimeTags():
    """Create runtime telemetry and history dataset tags for the main loop."""
    from MainController.State.PlcMappingStore import ensurePlcMappingTags

    ensureSharedRuntimeTags()
    ensureFleetConfigTags()
    ensurePlcMappingTags()
    ensureAllRobotRunnerTags()
    ensureFolder(plcPlacesPath())


def ensureFleetConfigTags():
    """Provision the Fleet config surface explicitly from OTTO-owned defaults."""
    ensureBaseFleetConfigTags()
    ensureHttpLogConfigTags()
    ensureWorkflowConfigTag()
