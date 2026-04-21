from Otto_API.Common.RuntimeHistory import ensureRuntimeTags as ensureSharedRuntimeTags
from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureUdtInstancePath

from MainController.State.Paths import MAINCONTROL_ROBOTS_BASE
from MainController.State.Paths import PLC_BASE
from MainController.State.Paths import internalStatePaths
from MainController.State.Paths import plcPaths


def ensureRobotRunnerTags(robotName):
    """Provision the per-robot controller state and PLC interface UDT instances on demand."""
    internalPaths = internalStatePaths(robotName)
    plcTagPaths = plcPaths(robotName)

    ensureMainControlRobotTags(robotName)
    ensureFolder(PLC_BASE)
    ensureUdtInstancePath(internalPaths["base"], "MainControl_Robot")
    ensureUdtInstancePath(plcTagPaths["base"], "PLC_RobotInterface")


def ensureMainControlRobotTags(robotName):
    """Ensure the single robot-scoped MainControl UDT instance exists."""
    robotPath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
    ensureFolder(MAINCONTROL_ROBOTS_BASE)
    ensureUdtInstancePath(robotPath, "MainControl_Robot")


def ensureRuntimeTags():
    """Create runtime telemetry and history dataset tags for the main loop."""
    ensureSharedRuntimeTags()
