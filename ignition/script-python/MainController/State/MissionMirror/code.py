from Otto_API.Common.TagIO import writeRequiredTagValues

from MainController.State.Paths import MAINCONTROL_ROBOTS_BASE
from MainController.State.Paths import ROBOT_NAMES


MISSION_SORTING_ROBOT_FIELDS = [
    ("ActiveMissionCount", "active_mission_count", 0),
    ("FailedMissionCount", "failed_mission_count", 0),
    ("MissionStarved", "mission_starved", False),
    ("MissionReadyforAttachment", "mission_ready_for_attachment", False),
    ("CurrentMissionName", "current_mission_name", ""),
    ("CurrentMissionId", "current_mission_id", ""),
    ("CurrentMissionStatus", "current_mission_status", ""),
]


def writeMissionSortingRobotMirror(robotSummaryByFolder, robotNames=None):
    """Mirror mission-sorting robot summary data onto MainControl/Robots tags."""
    if robotNames is None:
        robotNames = ROBOT_NAMES

    robotSummaryByFolder = dict(robotSummaryByFolder or {})
    writePaths = []
    writeValues = []
    for robotName in list(robotNames or []):
        summary = dict(robotSummaryByFolder.get(robotName) or {})
        basePath = MAINCONTROL_ROBOTS_BASE + "/" + robotName
        for memberName, dataKey, defaultValue in list(MISSION_SORTING_ROBOT_FIELDS):
            writePaths.append(basePath + "/" + memberName)
            writeValues.append(summary.get(dataKey, defaultValue))

    if writePaths:
        writeRequiredTagValues(
            writePaths,
            writeValues,
            labels=["MainController mission mirror"] * len(writePaths)
        )
