import re

from Otto_API.Common.SyncHelpers import sanitizeTagName
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Missions.MissionActions import resolveMissionRobotId
from Otto_API.Missions.Records import coerceMissionRecord
from Otto_API.Robots.SyncHelpers import readRobotInventoryMetadata


UNASSIGNED_FOLDER = "Unassigned"
UNKNOWN_ROBOT_FOLDER = "Unknown_Robot"
DEFAULT_TERMINAL_STATUSES = [
    "CANCELLED",
    "SUCCEEDED",
    "REVOKED",
]
DEFAULT_FAILED_STATUSES = [
    "FAILED",
]


def make_instance_name(mission):
    """
    Creates a readable and mostly-unique mission tag name.
    """
    missionRecord = coerceMissionRecord(mission)
    name = sanitizeTagName(missionRecord.name)
    short = missionRecord.id[:8]
    return "{}_{}".format(name, short)


def classify_mission_bucket(missionStatus, terminalStatuses=None, failedStatuses=None):
    """
    Classify a mission into the Active, Failed, or Completed bucket.
    """
    if terminalStatuses is None:
        terminalStatuses = DEFAULT_TERMINAL_STATUSES
    if failedStatuses is None:
        failedStatuses = DEFAULT_FAILED_STATUSES

    status = str(missionStatus or "").upper()
    if status in failedStatuses:
        return "failed"
    if status in terminalStatuses:
        return "completed"
    return "active"


def readRobotFolderMappings(robotsPath=None, logger=None):
    """
    Build lookup maps for robot folder names and robot IDs.
    """
    if robotsPath is None:
        robotsPath = getFleetRobotsPath()

    try:
        inventory = readRobotInventoryMetadata(robotsPath)
    except Exception as exc:
        if logger is not None:
            logger.warn(
                "Failed to read robot inventory metadata from [{}]: {}".format(
                    robotsPath,
                    str(exc)
                )
            )
        return {
            "name_by_lower": {},
            "name_by_id": {},
        }

    return {
        "name_by_lower": dict(inventory.get("robot_name_by_lower", {})),
        "name_by_id": dict(inventory.get("robot_name_by_id", {})),
    }


def resolve_mission_robot_folder(mission, robotMappings=None, robotsPath=None, logger=None):
    """
    Resolve the mission's robot-specific folder name or return Unassigned.
    """
    if robotMappings is None:
        robotMappings = readRobotFolderMappings(robotsPath=robotsPath, logger=logger)

    resolvedRobot = resolveMissionRobotId(mission)
    if not resolvedRobot:
        return UNASSIGNED_FOLDER

    resolvedRobot = str(resolvedRobot).strip().lower()
    if not resolvedRobot:
        return UNASSIGNED_FOLDER

    robotName = robotMappings["name_by_lower"].get(resolvedRobot)
    if robotName:
        return robotName

    robotName = robotMappings["name_by_id"].get(resolvedRobot)
    if robotName:
        return robotName

    return UNKNOWN_ROBOT_FOLDER


def build_mission_bucket_paths(activePath, completedPath, failedPath, robotFolder, instanceName):
    """
    Return the fully-qualified mission instance path for each bucket.
    """
    return {
        "active": activePath + "/" + robotFolder + "/" + instanceName,
        "completed": completedPath + "/" + robotFolder + "/" + instanceName,
        "failed": failedPath + "/" + robotFolder + "/" + instanceName,
    }
