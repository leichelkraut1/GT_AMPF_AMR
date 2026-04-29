from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Models.Missions import findActiveMissionIdForRobot
from Otto_API.Models.Missions import findActiveMissionIdsForRobot
from Otto_API.Models.Results import OperationalResult
from Otto_API.Services.Missions.Operations import cancelMissionIds as cancelMissionIdsOperation
from Otto_API.Services.Missions.Operations import finalizeMissionId as finalizeMissionIdOperation
from Otto_API.TagSync.Missions.Tree import readMissionRobotAwareRecords


ACTIVE_MISSIONS_ROOT = getFleetMissionsPath() + "/Active"
ROBOTS_ROOT = getFleetRobotsPath()


def _log():
    return system.util.getLogger("Otto_API.Services.Missions.Commands")


def _warnResult(message):
    return OperationalResult(
        False,
        "warn",
        message,
    ).toDict()


def _resolveRobotId(robotName):
    robotIdPath = "{}/{}/ID".format(ROBOTS_ROOT, robotName)
    try:
        return str(readRequiredTagValue(robotIdPath, "Robot ID")).strip().lower()
    except ValueError:
        return None


def finalizeActiveMissionForRobot(robotName, finalizeMissionId=None):
    ottoLogger = _log()
    robotId = _resolveRobotId(robotName)
    if not robotId:
        message = "Robot [{}] has no valid id tag".format(robotName)
        ottoLogger.warn(message)
        return _warnResult(message)

    missionRecords = readMissionRobotAwareRecords(ACTIVE_MISSIONS_ROOT)
    missionId, warningMessage = findActiveMissionIdForRobot(robotId, missionRecords)
    if warningMessage:
        ottoLogger.warn(warningMessage)

    if not missionId:
        message = "No active mission found for robot [{}] (robot_id={})".format(robotName, robotId)
        ottoLogger.info(message)
        return _warnResult(message)

    if finalizeMissionId is None:
        finalizeMissionId = finalizeMissionIdOperation
    return finalizeMissionId(missionId)


def cancelActiveMissionsForRobot(robotName, cancelMissionIds=None):
    ottoLogger = _log()
    robotId = _resolveRobotId(robotName)
    if not robotId:
        message = "Robot [{}] has no valid id tag".format(robotName)
        ottoLogger.warn(message)
        return _warnResult(message)

    missionRecords = readMissionRobotAwareRecords(ACTIVE_MISSIONS_ROOT)
    missionIds, warningMessages = findActiveMissionIdsForRobot(robotId, missionRecords)
    for warningMessage in list(warningMessages or []):
        ottoLogger.warn(warningMessage)

    if not missionIds:
        message = "No active missions found for robot [{}] (robot_id={})".format(robotName, robotId)
        ottoLogger.info(message)
        return _warnResult(message)

    if cancelMissionIds is None:
        cancelMissionIds = cancelMissionIdsOperation
    result = cancelMissionIds(missionIds)
    if result.get("ok"):
        ottoLogger.info("Canceled {} mission(s) for robot [{}]".format(len(missionIds), robotName))
    return result
