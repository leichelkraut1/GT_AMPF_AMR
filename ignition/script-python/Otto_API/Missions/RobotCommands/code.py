from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Missions.MissionActions import findActiveMissionIdForRobot
from Otto_API.Missions.MissionActions import findActiveMissionIdsForRobot
from Otto_API.Missions.MissionTreeHelpers import readMissionRobotAwareRecords
from Otto_API.Services import Missions


ACTIVE_MISSIONS_ROOT = getFleetMissionsPath() + "/Active"
ROBOTS_ROOT = getFleetRobotsPath()


def _log():
	return system.util.getLogger("Otto_API.Missions.RobotCommands")


def _warnResult(message):
	return buildOperationResult(
		False,
		"warn",
		message,
		data={},
	)


def _resolveRobotId(robotName):
	"""Resolve the configured OTTO robot id for one Ignition robot name."""
	robotIdPath = "{}/{}/ID".format(ROBOTS_ROOT, robotName)
	try:
		return str(readRequiredTagValue(robotIdPath, "Robot ID")).strip().lower()
	except ValueError:
		return None


def finalizeActiveMissionForRobot(robotName, finalizeMissionId=None):
	"""Resolve the current active mission for a robot and finalize it by explicit mission id."""
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
		finalizeMissionId = Missions.finalizeMissionId
	return finalizeMissionId(missionId)


def cancelActiveMissionsForRobot(robotName, cancelMissionIds=None):
	"""Resolve all active missions for a robot and cancel them by explicit mission id list."""
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
		cancelMissionIds = Missions.cancelMissionIds
	result = cancelMissionIds(missionIds)
	if result.get("ok"):
		ottoLogger.info("Canceled {} mission(s) for robot [{}]".format(len(missionIds), robotName))
	return result
