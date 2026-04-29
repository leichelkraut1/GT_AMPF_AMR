from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.OperationHelpers import logOperationResult
from Otto_API.Common.TagIO import getOttoOperationsUrl
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Missions.MissionActions import parseTemplateJson
from Otto_API.TagSync.Missions.Tree import readMissionIdRecords
from Otto_API.Models.Missions import MissionRecord
from Otto_API.Models.Results import OperationalResult
from Otto_API.WebAPI.Missions import postCancelMissions
from Otto_API.WebAPI.Missions import postCreateMission
from Otto_API.WebAPI.Missions import postFinalizeMission

ACTIVE_MISSIONS_ROOT = getFleetMissionsPath() + "/Active"
FAILED_MISSIONS_ROOT = getFleetMissionsPath() + "/Failed"


def _log():
	return system.util.getLogger("Otto_API.Services.Missions")


def _buildResult(ok, level, message, missionId=None, responseText=None, payload=None):
	"""
	Builds a structured boundary result for service callers.
	"""
	return OperationalResult(
		ok,
		level,
		message,
		dataFields={
			"mission_id": missionId,
			"response_text": responseText,
			"payload": payload,
		},
	).toDict()


def _writeAndLogMissionResult(result, logger):
	"""
	Write response/message side effects and log one structured mission result.
	"""
	return logOperationResult(result, logger)


def createMission(templateTagPath, robotTagPath, missionName):
	"""
	Posts a mission to OTTO using a mission template and robot id from Ignition tags.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Posting mission from template [{}] for robot [{}]".format(templateTagPath, robotTagPath))

	try:
		try:
			robot_id = str(readRequiredTagValue(robotTagPath, "Robot ID"))
			template_json_str = str(readRequiredTagValue(templateTagPath, "Template"))
		except ValueError as e:
			msg = str(e)
			ottoLogger.error(msg)
			return _buildResult(ok=False, level="error", message=msg)

		try:
			template = parseTemplateJson(template_json_str)
		except ValueError as e:
			msg = "Invalid template: {}".format(str(e))
			ottoLogger.error(msg)
			return _buildResult(ok=False, level="error", message=msg)

		if not isinstance(template.get("tasks", []), list):
			ottoLogger.warn("Template 'tasks' field missing or not a list; using empty list")

		result = postCreateMission(
			fleetManagerURL,
			template,
			robot_id,
			missionName,
			httpPost
		).toDict()
		return _writeAndLogMissionResult(result, ottoLogger)

	except Exception as e:
		msg = "Error posting mission: {}".format(str(e))
		ottoLogger.error(msg)
		return _buildResult(ok=False, level="error", message=msg)


def finalizeMissionId(missionId):
	"""
	Finalize one explicit mission id.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Finalizing mission [{}]".format(missionId))

	result = postFinalizeMission(
		fleetManagerURL,
		missionId,
		httpPost
	).toDict()
	return _writeAndLogMissionResult(result, ottoLogger)


def cancelMissionIds(missionIds):
	"""
	Cancel an explicit list of mission ids.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Canceling explicit mission id list [{}]".format(list(missionIds or [])))

	result = postCancelMissions(
		fleetManagerURL,
		missionIds,
		httpPost,
		emptyWarnMessage="No explicit mission ids found to cancel",
		successMessage="Canceled {} explicit mission(s)",
		errorMessage="Error canceling explicit missions: {}",
	).toDict()
	return _writeAndLogMissionResult(result, ottoLogger)


def cancelAllActiveMissions():
	"""
	Cancels all known active missions currently present under the active mission tag tree.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Canceling all active missions")

	try:
		missionRecords = readMissionIdRecords(ACTIVE_MISSIONS_ROOT)
		targetMissionIds = [
			str(missionRecord.id)
			for missionRecord in MissionRecord.listFromDicts(missionRecords)
			if missionRecord.id
		]
		result = postCancelMissions(
			fleetManagerURL,
			targetMissionIds,
			httpPost,
			emptyWarnMessage="No active missions found to cancel",
			successMessage="Canceled {} active mission(s)",
			errorMessage="Error canceling active missions: {}",
		).toDict()
		return _writeAndLogMissionResult(result, ottoLogger)

	except Exception as e:
		msg = "Error canceling all active missions: {}".format(str(e))
		ottoLogger.error(msg)
		return _buildResult(ok=False, level="error", message=msg)


def cancelAllFailedMissions():
	"""
	Cancels all known failed missions currently present under the failed mission tag tree.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Canceling all failed missions")

	try:
		missionRecords = readMissionIdRecords(FAILED_MISSIONS_ROOT)
		targetMissionIds = [
			str(missionRecord.id)
			for missionRecord in MissionRecord.listFromDicts(missionRecords)
			if missionRecord.id
		]
		result = postCancelMissions(
			fleetManagerURL,
			targetMissionIds,
			httpPost,
			emptyWarnMessage="No failed missions found to cancel",
			successMessage="Canceled {} failed mission(s)",
			errorMessage="Error canceling failed missions: {}",
		).toDict()
		return _writeAndLogMissionResult(result, ottoLogger)

	except Exception as e:
		msg = "Error canceling all failed missions: {}".format(str(e))
		ottoLogger.error(msg)
		return _buildResult(ok=False, level="error", message=msg)
