from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.OperationHelpers import buildDataResult
from Otto_API.Common.OperationHelpers import logOperationResult
from Otto_API.Common.TagIO import getOttoOperationsUrl
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Missions.MissionActions import buildCancelMissionPayload
from Otto_API.Missions.MissionActions import buildCreateMissionPayload
from Otto_API.Missions.MissionActions import buildFinalizeMissionPayload
from Otto_API.Missions.MissionActions import findActiveMissionIdsForRobot
from Otto_API.Missions.MissionActions import findActiveMissionIdForRobot
from Otto_API.Missions.MissionActions import interpretCancelMissionResponse
from Otto_API.Missions.MissionActions import interpretCreateMissionResponse
from Otto_API.Missions.MissionActions import interpretFinalizeMissionResponse
from Otto_API.Missions.MissionActions import parseTemplateJson
from Otto_API.Missions.MissionTreeHelpers import readMissionIdRecords

ACTIVE_MISSIONS_ROOT = getFleetMissionsPath() + "/Active"
FAILED_MISSIONS_ROOT = getFleetMissionsPath() + "/Failed"


def _log():
	return system.util.getLogger("Otto_API.Missions.Post")


def _buildResult(ok, level, message, missionId=None, responseText=None, payload=None):
	"""
	Builds a structured result object for wrapper and helper callers.
	"""
	return buildDataResult(
		ok,
		level,
		message,
		mission_id=missionId,
		response_text=responseText,
		payload=payload,
	)


def _buildCancelBatchResult(ok, level, message, missionIds=None, responseTexts=None, payloads=None):
	"""
	Build a structured batch-cancel result object.
	"""
	missionIds = list(missionIds or [])
	responseTexts = list(responseTexts or [])
	payloads = list(payloads or [])
	firstMissionId = missionIds[0] if missionIds else None
	return buildDataResult(
		ok,
		level,
		message,
		mission_ids=missionIds,
		response_texts=responseTexts,
		payloads=payloads,
		mission_id=firstMissionId,
	)


def _writeAndLogMissionResult(result, logger):
	"""
	Write response/message side effects and log one structured mission result.
	"""
	return logOperationResult(result, logger)


def _runMissionCommandFromInputs(actionName, missionId, fleetManagerURL, postFunc):
	"""
	Run one explicit finalize/cancel mission command and return a structured result.
	"""
	actionName = str(actionName or "")
	if not missionId:
		message = "No mission id supplied for {}".format(actionName.replace("_", " ") or "command")
		return _buildResult(
			ok=False,
			level="warn",
			message=message,
		)

	if actionName == "finalize_mission":
		buildPayload = buildFinalizeMissionPayload
		interpretResponse = interpretFinalizeMissionResponse
		errorTemplate = "Error finalizing mission [{}]: {}"
	elif actionName == "cancel_mission":
		buildPayload = buildCancelMissionPayload
		interpretResponse = interpretCancelMissionResponse
		errorTemplate = "Error canceling mission [{}]: {}"
	else:
		return _buildResult(
			ok=False,
			level="error",
			message="Unsupported mission command [{}]".format(actionName),
			missionId=missionId,
		)

	try:
		missionPayload = buildPayload(missionId)
		jsonBody = system.util.jsonEncode(missionPayload)
		response = postFunc(
			url=fleetManagerURL,
			postData=jsonBody,
		)

		logLevel, message = interpretResponse(response, missionId)
		return _buildResult(
			ok=(logLevel == "info"),
			level=logLevel,
			message=message,
			missionId=missionId,
			responseText=response,
			payload=missionPayload,
		)
	except Exception as e:
		return _buildResult(
			ok=False,
			level="error",
			message=errorTemplate.format(missionId, str(e)),
			missionId=missionId,
		)


def _cancelMissionIdsFromInputs(
	targetMissionIds,
	fleetManagerURL,
	postFunc,
	emptyWarnMessage,
	successMessage,
	errorMessage
):
	"""
	Cancel an explicit list of mission ids and return a structured result.
	"""
	targetMissionIds = [str(missionId) for missionId in list(targetMissionIds or []) if missionId]
	def _cancelBatchResult(ok, level, message, missionIds=None, responseTexts=None, payloads=None):
		return _buildCancelBatchResult(
			ok=ok,
			level=level,
			message=message,
			missionIds=missionIds,
			responseTexts=responseTexts,
			payloads=payloads
		)

	if not targetMissionIds:
		return _cancelBatchResult(False, "warn", emptyWarnMessage)

	responseTexts = []
	payloads = []
	canceledMissionIds = []

	try:
		for missionId in targetMissionIds:
			result = _runMissionCommandFromInputs(
				"cancel_mission",
				missionId,
				fleetManagerURL,
				postFunc,
			)
			if not result.get("ok"):
				resultData = dict(result.get("data") or {})
				return _cancelBatchResult(
					False,
					result.get("level", "warn"),
					result.get("message", ""),
					missionIds=canceledMissionIds,
					responseTexts=responseTexts + [resultData.get("response_text")],
					payloads=payloads + [resultData.get("payload")]
				)

			canceledMissionIds.append(missionId)
			resultData = dict(result.get("data") or {})
			responseTexts.append(resultData.get("response_text"))
			payloads.append(resultData.get("payload"))

		return _cancelBatchResult(
			True,
			"info",
			successMessage.format(len(canceledMissionIds)),
			missionIds=canceledMissionIds,
			responseTexts=responseTexts,
			payloads=payloads
		)
	except Exception as e:
		return _cancelBatchResult(
			False,
			"error",
			errorMessage.format(str(e)),
			missionIds=canceledMissionIds,
			responseTexts=responseTexts,
			payloads=payloads
		)


def createMissionFromInputs(templateDict, robotId, missionName, fleetManagerURL, postFunc):
	"""
	Creates a mission from explicit inputs and returns a structured result.
	"""
	try:
		missionPayload = buildCreateMissionPayload(templateDict, robotId, missionName)
		jsonBody = system.util.jsonEncode(missionPayload)
		response = postFunc(
			url=fleetManagerURL,
			postData=jsonBody,
		)

		logLevel, message = interpretCreateMissionResponse(response)
		missionId = None
		if "ID:" in message:
			# The createMission response ID is currently used only for logging/diagnostics.
			# Mission state is reconciled from the periodic mission list sync rather than
			# seeding the Active mission tag tree directly from this response.
			missionId = message.split("ID:", 1)[1].strip()

		return _buildResult(
			ok=(logLevel == "info"),
			level=logLevel,
			message=message,
			missionId=missionId,
			responseText=response,
			payload=missionPayload,
		)
	except Exception as e:
		return _buildResult(
			ok=False,
			level="error",
			message="Error posting mission: {}".format(str(e)),
			payload=None,
		)


def finalizeMissionFromInputs(robotId, missionRecords, fleetManagerURL, postFunc):
	"""
	Finalizes a mission from explicit inputs and returns a structured result.
	"""
	targetMissionId, warningMessage = findActiveMissionIdForRobot(robotId, missionRecords)
	if targetMissionId is None:
		message = warningMessage or "No active mission found for robot ID [{}]".format(robotId)
		return _buildResult(ok=False, level="warn", message=message)

	try:
		missionPayload = buildFinalizeMissionPayload(targetMissionId)
		jsonBody = system.util.jsonEncode(missionPayload)
		response = postFunc(
			url=fleetManagerURL,
			postData=jsonBody,
		)

		logLevel, message = interpretFinalizeMissionResponse(response, targetMissionId)
		return _buildResult(
			ok=(logLevel == "info"),
			level=logLevel,
			message=message,
			missionId=targetMissionId,
			responseText=response,
			payload=missionPayload,
		)
	except Exception as e:
		return _buildResult(
			ok=False,
			level="error",
			message="Error finalizing mission for robot ID [{}]: {}".format(robotId, str(e)),
			missionId=targetMissionId,
		)


def finalizeMissionIdFromInputs(missionId, fleetManagerURL, postFunc):
	"""
	Finalize one explicit mission id and return a structured result.
	"""
	return _runMissionCommandFromInputs(
		"finalize_mission",
		missionId,
		fleetManagerURL,
		postFunc,
	)


def cancelMissionsFromInputs(robotId, missionRecords, fleetManagerURL, postFunc):
	"""
	Cancels all known active missions for the given robot ID and returns a structured result.
	"""
	targetMissionIds, warningMessages = findActiveMissionIdsForRobot(robotId, missionRecords)
	if not targetMissionIds:
		message = "No active missions found for robot ID [{}]".format(robotId)
		if warningMessages:
			message = warningMessages[0]
		return _cancelMissionIdsFromInputs(
			[],
			fleetManagerURL,
			postFunc,
			message,
			"Canceled {} mission(s) for robot ID [{}]".format("{}", robotId),
			"Error canceling missions for robot ID [{}]: {{}}".format(robotId)
		)

	return _cancelMissionIdsFromInputs(
		targetMissionIds,
		fleetManagerURL,
		postFunc,
		"No active missions found for robot ID [{}]".format(robotId),
		"Canceled {} mission(s) for robot ID [{}]".format("{}", robotId),
		"Error canceling missions for robot ID [{}]: {{}}".format(robotId)
	)


def cancelAllActiveMissionsFromInputs(missionRecords, fleetManagerURL, postFunc):
	"""
	Cancels all known active missions and returns a structured result.
	"""
	targetMissionIds = []
	for missionRecord in list(missionRecords or []):
		missionId = missionRecord.get("id")
		if missionId:
			targetMissionIds.append(str(missionId))

	return _cancelMissionIdsFromInputs(
		targetMissionIds,
		fleetManagerURL,
		postFunc,
		"No active missions found to cancel",
		"Canceled {} active mission(s)",
		"Error canceling active missions: {}"
	)


def createMission(templateTagPath, robotTagPath, missionName):
	"""
	Posts a mission to the OTTO Fleet Manager using the mission templates and robot ID specified by tag paths.
	Mission templates (workflows) can be pulled into Ignition using Otto_API.Workflows.Get.updateWorkflows()

	Args:
		templateTagPath (str): Full tag path to the mission template JSON string.
		robotTagPath (str): Full tag path to the robot ID string.
		missionName (str): The name of the mission that Fleet will display in the Work Monitor
	"""

	# --- Config ---
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Posting mission from template [{}] for robot [{}]".format(templateTagPath, robotTagPath))

	try:
		try:
			robot_id = readRequiredTagValue(robotTagPath, "Robot ID")
			template_json_str = readRequiredTagValue(templateTagPath, "Template")
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

		result = createMissionFromInputs(
			template,
			robot_id,
			missionName,
			fleetManagerURL,
			httpPost
		)
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

	result = finalizeMissionIdFromInputs(
		missionId,
		fleetManagerURL,
		httpPost
	)
	return _writeAndLogMissionResult(result, ottoLogger)


def cancelMissionIds(missionIds):
	"""
	Cancel an explicit list of mission ids.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Canceling explicit mission id list [{}]".format(list(missionIds or [])))

	result = _cancelMissionIdsFromInputs(
		missionIds,
		fleetManagerURL,
		httpPost,
		"No explicit mission ids found to cancel",
		"Canceled {} explicit mission(s)",
		"Error canceling explicit missions: {}"
	)
	return _writeAndLogMissionResult(result, ottoLogger)


def cancelAllActiveMissions():
	"""
	Cancels all known active missions currently present under [Otto_FleetManager]Fleet/Missions/Active.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Canceling all active missions")

	try:
		missionRecords = readMissionIdRecords(ACTIVE_MISSIONS_ROOT)
		result = cancelAllActiveMissionsFromInputs(
			missionRecords,
			fleetManagerURL,
			httpPost
		)
		return _writeAndLogMissionResult(result, ottoLogger)

	except Exception as e:
		msg = "Error canceling all active missions: {}".format(str(e))
		ottoLogger.error(msg)
		return _buildResult(ok=False, level="error", message=msg)


def cancelAllFailedMissions():
	"""
	Cancels all known failed missions currently present under [Otto_FleetManager]Fleet/Missions/Failed.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	ottoLogger = _log()

	ottoLogger.info("Canceling all failed missions")

	try:
		missionRecords = readMissionIdRecords(FAILED_MISSIONS_ROOT)
		targetMissionIds = [
			str(missionRecord.get("id"))
			for missionRecord in list(missionRecords or [])
			if missionRecord.get("id")
		]
		result = _cancelMissionIdsFromInputs(
			targetMissionIds,
			fleetManagerURL,
			httpPost,
			"No failed missions found to cancel",
			"Canceled {} failed mission(s)",
			"Error canceling failed missions: {}"
		)
		return _writeAndLogMissionResult(result, ottoLogger)

	except Exception as e:
		msg = "Error canceling all failed missions: {}".format(str(e))
		ottoLogger.error(msg)
		return _buildResult(ok=False, level="error", message=msg)
