from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import getMissionTriggerLastResponsePath
from Otto_API.Common.TagHelpers import getOttoOperationsUrl
from Otto_API.Common.TagHelpers import getFleetMissionsPath
from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import writeLastSystemResponse
from Otto_API.Common.TagHelpers import writeLastTriggerResponse
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
from Otto_API.Missions.MissionTreeHelpers import readMissionRobotAwareRecords

ACTIVE_MISSIONS_ROOT = getFleetMissionsPath() + "/Active"
FAILED_MISSIONS_ROOT = getFleetMissionsPath() + "/Failed"
ROBOTS_ROOT = getFleetRobotsPath()


def _log():
	return system.util.getLogger("Otto_API.Missions.Post")


def _buildResult(ok, level, message, missionId=None, responseText=None, payload=None):
	"""
	Builds a structured result object for wrapper and helper callers.
	"""
	return buildOperationResult(
		ok,
		level,
		message,
		data={
			"mission_id": missionId,
			"response_text": responseText,
			"payload": payload,
		},
		mission_id=missionId,
		response_text=responseText,
		payload=payload,
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
	if not targetMissionIds:
		return buildOperationResult(
			False,
			"warn",
			emptyWarnMessage,
			data={
				"mission_ids": [],
				"response_texts": [],
				"payloads": [],
			},
			mission_ids=[],
			response_texts=[],
			payloads=[],
		)

	responseTexts = []
	payloads = []
	canceledMissionIds = []

	try:
		for missionId in targetMissionIds:
			missionPayload = buildCancelMissionPayload(missionId)
			jsonBody = system.util.jsonEncode(missionPayload)
			response = postFunc(
				url=fleetManagerURL,
				postData=jsonBody,
			)
			logLevel, message = interpretCancelMissionResponse(response, missionId)
			if logLevel != "info":
				return buildOperationResult(
					False,
					logLevel,
					message,
					data={
						"mission_ids": canceledMissionIds,
						"response_texts": responseTexts + [response],
						"payloads": payloads + [missionPayload],
					},
					mission_ids=canceledMissionIds,
					response_texts=responseTexts + [response],
					payloads=payloads + [missionPayload],
				)

			canceledMissionIds.append(missionId)
			responseTexts.append(response)
			payloads.append(missionPayload)

		return buildOperationResult(
			True,
			"info",
			successMessage.format(len(canceledMissionIds)),
			data={
				"mission_ids": canceledMissionIds,
				"response_texts": responseTexts,
				"payloads": payloads,
			},
			mission_ids=canceledMissionIds,
			response_texts=responseTexts,
			payloads=payloads,
			mission_id=canceledMissionIds[0] if canceledMissionIds else None,
		)
	except Exception as e:
		return buildOperationResult(
			False,
			"error",
			errorMessage.format(str(e)),
			data={
				"mission_ids": canceledMissionIds,
				"response_texts": responseTexts,
				"payloads": payloads,
			},
			mission_ids=canceledMissionIds,
			response_texts=responseTexts,
			payloads=payloads,
			mission_id=canceledMissionIds[0] if canceledMissionIds else None,
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
	Mission templates (workflows) can be pulled into Ignition using Otto_API.Fleet.Get.updateWorkflows()

	Args:
		templateTagPath (str): Full tag path to the mission template JSON string.
		robotTagPath (str): Full tag path to the robot ID string.
		missionName (str): The name of the mission that Fleet will display in the Work Monitor
	"""

	# --- Config ---
	fleetManagerURL = getOttoOperationsUrl()
	responseTag = getMissionTriggerLastResponsePath()
	ottoLogger = _log()

	ottoLogger.info("Posting mission from template [{}] for robot [{}]".format(templateTagPath, robotTagPath))

	try:
		try:
			robot_id = readRequiredTagValue(robotTagPath, "Robot ID")
			template_json_str = readRequiredTagValue(templateTagPath, "Template")
		except ValueError as e:
			msg = str(e)
			ottoLogger.error(msg)
			writeLastTriggerResponse(msg, asyncWrite=True)
			return _buildResult(ok=False, level="error", message=msg)

		try:
			template = parseTemplateJson(template_json_str)
		except ValueError as e:
			msg = "Invalid template: {}".format(str(e))
			ottoLogger.error(msg)
			writeLastTriggerResponse(msg, asyncWrite=True)
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

		if result["response_text"] is not None:
			writeLastSystemResponse(result["response_text"], asyncWrite=True)

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeLastTriggerResponse(result["message"], asyncWrite=True)
		return result

	except Exception as e:
		msg = "Error posting mission: {}".format(str(e))
		ottoLogger.error(msg)
		writeLastTriggerResponse(msg, asyncWrite=True)
		return _buildResult(ok=False, level="error", message=msg)

def finalizeMission(robotName):
	"""
	Finalizes the active mission currently assigned to the specified robot by
	posting an updateMission JSON-RPC request to the OTTO Fleet Manager.

	Args:
		robotName (str): Name of the robot UDT instance under [Otto_FleetManager]Fleet/Robots
	"""

	# --- Config ---
	fleetManagerURL = getOttoOperationsUrl()
	responseTag = getMissionTriggerLastResponsePath()
	ottoLogger = _log()

	ottoLogger.info("Finalizing mission for robot [{}]".format(robotName))

	try:
		# --- Resolve robot ID ---
		robotIdPath = "{}/{}/id".format(ROBOTS_ROOT, robotName)
		try:
			robotIdValue = readRequiredTagValue(robotIdPath, "Robot ID")
		except ValueError:
			msg = "Robot [{}] has no valid id tag".format(robotName)
			ottoLogger.warn(msg)
			writeLastTriggerResponse(msg, asyncWrite=True)
			return _buildResult(ok=False, level="warn", message=msg)

		robot_id = str(robotIdValue).strip().lower()

		# --- Locate active mission assigned to this robot ---
		missionRecords = readMissionRobotAwareRecords(ACTIVE_MISSIONS_ROOT)

		targetMissionId, warningMessage = findActiveMissionIdForRobot(
			robot_id,
			missionRecords
		)
		if warningMessage:
			ottoLogger.warn(warningMessage)

		if not targetMissionId:
			msg = "No active mission found for robot [{}] (robot_id={})".format(
				robotName, robot_id
			)
			ottoLogger.info(msg)
			writeLastTriggerResponse(msg, asyncWrite=True)
			return _buildResult(ok=False, level="warn", message=msg)

		ottoLogger.info(
			"Found active mission [{}] for robot [{}]".format(
				targetMissionId, robotName
			)
		)

		result = finalizeMissionFromInputs(
			robot_id,
			missionRecords,
			fleetManagerURL,
			httpPost
		)

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeLastTriggerResponse(result["message"], asyncWrite=True)
		return result

	except Exception as e:
		msg = "Error finalizing mission for robot [{}]: {}".format(robotName, str(e))
		ottoLogger.error(msg)
		writeLastTriggerResponse(msg, asyncWrite=True)
		return _buildResult(ok=False, level="error", message=msg)


def cancelMission(robotName):
	"""
	Cancels all active missions currently assigned to the specified robot by
	posting cancelMission JSON-RPC requests to the OTTO Fleet Manager.

	Args:
		robotName (str): Name of the robot UDT instance under [Otto_FleetManager]Fleet/Robots
	"""

	fleetManagerURL = getOttoOperationsUrl()
	responseTag = getMissionTriggerLastResponsePath()
	ottoLogger = _log()

	ottoLogger.info("Canceling mission(s) for robot [{}]".format(robotName))

	try:
		robotIdPath = "{}/{}/id".format(ROBOTS_ROOT, robotName)
		try:
			robotIdValue = readRequiredTagValue(robotIdPath, "Robot ID")
		except ValueError:
			msg = "Robot [{}] has no valid id tag".format(robotName)
			ottoLogger.warn(msg)
			writeLastTriggerResponse(msg, asyncWrite=True)
			return _buildResult(ok=False, level="warn", message=msg)

		robot_id = str(robotIdValue).strip().lower()
		missionRecords = readMissionRobotAwareRecords(ACTIVE_MISSIONS_ROOT)
		result = cancelMissionsFromInputs(
			robot_id,
			missionRecords,
			fleetManagerURL,
			httpPost
		)

		if result.get("response_texts"):
			writeLastSystemResponse(result["response_texts"][-1], asyncWrite=True)

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeLastTriggerResponse(result["message"], asyncWrite=True)
		return result

	except Exception as e:
		msg = "Error canceling mission(s) for robot [{}]: {}".format(robotName, str(e))
		ottoLogger.error(msg)
		writeLastTriggerResponse(msg, asyncWrite=True)
		return _buildResult(ok=False, level="error", message=msg)


def cancelAllActiveMissions():
	"""
	Cancels all known active missions currently present under [Otto_FleetManager]Fleet/Missions/Active.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	responseTag = getMissionTriggerLastResponsePath()
	ottoLogger = _log()

	ottoLogger.info("Canceling all active missions")

	try:
		missionRecords = readMissionIdRecords(ACTIVE_MISSIONS_ROOT)
		result = cancelAllActiveMissionsFromInputs(
			missionRecords,
			fleetManagerURL,
			httpPost
		)

		if result.get("response_texts"):
			writeLastSystemResponse(result["response_texts"][-1], asyncWrite=True)

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeLastTriggerResponse(result["message"], asyncWrite=True)
		return result

	except Exception as e:
		msg = "Error canceling all active missions: {}".format(str(e))
		ottoLogger.error(msg)
		writeLastTriggerResponse(msg, asyncWrite=True)
		return _buildResult(ok=False, level="error", message=msg)


def cancelAllFailedMissions():
	"""
	Cancels all known failed missions currently present under [Otto_FleetManager]Fleet/Missions/Failed.
	"""
	fleetManagerURL = getOttoOperationsUrl()
	responseTag = getMissionTriggerLastResponsePath()
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

		if result.get("response_texts"):
			writeLastSystemResponse(result["response_texts"][-1], asyncWrite=True)

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeLastTriggerResponse(result["message"], asyncWrite=True)
		return result

	except Exception as e:
		msg = "Error canceling all failed missions: {}".format(str(e))
		ottoLogger.error(msg)
		writeLastTriggerResponse(msg, asyncWrite=True)
		return _buildResult(ok=False, level="error", message=msg)
