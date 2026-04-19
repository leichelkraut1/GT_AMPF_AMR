from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import writeTagValueAsync
from Otto_API.Missions.MissionActions import buildCreateMissionPayload
from Otto_API.Missions.MissionActions import buildFinalizeMissionPayload
from Otto_API.Missions.MissionActions import findActiveMissionIdForRobot
from Otto_API.Missions.MissionActions import interpretCreateMissionResponse
from Otto_API.Missions.MissionActions import interpretFinalizeMissionResponse
from Otto_API.Missions.MissionActions import parseTemplateJson


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
	fleetManagerURL = readRequiredTagValue(
		"[Otto_FleetManager]Url_ApiBase",
		"API base URL"
	) + "/operations/"
	responseTag = "[Otto_FleetManager]Missions/Triggers/lastResponse"
	ottoLogger = system.util.getLogger("OTTO_API_Logger")

	ottoLogger.info("Posting mission from template [{}] for robot [{}]".format(templateTagPath, robotTagPath))

	try:
		try:
			robot_id = readRequiredTagValue(robotTagPath, "Robot ID")
			template_json_str = readRequiredTagValue(templateTagPath, "Template")
		except ValueError as e:
			msg = str(e)
			ottoLogger.error(msg)
			writeTagValueAsync(responseTag, msg)
			return _buildResult(ok=False, level="error", message=msg)

		try:
			template = parseTemplateJson(template_json_str)
		except ValueError as e:
			msg = "Invalid template: {}".format(str(e))
			ottoLogger.error(msg)
			writeTagValueAsync(responseTag, msg)
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
			writeTagValueAsync("[Otto_FleetManager]System/lastResponse", result["response_text"])

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeTagValueAsync(responseTag, result["message"])
		return result

	except Exception as e:
		msg = "Error posting mission: {}".format(str(e))
		ottoLogger.error(msg)
		writeTagValueAsync(responseTag, msg)
		return _buildResult(ok=False, level="error", message=msg)

def finalizeMission(robotName):
	"""
	Finalizes the active mission currently assigned to the specified robot by
	posting an updateMission JSON-RPC request to the OTTO Fleet Manager.

	Args:
		robotName (str): Name of the robot UDT instance under [Otto_FleetManager]Robots
	"""

	# --- Config ---
	fleetManagerURL = readRequiredTagValue(
		"[Otto_FleetManager]Url_ApiBase",
		"API base URL"
	) + "/operations/"
	responseTag = "[Otto_FleetManager]Missions/Triggers/lastResponse"
	ottoLogger = system.util.getLogger("OTTO_API_Logger")

	ottoLogger.info("Finalizing mission for robot [{}]".format(robotName))

	try:
		# --- Resolve robot ID ---
		robotIdPath = "[Otto_FleetManager]Robots/{}/id".format(robotName)
		try:
			robotIdValue = readRequiredTagValue(robotIdPath, "Robot ID")
		except ValueError:
			msg = "Robot [{}] has no valid id tag".format(robotName)
			ottoLogger.warn(msg)
			writeTagValueAsync(responseTag, msg)
			return _buildResult(ok=False, level="warn", message=msg)

		robot_id = str(robotIdValue).strip().lower()

		# --- Locate active mission assigned to this robot ---
		activeMissionsPath = "[Otto_FleetManager]Missions/Active"
		missionBrowseResults = system.tag.browse(activeMissionsPath).getResults()
		missionRecords = []
		missionRows = []

		for mission in missionBrowseResults:
			if str(mission.get("tagType")) != "UdtInstance":
				continue
			missionBasePath = str(mission.get("fullPath"))
			missionRows.append({
				"assigned_robot_path": missionBasePath + "/assigned_robot",
				"id_path": missionBasePath + "/id",
			})

		readPaths = []
		for missionRow in missionRows:
			readPaths.extend([
				missionRow["assigned_robot_path"],
				missionRow["id_path"],
			])

		readResults = []
		if readPaths:
			readResults = system.tag.readBlocking(readPaths)

		for index, missionRow in enumerate(missionRows):
			offset = index * 2
			assignedRobotValue = readResults[offset].value if readResults[offset].quality.isGood() else None
			if not assignedRobotValue:
				continue

			missionRecords.append({
				"assigned_robot": assignedRobotValue,
				"id": readResults[offset + 1].value if readResults[offset + 1].quality.isGood() else None,
			})

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
			writeTagValueAsync(responseTag, msg)
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

		writeTagValueAsync(responseTag, result["message"])
		return result

	except Exception as e:
		msg = "Error finalizing mission for robot [{}]: {}".format(robotName, str(e))
		ottoLogger.error(msg)
		writeTagValueAsync(responseTag, msg)
		return _buildResult(ok=False, level="error", message=msg)
