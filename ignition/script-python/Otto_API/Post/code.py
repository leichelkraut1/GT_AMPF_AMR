import json
import time
import uuid
from Otto_API.ResultHelpers import buildOperationResult
from Otto_API.Get import sanitizeTagName
from Otto_API.TagHelpers import readOptionalTagValue
from Otto_API.TagHelpers import readRequiredTagValue


def parseTemplateJson(templateJsonStr):
	"""
	Parses a workflow template JSON string and returns the template dict.
	"""
	try:
		template = json.loads(templateJsonStr)
	except Exception as e:
		raise ValueError("Invalid template JSON: {}".format(str(e)))

	if not isinstance(template, dict):
		raise ValueError("Template JSON was returned as invalid")

	return template


def buildCreateMissionPayload(templateDict, robotId, missionName, nowEpoch=None, uuidFactory=None):
	"""
	Builds the JSON-RPC payload for createMission from explicit inputs.
	"""
	if nowEpoch is None:
		nowEpoch = time.time()

	if uuidFactory is None:
		uuidFactory = uuid.uuid4

	tasks = templateDict.get("tasks", [])
	if not isinstance(tasks, list):
		tasks = []

	missionPriority = templateDict.get("priority", 100)
	suffix = sanitizeTagName(str(nowEpoch))

	return {
		"id": int(nowEpoch),
		"jsonrpc": "2.0",
		"method": "createMission",
		"params": {
			"mission": {
				"client_reference_id": str(uuidFactory()),
				"description": missionName + " - " + suffix,
				"finalized": False,
				"force_robot": robotId,
				"force_team": None,
				"max_duration": "0",
				"metadata": "",
				"name": missionName + " - " + suffix,
				"nominal_duration": "0",
				"priority": missionPriority
			},
			"tasks": tasks
		}
	}


def interpretCreateMissionResponse(responseText):
	"""
	Parses a createMission response and returns (logLevel, message).
	"""
	try:
		respJson = json.loads(responseText)
	except Exception as e:
		return ("error", "Fleet Manager returned non-JSON response: {}".format(str(e)))

	if "result" in respJson:
		result = respJson["result"]
		missionId = result.get("uuid") or result.get("id")
		if missionId:
			return ("info", "Mission created successfully - ID: {}".format(missionId))
		return ("warn", "Mission created, but no UUID found: {}".format(json.dumps(result)))

	if "error" in respJson:
		return ("warn", "API Error: {}".format(json.dumps(respJson["error"])))

	return ("warn", "Unexpected response: {}".format(responseText))


def findActiveMissionIdForRobot(robotUuid, missionRecords):
	"""
	Finds the active mission ID/UUID assigned to the given robot UUID.
	Returns (missionId, warningMessage).
	"""
	for missionRecord in missionRecords:
		assignedRobotUuid = missionRecord.get("assigned_robot")
		if assignedRobotUuid is None:
			continue

		assignedRobotUuid = str(assignedRobotUuid).strip().lower()
		if assignedRobotUuid != robotUuid:
			continue

		missionId = missionRecord.get("uuid") or missionRecord.get("id")
		if missionId:
			return (str(missionId), None)

		return (
			None,
			"Matched mission for robot UUID [{}], but no uuid/id found".format(robotUuid)
		)

	return (None, None)


def buildFinalizeMissionPayload(missionId, nowEpoch=None):
	"""
	Builds the JSON-RPC payload for updateMission(finalized=True).
	"""
	if nowEpoch is None:
		nowEpoch = time.time()

	return {
		"id": int(nowEpoch),
		"jsonrpc": "2.0",
		"method": "updateMission",
		"params": {
			"append_tasks": [],
			"fields": {
				"finalized": True
			},
			"id": missionId
		}
	}


def interpretFinalizeMissionResponse(responseText, missionId):
	"""
	Parses an updateMission response and returns (logLevel, message).
	"""
	try:
		respJson = json.loads(responseText)
	except Exception as e:
		return ("error", "Non-JSON response while finalizing mission: {}".format(str(e)))

	if "result" in respJson:
		return ("info", "Mission [{}] finalized successfully".format(missionId))

	if "error" in respJson:
		return ("warn", "API Error while finalizing mission: {}".format(json.dumps(respJson["error"])))

	return ("warn", "Unexpected response while finalizing mission: {}".format(responseText))


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


def createMissionFromInputs(templateDict, robotId, missionName, fleetManagerURL, httpPost):
	"""
	Creates a mission from explicit inputs and returns a structured result.
	"""
	try:
		missionPayload = buildCreateMissionPayload(templateDict, robotId, missionName)
		jsonBody = system.util.jsonEncode(missionPayload)
		response = httpPost(
			url=fleetManagerURL,
			postData=jsonBody,
			contentType="application/json",
			headerValues={"Accept": "application/json"},
			bypassCertValidation=True
		)

		logLevel, message = interpretCreateMissionResponse(response)
		missionId = None
		if "ID:" in message:
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


def finalizeMissionFromInputs(robotUuid, missionRecords, fleetManagerURL, httpPost):
	"""
	Finalizes a mission from explicit inputs and returns a structured result.
	"""
	targetMissionUUID, warningMessage = findActiveMissionIdForRobot(robotUuid, missionRecords)
	if targetMissionUUID is None:
		message = warningMessage or "No active mission found for robot UUID [{}]".format(robotUuid)
		return _buildResult(ok=False, level="warn", message=message)

	try:
		missionPayload = buildFinalizeMissionPayload(targetMissionUUID)
		jsonBody = system.util.jsonEncode(missionPayload)
		response = httpPost(
			url=fleetManagerURL,
			postData=jsonBody,
			contentType="application/json",
			headerValues={"Accept": "application/json"},
			bypassCertValidation=True
		)

		logLevel, message = interpretFinalizeMissionResponse(response, targetMissionUUID)
		return _buildResult(
			ok=(logLevel == "info"),
			level=logLevel,
			message=message,
			missionId=targetMissionUUID,
			responseText=response,
			payload=missionPayload,
		)
	except Exception as e:
		return _buildResult(
			ok=False,
			level="error",
			message="Error finalizing mission for robot UUID [{}]: {}".format(robotUuid, str(e)),
			missionId=targetMissionUUID,
		)


def createMission(templateTagPath, robotTagPath, missionName):
	"""
	Posts a mission to the OTTO Fleet Manager using the mission templates and robot ID specified by tag paths.
	Mission templates (workflows) can be pulled into Ignition using Otto_API.Gets.updateMissionTemplates()

	Args:
		templateTagPath (str): Full tag path to the mission template JSON string.
		robotTagPath (str): Full tag path to the robot UUID string.
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
			system.tag.writeAsync(responseTag, msg)
			return _buildResult(ok=False, level="error", message=msg)

		try:
			template = parseTemplateJson(template_json_str)
		except ValueError as e:
			msg = "Invalid template: {}".format(str(e))
			ottoLogger.error(msg)
			system.tag.writeAsync(responseTag, msg)
			return _buildResult(ok=False, level="error", message=msg)

		if not isinstance(template.get("tasks", []), list):
			ottoLogger.warn("Template 'tasks' field missing or not a list; using empty list")

		result = createMissionFromInputs(
			template,
			robot_id,
			missionName,
			fleetManagerURL,
			system.net.httpPost
		)

		if result["response_text"] is not None:
			system.tag.writeAsync("[Otto_FleetManager]System/lastResponse", result["response_text"])

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		system.tag.writeAsync(responseTag, result["message"])
		return result

	except Exception as e:
		msg = "Error posting mission: {}".format(str(e))
		ottoLogger.error(msg)
		system.tag.writeAsync(responseTag, msg)
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
		# --- Resolve robot UUID ---
		robotIdPath = "[Otto_FleetManager]Robots/{}/id".format(robotName)
		try:
			robotIdValue = readRequiredTagValue(robotIdPath, "Robot ID")
		except ValueError:
			msg = "Robot [{}] has no valid id tag".format(robotName)
			ottoLogger.warn(msg)
			system.tag.writeAsync(responseTag, msg)
			return _buildResult(ok=False, level="warn", message=msg)

		robot_uuid = str(robotIdValue).strip().lower()

		# --- Locate active mission assigned to this robot ---
		activeMissionsPath = "[Otto_FleetManager]Missions/Active"
		missionBrowseResults = system.tag.browse(activeMissionsPath).getResults()
		missionRecords = []

		for mission in missionBrowseResults:
			missionBasePath = str(mission.get("fullPath"))

			assignedRobotPath = missionBasePath + "/assigned_robot"
			assignedRobotValue = readOptionalTagValue(assignedRobotPath, None)
			if not assignedRobotValue:
				continue

			# --- Resolve mission identifier (prefer uuid, fallback to id) ---
			uuidPath = missionBasePath + "/uuid"
			idPath = missionBasePath + "/id"

			missionRecords.append({
				"assigned_robot": assignedRobotValue,
				"uuid": readRequiredTagValue(uuidPath, None),
				"id": readOptionalTagValue(idPath, None),
			})

		targetMissionUUID, warningMessage = findActiveMissionIdForRobot(
			robot_uuid,
			missionRecords
		)
		if warningMessage:
			ottoLogger.warn(warningMessage)

		if not targetMissionUUID:
			msg = "No active mission found for robot [{}] (robot_uuid={})".format(
				robotName, robot_uuid
			)
			ottoLogger.info(msg)
			system.tag.writeAsync(responseTag, msg)
			return _buildResult(ok=False, level="warn", message=msg)

		ottoLogger.info(
			"Found active mission [{}] for robot [{}]".format(
				targetMissionUUID, robotName
			)
		)

		result = finalizeMissionFromInputs(
			robot_uuid,
			missionRecords,
			fleetManagerURL,
			system.net.httpPost
		)

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		system.tag.writeAsync(responseTag, result["message"])
		return result

	except Exception as e:
		msg = "Error finalizing mission for robot [{}]: {}".format(robotName, str(e))
		ottoLogger.error(msg)
		system.tag.writeAsync(responseTag, msg)
		return _buildResult(ok=False, level="error", message=msg)
