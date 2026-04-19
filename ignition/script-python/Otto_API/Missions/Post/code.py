from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import writeTagValueAsync
from Otto_API.Missions.MissionActions import buildCancelMissionPayload
from Otto_API.Missions.MissionActions import buildCreateMissionPayload
from Otto_API.Missions.MissionActions import buildFinalizeMissionPayload
from Otto_API.Missions.MissionActions import findActiveMissionIdsForRobot
from Otto_API.Missions.MissionActions import findActiveMissionIdForRobot
from Otto_API.Missions.MissionActions import interpretCancelMissionResponse
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


def _readActiveMissionRecords():
	"""
	Read active mission assignments in one browse + one bulk read pass.
	"""
	def _browseMissionInstancesRecursive(folderPath):
		missionBasePaths = []
		pending = [folderPath]

		while pending:
			currentFolder = pending.pop(0)
			for row in system.tag.browse(currentFolder).getResults():
				tagType = str(row.get("tagType"))
				fullPath = str(row.get("fullPath"))
				if tagType == "UdtInstance":
					missionBasePaths.append(fullPath)
				elif tagType == "Folder":
					pending.append(fullPath)

		return missionBasePaths

	def _pickValue(*qualifiedValues):
		for qualifiedValue in qualifiedValues:
			if qualifiedValue is None or not qualifiedValue.quality.isGood():
				continue
			value = qualifiedValue.value
			if value is None:
				continue
			if not str(value).strip():
				continue
			return value
		return None

	activeMissionsPath = "[Otto_FleetManager]Missions/Active"
	missionRows = []

	for missionBasePath in _browseMissionInstancesRecursive(activeMissionsPath):
		missionRows.append({
			"assigned_robot_path": missionBasePath + "/assigned_robot",
			"assigned_robot_alt_path": missionBasePath + "/Assigned_Robot",
			"force_robot_path": missionBasePath + "/force_robot",
			"force_robot_alt_path": missionBasePath + "/Force_Robot",
			"forced_robot_path": missionBasePath + "/forced_robot",
			"forced_robot_alt_path": missionBasePath + "/Forced_Robot",
			"id_path": missionBasePath + "/id",
			"id_alt_path": missionBasePath + "/ID",
		})

	readPaths = []
	for missionRow in missionRows:
		readPaths.extend([
			missionRow["assigned_robot_path"],
			missionRow["assigned_robot_alt_path"],
			missionRow["force_robot_path"],
			missionRow["force_robot_alt_path"],
			missionRow["forced_robot_path"],
			missionRow["forced_robot_alt_path"],
			missionRow["id_path"],
			missionRow["id_alt_path"],
		])

	readResults = []
	if readPaths:
		readResults = system.tag.readBlocking(readPaths)

	missionRecords = []
	for index, missionRow in enumerate(missionRows):
		offset = index * 8
		assignedRobotValue = _pickValue(
			readResults[offset],
			readResults[offset + 1],
		)
		forceRobotValue = _pickValue(
			readResults[offset + 2],
			readResults[offset + 3],
		)
		forcedRobotValue = _pickValue(
			readResults[offset + 4],
			readResults[offset + 5],
		)
		missionIdValue = _pickValue(
			readResults[offset + 6],
			readResults[offset + 7],
		)

		if not assignedRobotValue and not forceRobotValue and not forcedRobotValue:
			continue

		missionRecords.append({
			"assigned_robot": assignedRobotValue,
			"force_robot": forceRobotValue,
			"forced_robot": forcedRobotValue,
			"id": missionIdValue,
		})

	return missionRecords


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
		return buildOperationResult(
			False,
			"warn",
			message,
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
			"Canceled {} mission(s) for robot ID [{}]".format(len(canceledMissionIds), robotId),
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
			"Error canceling missions for robot ID [{}]: {}".format(robotId, str(e)),
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


def cancelAllActiveMissionsFromInputs(missionRecords, fleetManagerURL, postFunc):
	"""
	Cancels all known active missions and returns a structured result.
	"""
	targetMissionIds = []
	for missionRecord in list(missionRecords or []):
		missionId = missionRecord.get("id")
		if missionId:
			targetMissionIds.append(str(missionId))

	if not targetMissionIds:
		return buildOperationResult(
			False,
			"warn",
			"No active missions found to cancel",
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
			"Canceled {} active mission(s)".format(len(canceledMissionIds)),
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
			"Error canceling active missions: {}".format(str(e)),
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
		missionRecords = _readActiveMissionRecords()

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


def cancelMission(robotName):
	"""
	Cancels all active missions currently assigned to the specified robot by
	posting cancelMission JSON-RPC requests to the OTTO Fleet Manager.

	Args:
		robotName (str): Name of the robot UDT instance under [Otto_FleetManager]Robots
	"""

	fleetManagerURL = readRequiredTagValue(
		"[Otto_FleetManager]Url_ApiBase",
		"API base URL"
	) + "/operations/"
	responseTag = "[Otto_FleetManager]Missions/Triggers/lastResponse"
	ottoLogger = system.util.getLogger("OTTO_API_Logger")

	ottoLogger.info("Canceling mission(s) for robot [{}]".format(robotName))

	try:
		robotIdPath = "[Otto_FleetManager]Robots/{}/id".format(robotName)
		try:
			robotIdValue = readRequiredTagValue(robotIdPath, "Robot ID")
		except ValueError:
			msg = "Robot [{}] has no valid id tag".format(robotName)
			ottoLogger.warn(msg)
			writeTagValueAsync(responseTag, msg)
			return _buildResult(ok=False, level="warn", message=msg)

		robot_id = str(robotIdValue).strip().lower()
		missionRecords = _readActiveMissionRecords()
		result = cancelMissionsFromInputs(
			robot_id,
			missionRecords,
			fleetManagerURL,
			httpPost
		)

		if result.get("response_texts"):
			writeTagValueAsync("[Otto_FleetManager]System/lastResponse", result["response_texts"][-1])

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeTagValueAsync(responseTag, result["message"])
		return result

	except Exception as e:
		msg = "Error canceling mission(s) for robot [{}]: {}".format(robotName, str(e))
		ottoLogger.error(msg)
		writeTagValueAsync(responseTag, msg)
		return _buildResult(ok=False, level="error", message=msg)


def cancelAllActiveMissions():
	"""
	Cancels all known active missions currently present under [Otto_FleetManager]Missions/Active.
	"""
	fleetManagerURL = readRequiredTagValue(
		"[Otto_FleetManager]Url_ApiBase",
		"API base URL"
	) + "/operations/"
	responseTag = "[Otto_FleetManager]Missions/Triggers/lastResponse"
	ottoLogger = system.util.getLogger("OTTO_API_Logger")

	ottoLogger.info("Canceling all active missions")

	try:
		missionRecords = _readActiveMissionRecords()
		result = cancelAllActiveMissionsFromInputs(
			missionRecords,
			fleetManagerURL,
			httpPost
		)

		if result.get("response_texts"):
			writeTagValueAsync("[Otto_FleetManager]System/lastResponse", result["response_texts"][-1])

		if result["level"] == "info":
			ottoLogger.info(result["message"])
		elif result["level"] == "warn":
			ottoLogger.warn(result["message"])
		else:
			ottoLogger.error(result["message"])

		writeTagValueAsync(responseTag, result["message"])
		return result

	except Exception as e:
		msg = "Error canceling all active missions: {}".format(str(e))
		ottoLogger.error(msg)
		writeTagValueAsync(responseTag, msg)
		return _buildResult(ok=False, level="error", message=msg)
