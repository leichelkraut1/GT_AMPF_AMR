import json
import time
import uuid
from Otto_API.Get import sanitizeTagName

now = system.date.now

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
	fleetManagerURL = system.tag.read("[Otto_FleetManager]Url_ApiBase").value + "/operations/"
	responseTag = "[Otto_FleetManager]Missions/Triggers/lastResponse"
	ottoLogger = system.util.getLogger("OTTO_API_Logger")
	
	ottoLogger.info("Posting mission from template [{}] for robot [{}]".format(templateTagPath, robotTagPath))

	try:
		# --- Read required tags ---
		tagResults = system.tag.readBlocking([robotTagPath, templateTagPath])
		robot_id = tagResults[0].value
		template_json_str = tagResults[1].value

		# --- Parse template JSON ---
		try:
			template = json.loads(template_json_str)
			if not isinstance(template, dict):
				raise ValueError("Template JSON was returned as invalid")
		except Exception as e:
			msg = "Invalid template: {}".format(str(e))
			ottoLogger.error(msg)
			system.tag.writeAsync(responseTag, msg)
			return

		tasks = template.get("tasks", [])
		if not isinstance(tasks, list):
			ottoLogger.warn("Template 'tasks' field missing or not a list; using empty list")
			tasks = []

		mission_priority = template.get("priority", 100)

		# --- Build mission payload ---
		mission_payload = {
			"id": int(time.time()),
			"jsonrpc": "2.0",
			"method": "createMission",
			"params": {
				"mission": {
					"client_reference_id": str(uuid.uuid4()),
					"description": missionName + " - " + sanitizeTagName(str(time.time())),
					"finalized": False,
					"force_robot": robot_id,
					"force_team": None,
					"max_duration": "0",
					"metadata": "",
					"name": missionName + " - " + sanitizeTagName(str(time.time())),
					"nominal_duration": "0",
					"priority": mission_priority
				},
				"tasks": tasks
			}
		}

		json_body = system.util.jsonEncode(mission_payload)

		# --- Send Post request ---
		response = system.net.httpPost(
			url=fleetManagerURL,
			postData=json_body,
			contentType="application/json",
			headerValues={"Accept":"application/json"},
			bypassCertValidation=True
		)
		system.tag.writeAsync("[Otto_FleetManager]System/lastResponse", response)

		# --- Parse response ---
		lastResponse = ""
		try:
			resp_json = json.loads(response)
			if "result" in resp_json:
				result = resp_json["result"]
				mission_id = result.get("uuid") or result.get("id")
				if mission_id:
					lastResponse = "Mission created successfully - ID: {}".format(mission_id)
					ottoLogger.info(lastResponse)
				else:
					lastResponse = "Mission created, but no UUID found: {}".format(json.dumps(result))
					ottoLogger.warn(lastResponse)
			elif "error" in resp_json:
				lastResponse = "API Error: {}".format(json.dumps(resp_json["error"]))
				ottoLogger.warn(lastResponse)
			else:
				lastResponse = "Unexpected response: {}".format(response)
				ottoLogger.warn(lastResponse)
		except Exception as e:
			lastResponse = "Fleet Manager returned non-JSON response: {}".format(str(e))
			ottoLogger.error(lastResponse)

	except Exception as e:
		msg = "Error posting mission: {}".format(str(e))
		ottoLogger.error(msg)


import json
import time

import json
import time

def finalizeMission(robotName):
	"""
	Finalizes the active mission currently assigned to the specified robot by
	posting an updateMission JSON-RPC request to the OTTO Fleet Manager.

	Args:
		robotName (str): Name of the robot UDT instance under [Otto_FleetManager]Robots
	"""

	# --- Config ---
	fleetManagerURL = system.tag.read("[Otto_FleetManager]Url_ApiBase").value + "/operations/"
	responseTag = "[Otto_FleetManager]Missions/Triggers/lastResponse"
	ottoLogger = system.util.getLogger("OTTO_API_Logger")

	ottoLogger.info("Finalizing mission for robot [{}]".format(robotName))

	try:
		# --- Resolve robot UUID ---
		robotIdPath = "[Otto_FleetManager]Robots/{}/id".format(robotName)
		robotIdResult = system.tag.readBlocking([robotIdPath])[0]

		if not robotIdResult.quality.isGood() or not robotIdResult.value:
			msg = "Robot [{}] has no valid id tag".format(robotName)
			ottoLogger.warn(msg)
			system.tag.writeAsync(responseTag, msg)
			return

		robot_uuid = str(robotIdResult.value).strip().lower()

		# --- Locate active mission assigned to this robot ---
		activeMissionsPath = "[Otto_FleetManager]Missions/Active"
		missionBrowseResults = system.tag.browse(activeMissionsPath).getResults()

		targetMissionUUID = None

		for mission in missionBrowseResults:
			missionBasePath = str(mission.get("fullPath"))

			assignedRobotPath = missionBasePath + "/assigned_robot"
			assignedRobotResult = system.tag.readBlocking([assignedRobotPath])[0]

			if not assignedRobotResult.quality.isGood():
				continue

			if not assignedRobotResult.value:
				continue

			assigned_robot_uuid = str(assignedRobotResult.value).strip().lower()

			if assigned_robot_uuid != robot_uuid:
				continue

			# --- Resolve mission identifier (prefer uuid, fallback to id) ---
			uuidPath = missionBasePath + "/uuid"
			idPath = missionBasePath + "/id"

			idResults = system.tag.readBlocking([uuidPath, idPath])

			if idResults[0].quality.isGood() and idResults[0].value:
				targetMissionUUID = str(idResults[0].value)
			elif idResults[1].quality.isGood() and idResults[1].value:
				targetMissionUUID = str(idResults[1].value)
			else:
				ottoLogger.warn(
					"Matched mission for robot [{}], but no uuid/id found at [{}]".format(
						robotName, missionBasePath
					)
				)

			break

		if not targetMissionUUID:
			msg = "No active mission found for robot [{}] (robot_uuid={})".format(
				robotName, robot_uuid
			)
			ottoLogger.info(msg)
			system.tag.writeAsync(responseTag, msg)
			return

		ottoLogger.info(
			"Found active mission [{}] for robot [{}]".format(
				targetMissionUUID, robotName
			)
		)

		# --- Build updateMission payload ---
		mission_payload = {
			"id": int(time.time()),
			"jsonrpc": "2.0",
			"method": "updateMission",
			"params": {
				"append_tasks": [],
				"fields": {
					"finalized": True
				},
				"id": targetMissionUUID
			}
		}

		json_body = system.util.jsonEncode(mission_payload)

		# --- Send POST request ---
		response = system.net.httpPost(
			url=fleetManagerURL,
			postData=json_body,
			contentType="application/json",
			headerValues={"Accept": "application/json"},
			bypassCertValidation=True
		)

		# --- Parse response ---
		try:
			resp_json = json.loads(response)

			if "result" in resp_json:
				msg = "Mission [{}] finalized successfully".format(targetMissionUUID)
				ottoLogger.info(msg)
				system.tag.writeAsync(responseTag, msg)

			elif "error" in resp_json:
				msg = "API Error while finalizing mission: {}".format(
					json.dumps(resp_json["error"])
				)
				ottoLogger.warn(msg)
				system.tag.writeAsync(responseTag, msg)

			else:
				msg = "Unexpected response while finalizing mission: {}".format(response)
				ottoLogger.warn(msg)
				system.tag.writeAsync(responseTag, msg)

		except Exception as e:
			msg = "Non-JSON response while finalizing mission: {}".format(str(e))
			ottoLogger.error(msg)
			system.tag.writeAsync(responseTag, msg)

	except Exception as e:
		msg = "Error finalizing mission for robot [{}]: {}".format(robotName, str(e))
		ottoLogger.error(msg)
		system.tag.writeAsync(responseTag, msg)