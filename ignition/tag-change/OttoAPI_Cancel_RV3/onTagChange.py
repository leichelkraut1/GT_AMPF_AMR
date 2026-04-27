def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Missions/Cancel/cancel_RV3", False)
		Otto_API.Missions.RobotCommands.cancelActiveMissionsForRobot(
			robotName="AMPF_AMR_RV3"
		)