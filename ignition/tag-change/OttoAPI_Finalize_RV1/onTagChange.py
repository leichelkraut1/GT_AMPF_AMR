def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Missions/Finalize/finalize_RV1", False)
		Otto_API.Missions.RobotCommands.finalizeActiveMissionForRobot(
			robotName="AMPF_AMR_RV1"
		)
