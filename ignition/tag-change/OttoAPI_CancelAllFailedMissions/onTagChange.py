def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Cancel/cancelAllFailedMissions", False)
		Otto_API.Missions.Post.cancelAllFailedMissions()
