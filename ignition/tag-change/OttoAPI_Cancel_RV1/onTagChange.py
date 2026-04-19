def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Cancel/cancel_RV1", False)
		Otto_API.Missions.Post.cancelMission(
			robotName="AMPF_AMR_RV1"
		)
