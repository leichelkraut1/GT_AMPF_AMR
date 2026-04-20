def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Containers/UpdateContainer1ToPlace1", False)
		Otto_API.Containers.Post.UpdatePlaceById(
			Otto_API.TriggerHelpers.CONTAINER1_ID,
			Otto_API.TriggerHelpers.PLACE1_ID
		)
