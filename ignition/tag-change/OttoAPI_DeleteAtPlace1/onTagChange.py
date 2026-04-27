def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Containers/DeleteAtPlace1", False)
		Otto_API.Containers.Post.deleteContainersAtPlace(
			Otto_API.TriggerHelpers.readContainerTriggerPlaceId()
		)