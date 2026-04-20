def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Containers/DeleteContainer1", False)
		Otto_API.Containers.Post.DeleteById(
			Otto_API.TriggerHelpers.readContainerTriggerContainerId()
		)
