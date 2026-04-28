def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Containers/CreateContainer1", False)
		Otto_API.Services.Containers.createContainerAtPlace(
			Otto_API.TriggerHelpers.readContainerTriggerTemplatePath(),
			Otto_API.TriggerHelpers.readContainerTriggerPlaceId()
		)
