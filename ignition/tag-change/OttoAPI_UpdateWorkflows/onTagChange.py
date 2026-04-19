def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if system.tag.read("[Otto_FleetManager]Triggers/SystemUpdates/updateWorkflows").value == True:
	    system.tag.writeAsync("[Otto_FleetManager]Triggers/SystemUpdates/updateWorkflows", 0)
	    Otto_API.Fleet.Get.updateWorkflows()
