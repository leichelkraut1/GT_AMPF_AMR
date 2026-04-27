def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if system.tag.read("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/updateTriggers").value == True:
	    system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/updateTriggers", 0)
	    Otto_API.TriggerHelpers.ensureMissionTriggerTags()