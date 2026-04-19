def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if system.tag.read("[Otto_FleetManager]Triggers/SystemUpdates/updateRobots").value == True:
	    system.tag.writeAsync("[Otto_FleetManager]Triggers/SystemUpdates/updateRobots", 0)  
	    Otto_API.Fleet.Get.updateRobots()	
