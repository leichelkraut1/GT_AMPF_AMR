def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if system.tag.read("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/updateRobots").value == True:
	    system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/updateRobots", 0)  
	    Otto_API.Robots.Get.updateRobots()	
