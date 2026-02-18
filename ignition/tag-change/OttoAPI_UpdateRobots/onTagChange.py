def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if system.tag.read("[Otto_FleetManager]Triggers/SystemUpdates/updateRobots").value == True:
	    system.tag.writeAsync("[Otto_FleetManager]Triggers/SystemUpdates/updateRobots", 0)  
	    Otto_API.Get.updateRobots()	