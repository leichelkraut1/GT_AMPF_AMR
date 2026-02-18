def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if system.tag.read("[Otto_FleetManager]Triggers/SystemUpdates/updatePlaces").value == True:
	    system.tag.writeAsync("[Otto_FleetManager]Triggers/SystemUpdates/updatePlaces", 0)
	    Otto_API.Get.updatePlaces()
	