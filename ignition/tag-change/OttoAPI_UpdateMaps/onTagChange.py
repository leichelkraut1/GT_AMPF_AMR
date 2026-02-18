def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if system.tag.read("[Otto_FleetManager]Triggers/SystemUpdates/updateMaps").value == True:
	    system.tag.writeAsync("[Otto_FleetManager]Triggers/SystemUpdates/updateMaps", 0)
	    Otto_API.Get.updateMaps()