def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if system.tag.read("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/UpdateAndCleanPLCTags").value == True:
	    import MainController.State.PlcMappingStore
	    system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/UpdateAndCleanPLCTags", 0)
	    MainController.State.PlcMappingStore.syncPlcFleetTags()
