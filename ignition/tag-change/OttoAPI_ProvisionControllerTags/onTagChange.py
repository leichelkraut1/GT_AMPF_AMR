def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if system.tag.read("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/ProvisionControllerTags").value:
	    import MainController.State.Provisioning
	    system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/ProvisionControllerTags", 0)
	    MainController.State.Provisioning.ensureControllerTags()
