def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	system.tag.writeAsync("[Otto_FleetManager]DemoMode/AttachmentPhase/RV2/DemoComplete", newValue)
