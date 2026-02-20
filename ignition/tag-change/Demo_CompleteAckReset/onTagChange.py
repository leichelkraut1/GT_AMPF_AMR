def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == False:
		system.tag.writeAsync("[Otto_FleetManager]DemoMode/AttachmentPhase/RV2/DemoCompleteAck", [False])
