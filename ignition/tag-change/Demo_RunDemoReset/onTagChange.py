def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]DemoMode/AttachmentPhase/RV2/RunDemo", False)