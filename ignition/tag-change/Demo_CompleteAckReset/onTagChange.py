def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if newValue.value == False:
		system.tag.writeAsync("[Otto_FleetManager]DemoMode/AttachmentPhase/RV2/DemoCompleteAck", [False])