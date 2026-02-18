def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	system.tag.writeAsync("[Otto_FleetManager]DemoMode/AttachmentPhase/RV2/DemoRunning", newValue)