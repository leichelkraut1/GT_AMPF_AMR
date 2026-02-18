def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	system.tag.writeAsync("[default]DemoMode/RV2_DemoCompleteAck", newValue)