def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	system.tag.writeAsync("[default]DemoMode/RV2_RunDemo", newValue)
