def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Create/create_WF2_RV1", False)
		Otto_API.Post.createMission(
			templateTagPath="[Otto_FleetManager]Workflows/WF2_ServiceXPC/jsonString",
			robotTagPath="[Otto_FleetManager]Robots/AMPF_AMR_RV1/ID",
			missionName="Service XPC with RV1"
		)
