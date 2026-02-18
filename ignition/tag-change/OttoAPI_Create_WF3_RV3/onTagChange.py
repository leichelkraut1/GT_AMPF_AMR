def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Create/create_WF3_RV3", False)
		Otto_API.Post.createMission(
			templateTagPath="[Otto_FleetManager]Workflows/WF3_WetLabTransfer/jsonString",
			robotTagPath="[Otto_FleetManager]Robots/AMPF_AMR_RV3/ID",
			missionName="Wet Lab Transfer with RV3"
		)