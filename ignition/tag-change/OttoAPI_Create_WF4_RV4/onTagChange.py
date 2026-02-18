def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Create/create_WF4_RV4", False)
		Otto_API.Post.createMission(
			templateTagPath="[Otto_FleetManager]Workflows/WF4_TableTransfer/jsonString",
			robotTagPath="[Otto_FleetManager]Robots/AMPF_AMR_RV4/ID",
			missionName="Tabletop Transfer with RV4"
		)