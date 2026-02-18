def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Create/create_WF1_RV4", False)
		Otto_API.Post.createMission(
			templateTagPath="[Otto_FleetManager]Workflows/WF1_PrimusService/jsonString",
			robotTagPath="[Otto_FleetManager]Robots/AMPF_AMR_RV4/ID",
			missionName="Service Primus with RV4"
		)