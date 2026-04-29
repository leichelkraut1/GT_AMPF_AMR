def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Missions/Create/create_WF1_RV3", False)
		Otto_API.Services.Missions.Operations.createMission(
			templateTagPath="[Otto_FleetManager]Fleet/Workflows/WF1_PrimusService/jsonString",
			robotTagPath="[Otto_FleetManager]Fleet/Robots/AMPF_AMR_RV3/ID",
			missionName="Service Primus with RV3"
		)