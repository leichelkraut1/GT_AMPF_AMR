def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Missions/Create/create_WF4_RV3", False)
		Otto_API.Services.Missions.createMission(
			templateTagPath="[Otto_FleetManager]Fleet/Workflows/WF4_TableTransfer/jsonString",
			robotTagPath="[Otto_FleetManager]Fleet/Robots/AMPF_AMR_RV3/ID",
			missionName="Tabletop Transfer with RV3"
		)