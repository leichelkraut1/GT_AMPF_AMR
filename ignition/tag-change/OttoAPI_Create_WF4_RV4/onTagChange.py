def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/Missions/Create/create_WF4_RV4", False)
		Otto_API.Missions.Post.createMission(
			templateTagPath="[Otto_FleetManager]Fleet/Workflows/WF4_TableTransfer/jsonString",
			robotTagPath="[Otto_FleetManager]Fleet/Robots/AMPF_AMR_RV4/ID",
			missionName="Tabletop Transfer with RV4"
		)