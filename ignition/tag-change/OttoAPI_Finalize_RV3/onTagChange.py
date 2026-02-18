def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Finalize/finalize_RV3", False)
		Otto_API.Post.finalizeMission(
			robotName="AMPF_AMR_RV3"
		)