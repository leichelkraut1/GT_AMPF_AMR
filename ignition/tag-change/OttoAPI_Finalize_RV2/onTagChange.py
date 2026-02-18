def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if newValue.value == True:
		system.tag.writeAsync("[Otto_FleetManager]Triggers/Missions/Finalize/finalize_RV2", False)
		Otto_API.Post.finalizeMission(
			robotName="AMPF_AMR_RV2"
		)