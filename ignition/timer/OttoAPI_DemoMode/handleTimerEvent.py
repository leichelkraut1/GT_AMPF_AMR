def handleTimerEvent():
	ottoLogger = system.util.getLogger("Otto_DemoMode")
	ottoLogger.info("Running Demo.run Cycle")
	try:
	    if system.tag.read("[Otto_FleetManager]DemoMode/ModeActive").value == True:
	        Otto_API.Demo.run()
	        if system.tag.read("[Otto_FleetManager]DemoMode/AutoFinalizeEnabled").value == True:
	        	Otto_API.Demo.finalizeStarvedMissions()
	except Exception as e:
	    ottoLogger.error("Demo Mode Failed to Run: " + str(e))