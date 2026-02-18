def handleTimerEvent():
	try:
	    if system.tag.read("[Otto_FleetManager]System/ServerStatus").value == "RUNNING":
	        Otto_API.Get.updateSystemStates()
	        Otto_API.Get.updateActivityStates()
	        Otto_API.Get.updateChargeLevels()
	        Otto_API.RobotReadiness.updateAvailableForWork()
	    else:
	        ottoLogger = system.util.getLogger("Otto_API_Logger")
	        ottoLogger.warn("Otto Server State is not capable of updating Vehicle Status")
	except Exception as e:
	    ottoLogger = system.util.getLogger("Otto_API_Logger")
	    ottoLogger.error("Error checking server status: " + str(e))