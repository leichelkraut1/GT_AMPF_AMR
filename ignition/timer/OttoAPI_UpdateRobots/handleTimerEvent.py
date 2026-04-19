def handleTimerEvent():
	"""Handle Ignition timer event for this resource."""
	try:
	    if system.tag.read("[Otto_FleetManager]System/ServerStatus").value == "RUNNING":
	        Otto_API.Fleet.Get.updateRobotOperationalState()
	    else:
	        ottoLogger = system.util.getLogger("Otto_API_Logger")
	        ottoLogger.warn("Otto Server State is not capable of updating Vehicle Status")
	except Exception as e:
	    ottoLogger = system.util.getLogger("Otto_API_Logger")
	    ottoLogger.error("Error checking server status: " + str(e))
