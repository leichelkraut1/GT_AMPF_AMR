def handleTimerEvent():
	"""Handle Ignition timer event for this resource."""
	ottoLogger = system.util.getLogger("Otto_API.Robots.Get")
	try:
	    if system.tag.read("[Otto_FleetManager]Fleet/System/ServerStatus").value == "RUNNING":
	        Otto_API.Robots.Get.updateRobotOperationalState()
	    else:
	        ottoLogger.warn("Otto Server State is not capable of updating Vehicle Status")
	except Exception as e:
	    ottoLogger.error("Error checking server status: " + str(e))
