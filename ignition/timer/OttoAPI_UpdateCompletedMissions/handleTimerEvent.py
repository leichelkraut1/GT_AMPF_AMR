def handleTimerEvent():
	"""Handle Ignition timer event for this resource."""
	Otto_API.Services.Missions.Sync.runTerminalMaintenance()
	Otto_API.Services.Containers.cleanupContainersWithoutLocation()
