def handleTimerEvent():
	"""Handle Ignition timer event for this resource."""
	Otto_API.Missions.MissionSorting.runTerminalMaintenance()
	Otto_API.Containers.Post.cleanupContainersWithoutLocation()
