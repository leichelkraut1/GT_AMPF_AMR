def handleTimerEvent():
	system.tag.writeBlocking(["[Otto_FleetManager]PLC/PLC_IgnitionHeartBeat"], [1])