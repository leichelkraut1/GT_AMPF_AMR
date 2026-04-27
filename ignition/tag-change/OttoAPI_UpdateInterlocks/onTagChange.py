def onTagChange(initialChange, newValue, previousValue, event, executionCount):
    if newValue.value == True:
        system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/updateInterlocks", 0)
        Otto_API.Interlocks.Runtime.runInterlockSyncCycle()
