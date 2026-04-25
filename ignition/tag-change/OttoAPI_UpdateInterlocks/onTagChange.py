def onTagChange(initialChange, newValue, previousValue, event, executionCount):
    """Handle Ignition tag-change event for the manual interlock update trigger."""
    import Otto_API.Interlocks.Runtime

    if bool(initialChange):
        return

    if bool(getattr(newValue, "value", False)):
        system.tag.writeAsync("[Otto_FleetManager]Fleet/Triggers/SystemUpdates/updateInterlocks", 0)
        Otto_API.Interlocks.Runtime.runInterlockSyncCycle()
