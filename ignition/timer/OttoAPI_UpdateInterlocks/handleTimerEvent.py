def handleTimerEvent():
    """Handle the periodic OTTO interlocks sync."""
    import Otto_API.TagSync.Interlocks.Runtime

    Otto_API.TagSync.Interlocks.Runtime.runInterlockSyncCycle()
