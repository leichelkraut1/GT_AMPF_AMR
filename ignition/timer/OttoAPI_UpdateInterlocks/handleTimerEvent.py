def handleTimerEvent():
    """Handle the periodic OTTO interlocks sync."""
    import Otto_API.Interlocks.Runtime

    Otto_API.Interlocks.Runtime.runInterlockSyncCycle()
