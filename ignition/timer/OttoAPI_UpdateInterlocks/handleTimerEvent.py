def handleTimerEvent():
    """Handle the periodic OTTO interlocks sync."""
    Otto_API.Interlocks.Runtime.runInterlockSyncCycle()
