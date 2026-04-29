def handleTimerEvent():
    """Handle the periodic OTTO interlocks sync."""
    Otto_API.Services.Interlocks.PlcSync.runInterlockSyncCycle()
