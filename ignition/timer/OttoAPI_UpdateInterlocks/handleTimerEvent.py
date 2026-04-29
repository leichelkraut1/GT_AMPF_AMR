def handleTimerEvent():
    """Handle the periodic OTTO interlocks sync."""
    import Otto_API.Services.Interlocks

    Otto_API.Services.Interlocks.runInterlockSyncCycle()
