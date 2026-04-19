def handleTimerEvent():
    """Handle Ignition timer event for the MainController workflow runner."""
    try:
        MainController.MainLoop.runMainControllerCycle()
    except Exception as e:
        logger = system.util.getLogger("MainController_MainLoop")
        logger.error("MainController workflow runner failed - " + str(e))
