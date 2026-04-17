def handleTimerEvent():
    """Handle Ignition timer event for the pilot mission command loop."""
    try:
        MainController.MainLoop.runPilotCreateWF1RV1()
    except Exception as e:
        logger = system.util.getLogger("MainController_MainLoop")
        logger.error("Pilot command loop failed - " + str(e))
