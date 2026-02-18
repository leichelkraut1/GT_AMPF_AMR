def updateAvailableForWork():
    """
    Evaluates SystemState, ActivityState, and ChargeLevel for each robot
    and sets /AvailableForWork based on mission eligibility rules.

    Availability rules:
      - SystemState must be "RUN"
      - ActivityState must be in ALLOWED_ACTIVITY_STATES
      - ChargeLevel >= minChargeLevelForMissioning
    """
    ottoLogger = system.util.getLogger("Otto_Logic_Logger")

    robotsBasePath = "[Otto_FleetManager]Robots"
    minChargePath = "[Otto_FleetManager]Missions/minChargeLevelForMissioning"

    ALLOWED_ACTIVITY_STATES = set([
        "PARKING",
        "IDLE",
        "WAITING"
    ])

    try:
        minCharge = system.tag.read(minChargePath).value
        if minCharge is None:
            ottoLogger.warn("minChargeLevelForMissioning is None")
            return

        browseResults = system.tag.browse(robotsBasePath).getResults()

        for tag in browseResults:
            if str(tag["tagType"]) != "UdtInstance":
                continue

            robotPath = robotsBasePath + "/" + tag["name"]

            systemStatePath = robotPath + "/SystemState"
            activityPath = robotPath + "/ActivityState"
            chargePath = robotPath + "/ChargeLevel"
            availablePath = robotPath + "/AvailableForWork"

            try:
                reads = system.tag.readBlocking([
                    systemStatePath,
                    activityPath,
                    chargePath
                ])

                systemState = reads[0].value
                activityState = reads[1].value
                chargeLevel = reads[2].value

                available = False

                if (
                    systemState is not None and
                    activityState is not None and
                    chargeLevel is not None
                ):
                    systemState = str(systemState).upper()
                    activityState = str(activityState).upper()

                    if (
                        systemState == "RUN" and
                        activityState in ALLOWED_ACTIVITY_STATES and
                        chargeLevel >= minCharge
                    ):
                        available = True

                system.tag.writeBlocking([availablePath], [available])

            except Exception as e:
                ottoLogger.warn(
                    "Failed to evaluate AvailableForWork for " +
                    tag["name"] + " - " + str(e)
                )

    except Exception as e:
        ottoLogger.error(
            "AvailableForWork evaluation failed - " + str(e)
        )