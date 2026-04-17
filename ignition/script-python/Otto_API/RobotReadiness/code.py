DEFAULT_ALLOWED_ACTIVITY_STATES = set([
    "PARKING",
    "IDLE",
    "WAITING"
])


def _normalizeStateValue(value):
    if value is None:
        return None
    return str(value).strip().upper()


def _buildRobotReadinessResult(
    robotName,
    available,
    reason,
    systemState=None,
    activityState=None,
    chargeLevel=None,
    minCharge=None
):
    return {
        "robot_name": robotName,
        "available": available,
        "reason": reason,
        "system_state": systemState,
        "activity_state": activityState,
        "charge_level": chargeLevel,
        "min_charge": minCharge,
    }


def evaluateRobotReadiness(
    robotName,
    systemState,
    activityState,
    chargeLevel,
    minCharge,
    allowedActivityStates=None
):
    """
    Evaluates a single robot snapshot from explicit inputs and returns
    a structured result describing mission readiness.
    """
    if allowedActivityStates is None:
        allowedActivityStates = DEFAULT_ALLOWED_ACTIVITY_STATES

    normalizedSystemState = _normalizeStateValue(systemState)
    normalizedActivityState = _normalizeStateValue(activityState)
    normalizedAllowedStates = set([
        _normalizeStateValue(value) for value in allowedActivityStates
        if value is not None
    ])

    if minCharge is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "min_charge_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge
        )

    if normalizedSystemState is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "system_state_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge
        )

    if normalizedActivityState is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "activity_state_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge
        )

    if chargeLevel is None:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "charge_level_missing",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge
        )

    if normalizedSystemState != "RUN":
        return _buildRobotReadinessResult(
            robotName,
            False,
            "system_state_not_run",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge
        )

    if normalizedActivityState not in normalizedAllowedStates:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "activity_state_not_allowed",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge
        )

    if chargeLevel < minCharge:
        return _buildRobotReadinessResult(
            robotName,
            False,
            "charge_below_minimum",
            normalizedSystemState,
            normalizedActivityState,
            chargeLevel,
            minCharge
        )

    return _buildRobotReadinessResult(
        robotName,
        True,
        "available",
        normalizedSystemState,
        normalizedActivityState,
        chargeLevel,
        minCharge
    )


def isRobotAvailable(
    systemState,
    activityState,
    chargeLevel,
    minCharge,
    allowedActivityStates=None
):
    """
    Convenience helper for callers that only need the final availability flag.
    """
    return evaluateRobotReadiness(
        None,
        systemState,
        activityState,
        chargeLevel,
        minCharge,
        allowedActivityStates
    )["available"]


def _buildUpdateResult(ok, level, message, minCharge=None, robotResults=None):
    robotResults = list(robotResults or [])
    return {
        "ok": ok,
        "level": level,
        "message": message,
        "min_charge": minCharge,
        "robots_evaluated": len(robotResults),
        "robots_available": len([
            result for result in robotResults if result.get("available")
        ]),
        "robot_results": robotResults,
    }


def updateAvailableForWork():
    """
    Evaluates SystemState, ActivityState, and ChargeLevel for each robot
    and sets /AvailableForWork based on mission eligibility rules.
    """
    ottoLogger = system.util.getLogger("Otto_Logic_Logger")

    robotsBasePath = "[Otto_FleetManager]Robots"
    minChargePath = "[Otto_FleetManager]Missions/minChargeLevelForMissioning"

    try:
        minCharge = system.tag.read(minChargePath).value
        if minCharge is None:
            message = "minChargeLevelForMissioning is None"
            ottoLogger.warn(message)
            return _buildUpdateResult(False, "warn", message)

        browseResults = system.tag.browse(robotsBasePath).getResults()
        robotResults = []

        for tag in browseResults:
            if str(tag["tagType"]) != "UdtInstance":
                continue

            robotName = str(tag["name"])
            robotPath = robotsBasePath + "/" + robotName

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

                readiness = evaluateRobotReadiness(
                    robotName,
                    reads[0].value,
                    reads[1].value,
                    reads[2].value,
                    minCharge
                )
                robotResults.append(readiness)
                system.tag.writeBlocking([availablePath], [readiness["available"]])

            except Exception as e:
                ottoLogger.warn(
                    "Failed to evaluate AvailableForWork for " +
                    robotName + " - " + str(e)
                )

        message = "AvailableForWork updated for {} robot(s)".format(
            len(robotResults)
        )
        return _buildUpdateResult(True, "info", message, minCharge, robotResults)

    except Exception as e:
        message = "AvailableForWork evaluation failed - " + str(e)
        ottoLogger.error(message)
        return _buildUpdateResult(False, "error", message)
