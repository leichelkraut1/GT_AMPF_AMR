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
    minCharge=None,
    activeMissionCount=None,
    failedMissionCount=None,
    chargingTof=None,
    chargingTs=None,
    chargingDelayMs=None,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None
):
    return {
        "robot_name": robotName,
        "available": available,
        "reason": reason,
        "system_state": systemState,
        "activity_state": activityState,
        "charge_level": chargeLevel,
        "min_charge": minCharge,
        "active_mission_count": activeMissionCount,
        "failed_mission_count": failedMissionCount,
        "charging_tof": chargingTof,
        "charging_ts": chargingTs,
        "charging_delay_ms": chargingDelayMs,
        "mission_last_update_ts": missionLastUpdateTs,
        "mission_last_update_success": missionLastUpdateSuccess,
    }


def evaluateRobotReadiness(
    robotName,
    systemState,
    activityState,
    chargeLevel,
    minCharge,
    activeMissionCount=None,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None,
    failedMissionCount=None,
    chargingTof=None,
    chargingTs=None,
    chargingDelayMs=None,
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

    baseResult = _buildRobotReadinessResult(
        robotName=robotName,
        available=False,
        reason="not_evaluated",
        systemState=normalizedSystemState,
        activityState=normalizedActivityState,
        chargeLevel=chargeLevel,
        minCharge=minCharge,
        activeMissionCount=activeMissionCount,
        failedMissionCount=failedMissionCount,
        chargingTof=chargingTof,
        chargingTs=chargingTs,
        chargingDelayMs=chargingDelayMs,
        missionLastUpdateTs=missionLastUpdateTs,
        missionLastUpdateSuccess=missionLastUpdateSuccess
    )

    def _result(reason, available=False):
        result = dict(baseResult)
        result["reason"] = reason
        result["available"] = bool(available)
        return result

    if minCharge is None:
        return _result("min_charge_missing")

    if normalizedSystemState is None:
        return _result("system_state_missing")

    if normalizedActivityState is None:
        return _result("activity_state_missing")

    if chargeLevel is None:
        return _result("charge_level_missing")

    if not missionLastUpdateSuccess:
        return _result("mission_data_not_successful")

    if missionLastUpdateTs is None or not str(missionLastUpdateTs).strip():
        return _result("mission_data_missing_timestamp")

    if normalizedSystemState != "RUN":
        return _result("system_state_not_run")

    if normalizedActivityState not in normalizedAllowedStates:
        return _result("activity_state_not_allowed")

    if chargeLevel < minCharge:
        return _result("charge_below_minimum")

    if chargingTof:
        return _result("recently_charging")

    if activeMissionCount is None:
        return _result("active_mission_count_missing")

    if activeMissionCount != 0:
        return _result("active_missions_present")

    if failedMissionCount is None:
        return _result("failed_mission_count_missing")

    if failedMissionCount != 0:
        return _result("failed_missions_present")

    return _result("available", available=True)


def isRobotAvailable(
    systemState,
    activityState,
    chargeLevel,
    minCharge,
    activeMissionCount=None,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None,
    failedMissionCount=None,
    chargingTof=None,
    chargingTs=None,
    chargingDelayMs=None,
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
        activeMissionCount,
        missionLastUpdateTs,
        missionLastUpdateSuccess,
        failedMissionCount,
        chargingTof,
        chargingTs,
        chargingDelayMs,
        allowedActivityStates
    )["available"]
def _reasonToTagValue(readiness):
    if readiness.get("available"):
        return ""
    return str(readiness.get("reason") or "")


def buildReadinessResultsAndWrites(
    robotSnapshots,
    minCharge,
    chargingDelayMs,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None
):
    """
    Evaluate readiness for explicit robot snapshots and build the tag writes.
    Each snapshot should include:
    - robot_name
    - robot_path
    - system_state
    - activity_state
    - charge_level
    - active_mission_count
    - failed_mission_count
    - charging_tof
    - charging_ts
    """
    robotResults = []
    writePaths = []
    writeValues = []

    for snapshot in list(robotSnapshots or []):
        readiness = evaluateRobotReadiness(
            snapshot.get("robot_name"),
            snapshot.get("system_state"),
            snapshot.get("activity_state"),
            snapshot.get("charge_level"),
            minCharge,
            snapshot.get("active_mission_count"),
            missionLastUpdateTs,
            missionLastUpdateSuccess,
            snapshot.get("failed_mission_count"),
            snapshot.get("charging_tof"),
            snapshot.get("charging_ts"),
            chargingDelayMs,
        )
        robotResults.append(readiness)

        robotPath = snapshot.get("robot_path")
        if robotPath:
            writePaths.extend([
                robotPath + "/AvailableForWork",
                robotPath + "/NotReadyReason",
            ])
            writeValues.extend([
                readiness["available"],
                _reasonToTagValue(readiness),
            ])

    return {
        "robot_results": robotResults,
        "write_paths": writePaths,
        "write_values": writeValues,
    }
