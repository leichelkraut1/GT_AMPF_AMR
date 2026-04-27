from Otto_API.Fleet.Records import RobotReadinessResult
from Otto_API.Robots.Records import RobotSnapshot


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
    return RobotReadinessResult(
        robotName,
        available,
        reason,
        systemState,
        activityState,
        chargeLevel,
        minCharge,
        activeMissionCount,
        failedMissionCount,
        chargingTof,
        chargingTs,
        chargingDelayMs,
        missionLastUpdateTs,
        missionLastUpdateSuccess,
    )


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
        return _buildRobotReadinessResult(
            robotName=baseResult.robot_name,
            available=available,
            reason=reason,
            systemState=baseResult.system_state,
            activityState=baseResult.activity_state,
            chargeLevel=baseResult.charge_level,
            minCharge=baseResult.min_charge,
            activeMissionCount=baseResult.active_mission_count,
            failedMissionCount=baseResult.failed_mission_count,
            chargingTof=baseResult.charging_tof,
            chargingTs=baseResult.charging_ts,
            chargingDelayMs=baseResult.charging_delay_ms,
            missionLastUpdateTs=baseResult.mission_last_update_ts,
            missionLastUpdateSuccess=baseResult.mission_last_update_success,
        )

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
    ).isReady()


def _reasonToTagValue(readiness):
    if readiness.isReady():
        return ""
    return readiness.notReadyReason()


def _coerceRobotSnapshot(snapshot):
    if isinstance(snapshot, RobotSnapshot):
        return snapshot
    return RobotSnapshot.fromDict(snapshot)


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
        snapshot = _coerceRobotSnapshot(snapshot)
        readiness = evaluateRobotReadiness(
            snapshot.robot_name,
            snapshot.system_state,
            snapshot.activity_state,
            snapshot.charge_level,
            minCharge,
            snapshot.active_mission_count,
            missionLastUpdateTs,
            missionLastUpdateSuccess,
            snapshot.failed_mission_count,
            snapshot.charging_tof,
            snapshot.charging_ts,
            chargingDelayMs,
        )
        robotResults.append(readiness.toDict())

        robotPath = snapshot.robot_path
        if robotPath:
            writePaths.extend([
                robotPath + "/AvailableForWork",
                robotPath + "/NotReadyReason",
            ])
            writeValues.extend([
                readiness.isReady(),
                _reasonToTagValue(readiness),
            ])

    return {
        "robot_results": robotResults,
        "write_paths": writePaths,
        "write_values": writeValues,
    }
