from Otto_API.Fleet.Records import RobotReadinessContext
from Otto_API.Fleet.Records import RobotReadinessResult
from Otto_API.Robots.Records import RobotSnapshot


DEFAULT_ALLOWED_ACTIVITY_STATES = set([
    "PARKING",
    "IDLE",
    "WAITING"
])


def _coerceRobotSnapshot(snapshot):
    if isinstance(snapshot, RobotSnapshot):
        return snapshot
    return RobotSnapshot.fromDict(snapshot)


def _coerceReadinessContext(
    context=None,
    minCharge=None,
    chargingDelayMs=None,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None,
    allowedActivityStates=None
):
    if isinstance(context, RobotReadinessContext):
        return context

    if isinstance(context, dict):
        return RobotReadinessContext.fromDict(context)

    return RobotReadinessContext(
        minCharge,
        chargingDelayMs,
        missionLastUpdateTs,
        missionLastUpdateSuccess,
        DEFAULT_ALLOWED_ACTIVITY_STATES if allowedActivityStates is None else allowedActivityStates,
    )


def evaluateRobotReadiness(snapshot, context):
    """
    Evaluate one robot snapshot and return a structured readiness result.
    """
    snapshot = _coerceRobotSnapshot(snapshot)
    context = _coerceReadinessContext(context)

    reason = "available"
    available = True

    if context.min_charge is None:
        reason = "min_charge_missing"
        available = False
    elif snapshot.system_state is None:
        reason = "system_state_missing"
        available = False
    elif snapshot.activity_state is None:
        reason = "activity_state_missing"
        available = False
    elif snapshot.charge_level is None:
        reason = "charge_level_missing"
        available = False
    elif not context.mission_last_update_success:
        reason = "mission_data_not_successful"
        available = False
    elif context.mission_last_update_ts is None or not str(context.mission_last_update_ts).strip():
        reason = "mission_data_missing_timestamp"
        available = False
    elif snapshot.system_state != "RUN":
        reason = "system_state_not_run"
        available = False
    elif snapshot.activity_state not in context.allowed_activity_states:
        reason = "activity_state_not_allowed"
        available = False
    elif snapshot.charge_level < context.min_charge:
        reason = "charge_below_minimum"
        available = False
    elif snapshot.isCharging():
        reason = "recently_charging"
        available = False
    elif snapshot.active_mission_count is None:
        reason = "active_mission_count_missing"
        available = False
    elif snapshot.active_mission_count != 0:
        reason = "active_missions_present"
        available = False
    elif snapshot.failed_mission_count is None:
        reason = "failed_mission_count_missing"
        available = False
    elif snapshot.failed_mission_count != 0:
        reason = "failed_missions_present"
        available = False

    return RobotReadinessResult.fromSnapshot(snapshot, context, available, reason)


def isRobotAvailable(snapshot, context):
    """
    Convenience helper for callers that only need the final availability flag.
    """
    return evaluateRobotReadiness(snapshot, context).isReady()


def buildReadinessResultsAndWrites(
    robotSnapshots,
    minCharge,
    chargingDelayMs,
    missionLastUpdateTs=None,
    missionLastUpdateSuccess=None
):
    """
    Evaluate readiness for explicit robot snapshots and build the tag writes.
    """
    robotResults = []
    writePaths = []
    writeValues = []
    context = RobotReadinessContext(
        minCharge,
        chargingDelayMs,
        missionLastUpdateTs,
        missionLastUpdateSuccess,
        DEFAULT_ALLOWED_ACTIVITY_STATES,
    )

    for snapshot in list(robotSnapshots or []):
        snapshot = _coerceRobotSnapshot(snapshot)
        readiness = evaluateRobotReadiness(snapshot, context)
        robotResults.append(readiness.toDict())

        robotPath = snapshot.robot_path
        if robotPath:
            writePaths.extend([
                robotPath + "/AvailableForWork",
                robotPath + "/NotReadyReason",
            ])
            writeValues.extend([
                readiness.isReady(),
                readiness.notReadyReason(),
            ])

    return {
        "robot_results": robotResults,
        "write_paths": writePaths,
        "write_values": writeValues,
    }
