from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import coerceBool
from Otto_API.Common.RecordHelpers import coerceFloatOrNone
from Otto_API.Common.RecordHelpers import coerceIntOrNone
from Otto_API.Common.RecordHelpers import coerceText
from Otto_API.Common.RecordHelpers import coerceUpperText


def _normalizedActivityStates(allowedActivityStates):
    normalized = []
    seen = set()
    for value in list(allowedActivityStates or []):
        text = coerceUpperText(value, None)
        if text is None or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


class RobotReadinessContext(MappingRecordBase):
    FIELDS = (
        "min_charge",
        "charging_delay_ms",
        "mission_last_update_ts",
        "mission_last_update_success",
        "allowed_activity_states",
    )

    def __init__(
        self,
        minCharge,
        chargingDelayMs=None,
        missionLastUpdateTs=None,
        missionLastUpdateSuccess=None,
        allowedActivityStates=None,
    ):
        self.min_charge = coerceFloatOrNone(minCharge)
        self.charging_delay_ms = coerceIntOrNone(chargingDelayMs)
        self.mission_last_update_ts = coerceText(missionLastUpdateTs, None)
        self.mission_last_update_success = coerceBool(missionLastUpdateSuccess, False)
        self.allowed_activity_states = _normalizedActivityStates(allowedActivityStates)

    @classmethod
    def fromDict(cls, record):
        record = dict(record or {})
        return cls(
            record.get("min_charge"),
            record.get("charging_delay_ms"),
            record.get("mission_last_update_ts"),
            record.get("mission_last_update_success"),
            record.get("allowed_activity_states"),
        )


class RobotReadinessResult(MappingRecordBase):
    FIELDS = (
        "robot_name",
        "available",
        "reason",
        "system_state",
        "activity_state",
        "charge_level",
        "min_charge",
        "active_mission_count",
        "failed_mission_count",
        "charging_tof",
        "charging_ts",
        "charging_delay_ms",
        "mission_last_update_ts",
        "mission_last_update_success",
    )

    def __init__(
        self,
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
        missionLastUpdateSuccess=None,
    ):
        self.robot_name = coerceText(robotName)
        self.available = coerceBool(available, False)
        self.reason = coerceText(reason)
        self.system_state = coerceText(systemState, None)
        self.activity_state = coerceText(activityState, None)
        self.charge_level = coerceFloatOrNone(chargeLevel)
        self.min_charge = coerceFloatOrNone(minCharge)
        self.active_mission_count = coerceIntOrNone(activeMissionCount)
        self.failed_mission_count = coerceIntOrNone(failedMissionCount)
        self.charging_tof = coerceBool(chargingTof, False)
        self.charging_ts = coerceIntOrNone(chargingTs)
        self.charging_delay_ms = coerceIntOrNone(chargingDelayMs)
        self.mission_last_update_ts = coerceText(missionLastUpdateTs, None)
        self.mission_last_update_success = coerceBool(missionLastUpdateSuccess, False)

    @classmethod
    def fromDict(cls, record):
        record = dict(record or {})
        return cls(
            record.get("robot_name"),
            record.get("available"),
            record.get("reason"),
            record.get("system_state"),
            record.get("activity_state"),
            record.get("charge_level"),
            record.get("min_charge"),
            record.get("active_mission_count"),
            record.get("failed_mission_count"),
            record.get("charging_tof"),
            record.get("charging_ts"),
            record.get("charging_delay_ms"),
            record.get("mission_last_update_ts"),
            record.get("mission_last_update_success"),
        )

    @classmethod
    def fromSnapshot(cls, snapshot, context, available, reason):
        return cls(
            snapshot.robot_name,
            available,
            reason,
            snapshot.system_state,
            snapshot.activity_state,
            snapshot.charge_level,
            context.min_charge,
            snapshot.active_mission_count,
            snapshot.failed_mission_count,
            snapshot.charging_tof,
            snapshot.charging_ts,
            context.charging_delay_ms,
            context.mission_last_update_ts,
            context.mission_last_update_success,
        )

    def isReady(self):
        return bool(self.available)

    def notReadyReason(self):
        if self.isReady():
            return ""
        return str(self.reason or "")
