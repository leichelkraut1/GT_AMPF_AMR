from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import coerceBool
from Otto_API.Common.RecordHelpers import coerceFloatOrNone
from Otto_API.Common.RecordHelpers import coerceInt
from Otto_API.Common.RecordHelpers import coerceIntOrNone
from Otto_API.Common.RecordHelpers import coerceText
from Otto_API.Common.RecordHelpers import coerceUpperText
from Otto_API.Common.TimeHelpers import parseIsoTimestampToEpochMillis


class RobotSystemStateEntry(MappingRecordBase):
    FIELDS = (
        "robot",
        "priority",
        "created",
        "system_state",
        "sub_system_state",
    )

    def __init__(self, robotId, priority, created, systemState, subSystemState):
        self.robot = coerceText(robotId)
        self.priority = coerceInt(priority, 9999)
        self.created = coerceText(created)
        self.system_state = coerceText(systemState, None)
        self.sub_system_state = coerceText(subSystemState, None)

    @classmethod
    def fromDict(cls, entry):
        if isinstance(entry, cls):
            return entry
        entry = dict(entry or {})
        return cls(
            entry.get("robot"),
            entry.get("priority", 9999),
            entry.get("created"),
            entry.get("system_state"),
            entry.get("sub_system_state"),
        )

    def createdSortValue(self, logger=None):
        if not self.created:
            return 0
        try:
            return parseIsoTimestampToEpochMillis(self.created)
        except Exception as exc:
            if logger is not None:
                logger.warn(
                    "Falling back to digit-only mission created sort for [{}]: {}".format(
                        self.created,
                        str(exc)
                    )
                )
            digits = "".join([ch for ch in self.created if ch.isdigit()])
            if not digits:
                return 0
            try:
                return int(digits)
            except Exception:
                return 0


class RobotPlace(MappingRecordBase):
    FIELDS = ("place_id", "place_name")

    def __init__(self, placeId, placeName):
        self.place_id = coerceText(placeId)
        self.place_name = coerceText(placeName)

    @classmethod
    def fromDict(cls, record):
        if isinstance(record, cls):
            return record
        record = dict(record or {})
        return cls(
            record.get("place_id") or record.get("id"),
            record.get("place_name") or record.get("name"),
        )

    @classmethod
    def empty(cls):
        return cls("", "")


class RobotSnapshot(MappingRecordBase):
    FIELDS = (
        "robot_name",
        "robot_path",
        "system_state",
        "activity_state",
        "charge_level",
        "active_mission_count",
        "failed_mission_count",
        "place_id",
        "place_name",
        "charging_tof",
        "charging_ts",
    )

    def __init__(
        self,
        robotName,
        robotPath,
        systemState,
        activityState,
        chargeLevel,
        activeMissionCount,
        failedMissionCount,
        placeId,
        placeName,
        chargingTof,
        chargingTs,
    ):
        self.robot_name = coerceText(robotName)
        self.robot_path = coerceText(robotPath)
        self.system_state = coerceUpperText(systemState, None)
        self.activity_state = coerceUpperText(activityState, None)
        self.charge_level = coerceFloatOrNone(chargeLevel)
        self.active_mission_count = coerceIntOrNone(activeMissionCount)
        self.failed_mission_count = coerceIntOrNone(failedMissionCount)
        self.place_id = coerceText(placeId)
        self.place_name = coerceText(placeName)
        self.charging_tof = coerceBool(chargingTof, False)
        self.charging_ts = coerceIntOrNone(chargingTs)

    @classmethod
    def fromDict(cls, snapshot):
        if isinstance(snapshot, cls):
            return snapshot
        snapshot = dict(snapshot or {})
        return cls(
            snapshot.get("robot_name"),
            snapshot.get("robot_path"),
            snapshot.get("system_state"),
            snapshot.get("activity_state"),
            snapshot.get("charge_level"),
            snapshot.get("active_mission_count"),
            snapshot.get("failed_mission_count"),
            snapshot.get("place_id"),
            snapshot.get("place_name"),
            snapshot.get("charging_tof"),
            snapshot.get("charging_ts"),
        )

    def isCharging(self):
        return bool(self.charging_tof)

    def currentPlace(self):
        return RobotPlace(self.place_id, self.place_name)

    def withPlace(self, place):
        if place is None:
            return self.cloneWith(place_id="", place_name="")
        if not isinstance(place, RobotPlace):
            place = RobotPlace.fromDict(place)
        return self.cloneWith(
            place_id=place.place_id,
            place_name=place.place_name,
        )

    def withChargingState(self, chargingTof, chargingTs):
        return self.cloneWith(
            charging_tof=chargingTof,
            charging_ts=chargingTs,
        )

    def withUpdatedOperationalState(
        self,
        systemState=None,
        activityState=None,
        chargeLevel=None,
        activeMissionCount=None,
        failedMissionCount=None,
        place=None,
        chargingTof=None,
        chargingTs=None,
    ):
        updates = {
            "system_state": self.system_state if systemState is None else systemState,
            "activity_state": self.activity_state if activityState is None else activityState,
            "charge_level": self.charge_level if chargeLevel is None else chargeLevel,
            "active_mission_count": self.active_mission_count if activeMissionCount is None else activeMissionCount,
            "failed_mission_count": self.failed_mission_count if failedMissionCount is None else failedMissionCount,
            "charging_tof": self.charging_tof if chargingTof is None else chargingTof,
            "charging_ts": self.charging_ts if chargingTs is None else chargingTs,
        }
        if place is None:
            updates["place_id"] = self.place_id
            updates["place_name"] = self.place_name
        else:
            if not isinstance(place, RobotPlace):
                place = RobotPlace.fromDict(place)
            updates["place_id"] = place.place_id
            updates["place_name"] = place.place_name
        return self.cloneWith(**updates)
