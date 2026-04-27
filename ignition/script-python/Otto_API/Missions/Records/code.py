from Otto_API.Common.RecordHelpers import MappingRecordBase
from Otto_API.Common.RecordHelpers import RawBackedRecordBase
from Otto_API.Common.RecordHelpers import coerceUpperText
from Otto_API.Common.RecordHelpers import coerceText


TERMINAL_MISSION_STATUSES = set([
    "CANCELLED",
    "SUCCEEDED",
    "REVOKED",
    "FAILED",
])

ACTIVE_MISSION_STATUSES = set([
    "QUEUED",
    "ASSIGNED",
    "EXECUTING",
    "STARVED",
    "CANCELLING",
    "REASSIGNED",
    "RESTARTING",
    "BLOCKED",
])

ACTIVE_MISSION_STATUS_PRIORITY = {
    "STARVED": 0,
    "EXECUTING": 10,
    "ASSIGNED": 20,
    "BLOCKED": 30,
    "CANCELLING": 40,
    "REASSIGNED": 50,
    "RESTARTING": 60,
    "QUEUED": 100,
}


class MissionRecord(RawBackedRecordBase):
    FIELDS = (
        "id",
        "name",
        "mission_status",
        "assigned_robot",
        "force_robot",
    )
    RAW_FIELD_ALIASES = {
        "name": ("mission_name",),
        "assigned_robot": ("Assigned_Robot",),
        "force_robot": ("forced_robot", "Force_Robot", "Forced_Robot"),
    }

    def __init__(self, missionId, name, missionStatus, assignedRobot=None, forceRobot=None, rawData=None):
        self.id = coerceText(missionId)
        self.name = coerceText(name)
        self.mission_status = coerceUpperText(missionStatus, "")
        self.assigned_robot = coerceText(assignedRobot)
        self.force_robot = coerceText(forceRobot)
        RawBackedRecordBase.__init__(self, rawData)

    @classmethod
    def fromDict(cls, mission):
        mission = dict(mission or {})
        return cls(
            mission.get("id"),
            mission.get("name") or mission.get("mission_name"),
            mission.get("mission_status"),
            mission.get("assigned_robot") or mission.get("Assigned_Robot"),
            mission.get("force_robot") or mission.get("forced_robot") or mission.get("Force_Robot") or mission.get("Forced_Robot"),
            rawData=mission,
        )

    def isTerminal(self):
        return self.mission_status in TERMINAL_MISSION_STATUSES

    def isActive(self):
        return self.mission_status in ACTIVE_MISSION_STATUSES

    def assignedRobotId(self):
        for value in [self.assigned_robot, self.force_robot]:
            if str(value or "").strip():
                return str(value).strip().lower()
        return None

    def matchesRobotId(self, robotId):
        normalizedRobotId = str(robotId or "").strip().lower()
        if not normalizedRobotId:
            return False
        return self.assignedRobotId() == normalizedRobotId

    def activeStatusPriority(self):
        return ACTIVE_MISSION_STATUS_PRIORITY.get(self.mission_status, 90)

    def activeSortKey(self):
        return (
            self.activeStatusPriority(),
            str(self.get("path") or self.get("instance_path") or ""),
            self.name,
            self.id,
        )

    def currentMissionProjection(self):
        return {
            "current_mission_name": self.name,
            "current_mission_id": self.id,
            "current_mission_status": self.mission_status,
        }


class RobotMissionSummary(MappingRecordBase):
    FIELDS = (
        "active_mission_count",
        "failed_mission_count",
        "mission_starved",
        "mission_ready_for_attachment",
        "current_mission_name",
        "current_mission_id",
        "current_mission_status",
    )

    def __init__(self):
        self.active_mission_count = 0
        self.failed_mission_count = 0
        self.mission_starved = False
        self.mission_ready_for_attachment = False
        self.current_mission_name = ""
        self.current_mission_id = ""
        self.current_mission_status = ""

    def recordActiveMission(self, missionRecord, attachmentState=None):
        self.active_mission_count += 1
        attachmentState = dict(attachmentState or {})
        if attachmentState.get("mission_starved") is True:
            self.mission_starved = True
        if attachmentState.get("ready_for_attachment") is True:
            self.mission_ready_for_attachment = True

    def recordFailedMission(self, missionRecord):
        self.failed_mission_count += 1

    def setCurrentMission(self, missionRecord):
        if missionRecord is None:
            return
        projection = missionRecord.currentMissionProjection()
        self.current_mission_name = projection["current_mission_name"]
        self.current_mission_id = projection["current_mission_id"]
        self.current_mission_status = projection["current_mission_status"]

    def fleetMissionCountValues(self):
        return (
            int(self.active_mission_count or 0),
            int(self.failed_mission_count or 0),
        )

    def toFleetRobotMissionCountWrites(self, basePath):
        activeMissionCount, failedMissionCount = self.fleetMissionCountValues()
        return (
            [
                str(basePath or "") + "/ActiveMissionCount",
                str(basePath or "") + "/FailedMissionCount",
            ],
            [
                activeMissionCount,
                failedMissionCount,
            ],
        )
