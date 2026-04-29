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


def _coerceWorkflowNumber(value):
    try:
        number = int(value)
    except Exception:
        return None
    return number if number > 0 else None


class MissionRecord(RawBackedRecordBase):
    FIELDS = (
        "id",
        "name",
        "mission_status",
        "assigned_robot",
        "force_robot",
        "workflow_number",
        "instance_path",
        "path",
    )
    RAW_FIELD_ALIASES = {
        "name": ("mission_name",),
        "assigned_robot": ("Assigned_Robot",),
        "force_robot": ("forced_robot", "Force_Robot", "Forced_Robot"),
    }

    def __init__(
        self,
        missionId,
        name,
        missionStatus,
        assignedRobot=None,
        forceRobot=None,
        workflowNumber=None,
        instancePath="",
        path="",
        rawData=None
    ):
        self.id = coerceText(missionId)
        self.name = coerceText(name)
        self.mission_status = coerceUpperText(missionStatus, "")
        self.assigned_robot = coerceText(assignedRobot)
        self.force_robot = coerceText(forceRobot)
        self.workflow_number = _coerceWorkflowNumber(workflowNumber)
        self.instance_path = coerceText(instancePath)
        self.path = coerceText(path)
        RawBackedRecordBase.__init__(self, rawData)

    @classmethod
    def fromDict(cls, mission):
        if isinstance(mission, cls):
            return mission
        mission = dict(mission or {})
        return cls(
            mission.get("id"),
            mission.get("name") or mission.get("mission_name"),
            mission.get("mission_status"),
            mission.get("assigned_robot") or mission.get("Assigned_Robot"),
            mission.get("force_robot")
            or mission.get("forced_robot")
            or mission.get("Force_Robot")
            or mission.get("Forced_Robot"),
            mission.get("workflow_number"),
            mission.get("instance_path", ""),
            mission.get("path", ""),
            rawData=mission,
        )

    @classmethod
    def listFromDicts(cls, missions):
        return [cls.fromDict(mission) for mission in list(missions or [])]

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
        missionStatus = str(self.mission_status or "")
        return ACTIVE_MISSION_STATUS_PRIORITY.get(missionStatus, 90)

    def activeSortKey(self):
        return (
            self.activeStatusPriority(),
            self.path or self.instance_path,
            self.name,
            self.id,
        )

    def currentMissionProjection(self):
        return {
            "current_mission_name": self.name,
            "current_mission_id": self.id,
            "current_mission_status": self.mission_status,
        }


def _matchingMissionRecordsForRobot(robotId, missionRecords):
    return [
        missionRecord
        for missionRecord in MissionRecord.listFromDicts(missionRecords)
        if missionRecord.matchesRobotId(robotId)
    ]


def resolveMissionRobotId(missionRecord):
    missionRecord = MissionRecord.fromDict(missionRecord)
    return missionRecord.assignedRobotId()


def activeMissionStatusPriority(missionStatus):
    missionRecord = MissionRecord.fromDict({"mission_status": missionStatus})
    return missionRecord.activeStatusPriority()


def sortActiveMissionRecords(missionRecords):
    return sorted(
        MissionRecord.listFromDicts(missionRecords),
        key=lambda missionRecord: missionRecord.activeSortKey(),
    )


def selectCurrentActiveMissionRecord(missionRecords):
    ordered = sortActiveMissionRecords(missionRecords)
    if not ordered:
        return None
    return ordered[0]


def findActiveMissionIdForRobot(robotId, missionRecords):
    for missionRecord in sortActiveMissionRecords(_matchingMissionRecordsForRobot(robotId, missionRecords)):
        missionId = missionRecord.id
        if missionId:
            return (str(missionId), None)

        return (
            None,
            "Matched mission for robot ID [{}], but no mission id found".format(robotId)
        )

    return (None, None)


def findActiveMissionIdsForRobot(robotId, missionRecords):
    missionIds = []
    warnings = []

    for missionRecord in _matchingMissionRecordsForRobot(robotId, missionRecords):
        missionId = missionRecord.id
        if missionId:
            missionIds.append(str(missionId))
        else:
            warnings.append(
                "Matched mission for robot ID [{}], but no mission id found".format(robotId)
            )

    return (missionIds, warnings)


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
        if attachmentState is None:
            attachmentState = {}
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
