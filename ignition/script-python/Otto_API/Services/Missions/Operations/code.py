from Otto_API.Common.OperationHelpers import logOperationResult
from Otto_API.Common.TagIO import getOttoOperationsUrl
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Models.Missions import MissionRecord
from Otto_API.Models.Results import OperationalResult
from Otto_API.TagSync.Missions.Tree import readMissionIdRecords
from Otto_API.WebAPI.Missions.Commands import parseTemplateJson
from Otto_API.WebAPI.Missions.Commands import postCancelMissions
from Otto_API.WebAPI.Missions.Commands import postCreateMission
from Otto_API.WebAPI.Missions.Commands import postFinalizeMission


ACTIVE_MISSIONS_ROOT = getFleetMissionsPath() + "/Active"
FAILED_MISSIONS_ROOT = getFleetMissionsPath() + "/Failed"


def _log():
    return system.util.getLogger("Otto_API.Services.Missions.Operations")


class MissionOperationResult(OperationalResult):
    def __init__(self, ok, level, message, missionId=None, responseText=None, payload=None):
        self.mission_id = missionId
        self.response_text = responseText
        self.payload = payload
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            dataFields={
                "mission_id": self.mission_id,
                "response_text": self.response_text,
                "payload": self.payload,
            },
        )


def _buildResult(ok, level, message, missionId=None, responseText=None, payload=None):
    return MissionOperationResult(
        ok,
        level,
        message,
        missionId=missionId,
        responseText=responseText,
        payload=payload,
    ).toDict()


def _writeAndLogMissionResult(result, logger):
    return logOperationResult(result, logger)


def createMission(templateTagPath, robotTagPath, missionName):
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Posting mission from template [{}] for robot [{}]".format(templateTagPath, robotTagPath))

    try:
        try:
            robot_id = str(readRequiredTagValue(robotTagPath, "Robot ID"))
            template_json_str = str(readRequiredTagValue(templateTagPath, "Template"))
        except ValueError as e:
            msg = str(e)
            ottoLogger.error(msg)
            return _buildResult(ok=False, level="error", message=msg)

        try:
            template = parseTemplateJson(template_json_str)
        except ValueError as e:
            msg = "Invalid template: {}".format(str(e))
            ottoLogger.error(msg)
            return _buildResult(ok=False, level="error", message=msg)

        if not isinstance(template.get("tasks", []), list):
            ottoLogger.warn("Template 'tasks' field missing or not a list; using empty list")

        result = postCreateMission(
            fleetManagerURL,
            template,
            robot_id,
            missionName
        ).toDict()
        return _writeAndLogMissionResult(result, ottoLogger)

    except Exception as e:
        msg = "Error posting mission: {}".format(str(e))
        ottoLogger.error(msg)
        return _buildResult(ok=False, level="error", message=msg)


def finalizeMissionId(missionId):
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Finalizing mission [{}]".format(missionId))

    result = postFinalizeMission(
        fleetManagerURL,
        missionId
    ).toDict()
    return _writeAndLogMissionResult(result, ottoLogger)


def cancelMissionIds(missionIds):
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Canceling explicit mission id list [{}]".format(list(missionIds or [])))

    result = postCancelMissions(
        fleetManagerURL,
        missionIds,
        emptyWarnMessage="No explicit mission ids found to cancel",
        successMessage="Canceled {} explicit mission(s)",
        errorMessage="Error canceling explicit missions: {}",
    ).toDict()
    return _writeAndLogMissionResult(result, ottoLogger)


def cancelAllActiveMissions():
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Canceling all active missions")

    try:
        missionRecords = readMissionIdRecords(ACTIVE_MISSIONS_ROOT)
        targetMissionIds = [
            str(missionRecord.id)
            for missionRecord in MissionRecord.listFromDicts(missionRecords)
            if missionRecord.id
        ]
        result = postCancelMissions(
            fleetManagerURL,
            targetMissionIds,
            emptyWarnMessage="No active missions found to cancel",
            successMessage="Canceled {} active mission(s)",
            errorMessage="Error canceling active missions: {}",
        ).toDict()
        return _writeAndLogMissionResult(result, ottoLogger)

    except Exception as e:
        msg = "Error canceling all active missions: {}".format(str(e))
        ottoLogger.error(msg)
        return _buildResult(ok=False, level="error", message=msg)


def cancelAllFailedMissions():
    fleetManagerURL = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Canceling all failed missions")

    try:
        missionRecords = readMissionIdRecords(FAILED_MISSIONS_ROOT)
        targetMissionIds = [
            str(missionRecord.id)
            for missionRecord in MissionRecord.listFromDicts(missionRecords)
            if missionRecord.id
        ]
        result = postCancelMissions(
            fleetManagerURL,
            targetMissionIds,
            emptyWarnMessage="No failed missions found to cancel",
            successMessage="Canceled {} failed mission(s)",
            errorMessage="Error canceling failed missions: {}",
        ).toDict()
        return _writeAndLogMissionResult(result, ottoLogger)

    except Exception as e:
        msg = "Error canceling all failed missions: {}".format(str(e))
        ottoLogger.error(msg)
        return _buildResult(ok=False, level="error", message=msg)
