from Otto_API.AttachmentPhaseHelpers import deriveMissionAttachmentState
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagIO import writeRequiredTagValues
from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.TagPaths import getMissionLastUpdateSuccessPath
from Otto_API.Common.TagPaths import getMissionLastUpdateTsPath
from Otto_API.Common.TagPaths import getMissionMaxCompletedCountPath
from Otto_API.Missions.Buckets import classify_mission_bucket
from Otto_API.Missions.Buckets import make_instance_name
from Otto_API.Missions.Buckets import readRobotFolderMappings
from Otto_API.Missions.Buckets import resolve_mission_robot_folder
from Otto_API.Missions.MissionActions import selectCurrentActiveMissionRecord
from Otto_API.Missions.Get import getMissions
from Otto_API.Missions.Maintenance import cleanup_stale_bucket
from Otto_API.Missions.Maintenance import cleanup_terminal_folder
from Otto_API.Missions.Sync import mission_to_tag_values
from Otto_API.Missions.Sync import sync_mission_into_bucket
from Otto_API.Missions.MissionTreeHelpers import browseMissionInstances


BASE = getFleetMissionsPath()
ACTIVE_PATH = BASE + "/Active"
COMPLETED_PATH = BASE + "/Completed"
FAILED_PATH = BASE + "/Failed"
LAST_UPDATE_TS_PATH = getMissionLastUpdateTsPath()
LAST_UPDATE_SUCCESS_PATH = getMissionLastUpdateSuccessPath()
ROBOTS_PATH = getFleetRobotsPath()

ACTIVE_STATUSES = [
    "QUEUED",
    "ASSIGNED",
    "EXECUTING",
    "STARVED",
    "CANCELLING",
    "REASSIGNED",
    "RESTARTING",
    "BLOCKED"
]

TERMINAL_STATUSES = [
    "CANCELLED",
    "SUCCEEDED",
    "REVOKED"
]

FAILED_STATUSES = [
    "FAILED"
]

MAX_FAILED = 50
COMPLETED_RETENTION_DAYS = 5
FAILED_RETENTION_DAYS = 5

DEBUG_TAG_PATH = BASE + "/DebugEnabled"


def _log():
    """
    Returns the module logger.
    """
    return system.util.getLogger("Otto_API.Missions.MissionSorting")


def _debug_enabled():
    """
    Reads debug enable tag.
    """
    try:
        return bool(readOptionalTagValue(DEBUG_TAG_PATH, False))
    except Exception:
        return False


def _dlog(logger, debug, msg):
    """
    Conditional debug logger.
    """
    if debug:
        logger.info(msg)


def _buildSyncResult(ok, level, message, activeWanted=None, completedWanted=None, failedWanted=None, removed=None, robotSummaryByFolder=None):
    activeWanted = sorted(list(activeWanted or []))
    completedWanted = sorted(list(completedWanted or []))
    failedWanted = sorted(list(failedWanted or []))
    removed = list(removed or [])
    robotSummaryByFolder = dict(robotSummaryByFolder or {})
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "active_wanted": activeWanted,
            "completed_wanted": completedWanted,
            "failed_wanted": failedWanted,
            "removed": removed,
            "robot_summary_by_folder": robotSummaryByFolder,
        },
        active_wanted=activeWanted,
        completed_wanted=completedWanted,
        failed_wanted=failedWanted,
        removed=removed,
        robot_summary_by_folder=robotSummaryByFolder,
    )


def _readMaxCompletedCount():
    return int(readRequiredTagValue(
        getMissionMaxCompletedCountPath(),
        "Mission max completed count"
    ) or 0)


def _writeMissionUpdateStatus(success, timestampValue, logger=None):
    try:
        writeRequiredTagValues(
            [LAST_UPDATE_TS_PATH, LAST_UPDATE_SUCCESS_PATH],
            [timestampValue, bool(success)],
            ["Mission LastUpdateTS", "Mission LastUpdateSuccess"]
        )
    except Exception as exc:
        if logger is not None:
            logger.error("Failed to write mission update status: {}".format(str(exc)))
        raise


def _writeFleetRobotMissionCounts(robotSummaryByFolder, logger=None):
    """Mirror mission-count metadata back onto Fleet/Robots without touching MainControl tags."""
    writePaths = []
    writeValues = []
    for robotFolder, summary in sorted(dict(robotSummaryByFolder or {}).items()):
        basePath = ROBOTS_PATH + "/" + str(robotFolder or "")
        writePaths.extend([
            basePath + "/ActiveMissionCount",
            basePath + "/FailedMissionCount",
        ])
        writeValues.extend([
            int(dict(summary or {}).get("active_mission_count", 0) or 0),
            int(dict(summary or {}).get("failed_mission_count", 0) or 0),
        ])

    if not writePaths:
        return

    try:
        writeRequiredTagValues(
            writePaths,
            writeValues,
            labels=["Fleet robot mission counts"] * len(writePaths)
        )
    except Exception as exc:
        if logger is not None:
            logger.error("Failed to write Fleet robot mission counts: {}".format(str(exc)))
        raise


def run():
    """
    Main mission sorting entry point.
    """
    logger = _log()
    debug = _debug_enabled()

    _dlog(logger, debug, "MissionSorting.run START")

    result = None

    try:
        missions = []
        nowDate = system.date.now()
        nowTimestamp = system.date.format(nowDate, "yyyy-MM-dd HH:mm:ss.SSS")

        missions.extend(
            getMissions(
                logger,
                debug,
                mission_status=ACTIVE_STATUSES
            )
        )

        missions.extend(
            getMissions(
                logger,
                debug,
                mission_status=FAILED_STATUSES,
                limit=MAX_FAILED
            )
        )

        robotMappings = readRobotFolderMappings(robotsPath=ROBOTS_PATH, logger=logger)
        activeWanted = set()
        completedWanted = set()
        failedWanted = set()
        activeCountsByFolder = {}
        failedCountsByFolder = {}
        missionStarvedByFolder = {}
        attachmentReadyByFolder = {}
        attachmentMissionNameByFolder = {}
        currentMissionIdByFolder = {}
        currentMissionStatusByFolder = {}
        activeMissionsByFolder = {}
        removed = []

        for mission in missions:
            status = mission.get("mission_status", "")
            robotFolder = resolve_mission_robot_folder(
                mission,
                robotMappings=robotMappings,
                robotsPath=ROBOTS_PATH,
                logger=logger,
            )
            attachmentState = deriveMissionAttachmentState(mission)

            bucket = classify_mission_bucket(status, TERMINAL_STATUSES, FAILED_STATUSES)
            syncResult = sync_mission_into_bucket(
                mission,
                robotFolder,
                bucket,
                nowTimestamp,
                logger,
                ACTIVE_PATH,
                COMPLETED_PATH,
                FAILED_PATH,
                debug=debug
            )
            removed.extend(syncResult["removed"])

            if bucket == "completed":
                completedWanted.add(syncResult["paths"]["completed"])
            elif bucket == "failed":
                failedWanted.add(syncResult["paths"]["failed"])
                failedCountsByFolder[robotFolder] = failedCountsByFolder.get(robotFolder, 0) + 1
            else:
                activeWanted.add(syncResult["paths"]["active"])
                activeCountsByFolder[robotFolder] = activeCountsByFolder.get(robotFolder, 0) + 1
                activeMissionsByFolder.setdefault(robotFolder, []).append(mission)
                if attachmentState.get("mission_starved") is True:
                    missionStarvedByFolder[robotFolder] = True
                if attachmentState.get("ready_for_attachment") is True:
                    attachmentReadyByFolder[robotFolder] = True
                    attachmentMissionNameByFolder[robotFolder] = str(
                        attachmentState.get("attachment_mission_name") or mission.get("name") or ""
                    )

        for robotFolder, robotMissions in list(dict(activeMissionsByFolder or {}).items()):
            currentMission = selectCurrentActiveMissionRecord(robotMissions)
            if currentMission is None:
                continue
            currentMissionIdByFolder[robotFolder] = str(currentMission.get("id") or "")
            currentMissionStatusByFolder[robotFolder] = str(currentMission.get("mission_status") or "")

        robotSummaryByFolder = {}
        for robotFolder in sorted(robotMappings.get("name_by_lower", {}).values()):
            robotSummaryByFolder[robotFolder] = {
                "active_mission_count": int(activeCountsByFolder.get(robotFolder, 0) or 0),
                "failed_mission_count": int(failedCountsByFolder.get(robotFolder, 0) or 0),
                "mission_starved": bool(missionStarvedByFolder.get(robotFolder, False)),
                "mission_ready_for_attachment": bool(attachmentReadyByFolder.get(robotFolder, False)),
                "current_mission_name": str(attachmentMissionNameByFolder.get(robotFolder, "") or ""),
                "current_mission_id": str(currentMissionIdByFolder.get(robotFolder, "") or ""),
                "current_mission_status": str(currentMissionStatusByFolder.get(robotFolder, "") or ""),
            }

        _writeFleetRobotMissionCounts(robotSummaryByFolder, logger)

        removed.extend(
            cleanup_stale_bucket(
                ACTIVE_PATH,
                activeWanted,
                "active",
                logger,
                browseMissionInstances,
                debug=debug,
                nowTimestamp=nowTimestamp
            )
        )

        removed.extend(
            cleanup_stale_bucket(
                FAILED_PATH,
                failedWanted,
                "failed",
                logger,
                browseMissionInstances,
                debug=debug,
                nowTimestamp=nowTimestamp
            )
        )
        removed.extend(
            cleanup_terminal_folder(
                FAILED_PATH,
                FAILED_RETENTION_DAYS,
                MAX_FAILED,
                "failed",
                logger,
                browseMissionInstances,
                debug=debug,
                protectedPaths=failedWanted
            )
        )
        _writeMissionUpdateStatus(True, nowTimestamp, logger)
        result = _buildSyncResult(
            True,
            "info",
            "Mission sorting completed for {} mission(s)".format(len(missions)),
            activeWanted=activeWanted,
            completedWanted=completedWanted,
            failedWanted=failedWanted,
            removed=removed,
            robotSummaryByFolder=robotSummaryByFolder,
        )

    except Exception as e:
        try:
            _writeMissionUpdateStatus(
                False,
                system.date.format(system.date.now(), "yyyy-MM-dd HH:mm:ss.SSS"),
                logger
            )
        except Exception:
            logger.error("MissionSorting.run also failed to write failure status tags")
        logger.error("MissionSorting.run FAILED: {}".format(e))
        result = _buildSyncResult(
            False,
            "error",
            "Mission sorting failed: {}".format(e)
        )

    _dlog(logger, debug, "MissionSorting.run END")
    return result


def runTerminalMaintenance():
    """
    Slower maintenance pass for completed missions.
    """
    logger = _log()
    debug = _debug_enabled()

    _dlog(logger, debug, "MissionSorting.runTerminalMaintenance START")

    try:
        maxCompleted = _readMaxCompletedCount()
        nowDate = system.date.now()
        nowTimestamp = system.date.format(nowDate, "yyyy-MM-dd HH:mm:ss.SSS")
        robotMappings = readRobotFolderMappings(robotsPath=ROBOTS_PATH, logger=logger)
        completedWanted = set()
        removed = []

        missions = getMissions(
            logger,
            debug,
            mission_status=TERMINAL_STATUSES,
            limit=maxCompleted
        )

        for mission in missions:
            robotFolder = resolve_mission_robot_folder(
                mission,
                robotMappings=robotMappings,
                robotsPath=ROBOTS_PATH,
                logger=logger,
            )
            syncResult = sync_mission_into_bucket(
                mission,
                robotFolder,
                "completed",
                nowTimestamp,
                logger,
                ACTIVE_PATH,
                COMPLETED_PATH,
                FAILED_PATH,
                debug=debug
            )
            completedWanted.add(syncResult["paths"]["completed"])
            removed.extend(syncResult["removed"])

        removed.extend(
            cleanup_stale_bucket(
                COMPLETED_PATH,
                completedWanted,
                "completed",
                logger,
                browseMissionInstances,
                debug=debug,
                nowTimestamp=nowTimestamp
            )
        )
        removed.extend(
            cleanup_terminal_folder(
                COMPLETED_PATH,
                COMPLETED_RETENTION_DAYS,
                maxCompleted,
                "completed",
                logger,
                browseMissionInstances,
                debug=debug,
                protectedPaths=completedWanted
            )
        )

        result = _buildSyncResult(
            True,
            "info",
            "Completed mission maintenance processed {} mission(s)".format(len(missions)),
            completedWanted=completedWanted,
            removed=removed
        )
    except Exception as exc:
        logger.error("MissionSorting.runTerminalMaintenance FAILED: {}".format(exc))
        result = _buildSyncResult(
            False,
            "error",
            "Completed mission maintenance failed: {}".format(exc)
        )

    _dlog(logger, debug, "MissionSorting.runTerminalMaintenance END")
    return result
