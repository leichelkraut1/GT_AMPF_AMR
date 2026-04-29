from Otto_API.AttachmentPhase import deriveMissionAttachmentState
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagIO import writeRequiredTagValues
from Otto_API.Common.TagPaths import getFleetMissionsPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.TagPaths import getMissionLastUpdateSuccessPath
from Otto_API.Common.TagPaths import getMissionLastUpdateTsPath
from Otto_API.Common.TagPaths import getMissionMaxCompletedCountPath
from Otto_API.Missions.MissionActions import selectCurrentActiveMissionRecord
from Otto_API.Models.Missions import RobotMissionSummary
from Otto_API.TagSync.Missions.Buckets import classify_mission_bucket
from Otto_API.TagSync.Missions.Buckets import readRobotFolderMappings
from Otto_API.TagSync.Missions.Buckets import resolve_mission_robot_folder
from Otto_API.TagSync.Missions.Maintenance import cleanup_stale_bucket
from Otto_API.TagSync.Missions.Maintenance import cleanup_terminal_folder
from Otto_API.TagSync.Missions.Sync import mission_to_tag_values
from Otto_API.TagSync.Missions.Sync import sync_mission_into_bucket
from Otto_API.TagSync.Missions.Tree import browseMissionInstances
from Otto_API.WebAPI.Missions import fetchMissions


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


def _buildSyncResult(
    ok,
    level,
    message,
    activeWanted=None,
    completedWanted=None,
    failedWanted=None,
    removed=None,
    robotSummaryByFolder=None,
    issues=None
):
    activeWanted = sorted(list(activeWanted or []))
    completedWanted = sorted(list(completedWanted or []))
    failedWanted = sorted(list(failedWanted or []))
    removed = list(removed or [])
    robotSummaryByFolder = dict(robotSummaryByFolder or {})
    issues = list(issues or [])
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
            "issues": issues,
        },
        active_wanted=activeWanted,
        completed_wanted=completedWanted,
        failed_wanted=failedWanted,
        removed=removed,
        robot_summary_by_folder=robotSummaryByFolder,
        issues=issues,
    )


def _serializeRobotSummaryByFolder(robotSummaryByFolder, robotMappings):
    serialized = {}
    for robotFolder in sorted(robotMappings.get("name_by_lower", {}).values()):
        serialized[robotFolder] = _summaryForRobot(robotSummaryByFolder, robotFolder).toDict()
    return serialized


def _readMaxCompletedCount():
    return int(readRequiredTagValue(
        getMissionMaxCompletedCountPath(),
        "Mission max completed count"
    ) or 0)


def _fetchMissionRecords(logger, debug, missionStatus=None, limit=None, ordering=None):
    result = fetchMissions(
        getApiBaseUrl(),
        missionStatus=missionStatus,
        limit=limit,
        ordering=ordering,
        logger=logger,
        debug=debug,
    )
    return list(result.records or [])


class _MissionSortingFailure(RuntimeError):
    def __init__(self, issueId, message):
        RuntimeError.__init__(self, message)
        self.issue_id = str(issueId or "").strip()
        self.issue_message = str(message or "").strip()


def _buildRunFailureIssues(exc):
    issueId = str(getattr(exc, "issue_id", "") or "").strip()
    issueMessage = str(getattr(exc, "issue_message", "") or "").strip()
    errorText = issueMessage or str(exc or "")
    if issueId == "mission_sorting.update_status_write_failed":
        return [
            buildRuntimeIssue(
                "mission_sorting.update_status_write_failed",
                "Otto_API.Missions.MissionSorting",
                "error",
                errorText,
            )
        ]
    if issueId == "mission_sorting.robot_counts_write_failed":
        return [
            buildRuntimeIssue(
                "mission_sorting.robot_counts_write_failed",
                "Otto_API.Missions.MissionSorting",
                "error",
                errorText,
            )
        ]
    return [
        buildRuntimeIssue(
            "mission_sorting.run_failed",
            "Otto_API.Missions.MissionSorting",
            "error",
            "Mission sorting failed: {}".format(errorText),
        )
    ]


def _writeMissionUpdateStatus(success, timestampValue, logger=None):
    try:
        writeRequiredTagValues(
            [LAST_UPDATE_TS_PATH, LAST_UPDATE_SUCCESS_PATH],
            [timestampValue, bool(success)],
            ["Mission LastUpdateTS", "Mission LastUpdateSuccess"]
        )
    except Exception as exc:
        message = "Failed to write mission update status: {}".format(str(exc))
        if logger is not None:
            logger.error(message)
        raise _MissionSortingFailure("mission_sorting.update_status_write_failed", message)


def _writeFleetRobotMissionCounts(robotSummaryByFolder, robotMappings, logger=None):
    """Mirror mission-count metadata back onto Fleet/Robots without touching MainControl tags."""
    writePaths = []
    writeValues = []
    for robotFolder in sorted(robotMappings.get("name_by_lower", {}).values()):
        summary = _summaryForRobot(robotSummaryByFolder, robotFolder)
        basePath = ROBOTS_PATH + "/" + str(robotFolder or "")
        summaryPaths, summaryValues = summary.toFleetRobotMissionCountWrites(basePath)
        writePaths.extend(summaryPaths)
        writeValues.extend(summaryValues)

    if not writePaths:
        return

    try:
        writeRequiredTagValues(
            writePaths,
            writeValues,
            labels=["Fleet robot mission counts"] * len(writePaths)
        )
    except Exception as exc:
        message = "Failed to write Fleet robot mission counts: {}".format(str(exc))
        if logger is not None:
            logger.error(message)
        raise _MissionSortingFailure("mission_sorting.robot_counts_write_failed", message)


def _summaryForRobot(robotSummaryByFolder, robotFolder):
    summary = robotSummaryByFolder.get(robotFolder)
    if summary is None:
        summary = RobotMissionSummary()
        robotSummaryByFolder[robotFolder] = summary
    return summary


def _recordMissionSyncOutcome(
    mission,
    robotFolder,
    bucket,
    syncResult,
    attachmentState,
    activeWanted,
    completedWanted,
    failedWanted,
    robotSummaryByFolder,
    activeMissionsByFolder,
):
    if bucket == "completed":
        completedWanted.add(syncResult["paths"]["completed"])
        return

    summary = _summaryForRobot(robotSummaryByFolder, robotFolder)
    if bucket == "failed":
        failedWanted.add(syncResult["paths"]["failed"])
        summary.recordFailedMission(mission)
        return

    activeWanted.add(syncResult["paths"]["active"])
    summary.recordActiveMission(mission, attachmentState)
    activeMissionsByFolder.setdefault(robotFolder, []).append(mission)


def _selectCurrentMissionsByRobot(robotSummaryByFolder, activeMissionsByFolder):
    for robotFolder, robotMissions in list(activeMissionsByFolder.items()):
        currentMission = selectCurrentActiveMissionRecord(robotMissions)
        if currentMission is None:
            continue
        _summaryForRobot(robotSummaryByFolder, robotFolder).setCurrentMission(currentMission)


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

        missions.extend(_fetchMissionRecords(
            logger,
            debug,
            missionStatus=ACTIVE_STATUSES
        ) or [])

        missions.extend(_fetchMissionRecords(
            logger,
            debug,
            missionStatus=FAILED_STATUSES,
            limit=MAX_FAILED
        ) or [])

        robotMappings = readRobotFolderMappings(robotsPath=ROBOTS_PATH, logger=logger)
        activeWanted = set()
        completedWanted = set()
        failedWanted = set()
        robotSummaryByFolder = {}
        activeMissionsByFolder = {}
        removed = []

        for mission in missions:
            status = mission.mission_status
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
            _recordMissionSyncOutcome(
                mission,
                robotFolder,
                bucket,
                syncResult,
                attachmentState,
                activeWanted,
                completedWanted,
                failedWanted,
                robotSummaryByFolder,
                activeMissionsByFolder,
            )

        _selectCurrentMissionsByRobot(robotSummaryByFolder, activeMissionsByFolder)

        _writeFleetRobotMissionCounts(robotSummaryByFolder, robotMappings, logger)
        serializedRobotSummaryByFolder = _serializeRobotSummaryByFolder(robotSummaryByFolder, robotMappings)

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
            robotSummaryByFolder=serializedRobotSummaryByFolder,
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
            "Mission sorting failed: {}".format(e),
            issues=_buildRunFailureIssues(e),
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

        missions = list(_fetchMissionRecords(
            logger,
            debug,
            missionStatus=TERMINAL_STATUSES,
            limit=maxCompleted,
            ordering="-created",
        ) or [])

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
            "Completed mission maintenance failed: {}".format(exc),
            issues=[
                buildRuntimeIssue(
                    "mission_sorting.terminal_maintenance_failed",
                    "Otto_API.Missions.MissionSorting",
                    "error",
                    "Completed mission maintenance failed: {}".format(exc),
                )
            ],
        )

    _dlog(logger, debug, "MissionSorting.runTerminalMaintenance END")
    return result
