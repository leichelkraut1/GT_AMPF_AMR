from java.util import Date
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import deleteTagPath
from Otto_API.Common.TagHelpers import ensureUdtInstancePath
from Otto_API.Common.TagHelpers import getMissionLastUpdateSuccessPath
from Otto_API.Common.TagHelpers import getMissionLastUpdateTsPath
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.TagHelpers import writeRequiredTagValues
from Otto_API.Common.TagHelpers import writeTagValues
from Otto_API.AttachmentPhaseHelpers import deriveMissionAttachmentState
from Otto_API.Fleet.ContentSync import sanitizeTagName
from Otto_API.Fleet.FleetSync import parseIsoTimestampToEpochMillis
from Otto_API.Fleet.FleetSync import readRobotInventoryMetadata
from Otto_API.Fleet.Get import getMissions
from Otto_API.Missions.MissionActions import resolveMissionRobotId
from Otto_API.Missions.MissionTreeHelpers import browseMissionInstances

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

BASE = "[Otto_FleetManager]Missions"
ACTIVE_PATH = BASE + "/Active"
COMPLETED_PATH = BASE + "/Completed"
FAILED_PATH = BASE + "/Failed"
LAST_UPDATE_TS_PATH = getMissionLastUpdateTsPath()
LAST_UPDATE_SUCCESS_PATH = getMissionLastUpdateSuccessPath()

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

MAX_COMPLETED = 50
MAX_FAILED = 50
COMPLETED_RETENTION_DAYS = 5
FAILED_RETENTION_DAYS = 5

DEBUG_TAG_PATH = "[Otto_FleetManager]Missions/DebugEnabled"
ROBOTS_PATH = "[Otto_FleetManager]Robots"
UNASSIGNED_FOLDER = "Unassigned"
UNKNOWN_ROBOT_FOLDER = "Unkown_Robot"


# ---------------------------------------------------------------------------
# LOGGING / UTIL
# ---------------------------------------------------------------------------

def _log():
    """
    Returns the module logger
    """
    return system.util.getLogger("Otto_MissionSorting")


def _debug_enabled():
    """
    Reads debug enable tag
    """
    try:
        return bool(readOptionalTagValue(DEBUG_TAG_PATH, False))
    except Exception:
        return False


def _dlog(logger, debug, msg):
    """
    Conditional debug logger
    """
    if debug:
        logger.info(msg)


def parse_date(val):
    """
    Safely parses Ignition Date or string timestamps
    """
    if val is None:
        return None

    if hasattr(val, "before"):
        return val

    text = str(val).strip()

    if "T" in text and (text.endswith("Z") or "+" in text[10:] or "-" in text[10:]):
        try:
            return Date(parseIsoTimestampToEpochMillis(text))
        except Exception:
            pass

    try:
        return system.date.parse(text)
    except Exception:
        return None


def _buildSyncResult(ok, level, message, activeWanted=None, completedWanted=None, failedWanted=None, removed=None):
    activeWanted = sorted(list(activeWanted or []))
    completedWanted = sorted(list(completedWanted or []))
    failedWanted = sorted(list(failedWanted or []))
    removed = list(removed or [])
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "active_wanted": activeWanted,
            "completed_wanted": completedWanted,
            "failed_wanted": failedWanted,
            "removed": removed,
        },
        active_wanted=activeWanted,
        completed_wanted=completedWanted,
        failed_wanted=failedWanted,
        removed=removed,
    )


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


# ---------------------------------------------------------------------------
# TAG HELPERS
# ---------------------------------------------------------------------------

def make_instance_name(mission):
    """
    Creates a readable and mostly-unique mission tag name
    """
    name = sanitizeTagName(mission.get("name"))
    short = mission.get("id", "")[:8]
    return "{}_{}".format(name, short)


def classify_mission_bucket(missionStatus, terminalStatuses=None, failedStatuses=None):
    """
    Classify a mission into the Active, Failed, or Completed bucket.
    """
    if terminalStatuses is None:
        terminalStatuses = TERMINAL_STATUSES
    if failedStatuses is None:
        failedStatuses = FAILED_STATUSES

    status = str(missionStatus or "").upper()
    if status in failedStatuses:
        return "failed"
    if status in terminalStatuses:
        return "completed"
    return "active"


def mission_to_tag_values(mission):
    """
    Convert a mission record into api_Mission field values.
    """
    return {
        "ID": mission.get("id"),
        "Assigned_Robot": mission.get("assigned_robot"),
        "Client_Reference_ID": mission.get("client_reference_id"),
        "Created": mission.get("created"),
        "Current_Task": mission.get("current_task"),
        "Description": mission.get("description"),
        "Due_State": mission.get("due_state"),
        "Execution_End": mission.get("execution_end"),
        "Execution_Start": mission.get("execution_start"),
        "Execution_Time": mission.get("execution_time"),
        "Finalized": mission.get("finalized"),
        "Force_Robot": mission.get("force_robot"),
        "Force_Team": mission.get("force_team"),
        "Max_Duration": mission.get("max_duration"),
        "Metadata": mission.get("metadata"),
        "Mission_Status": mission.get("mission_status"),
        "Name": mission.get("name"),
        "Nominal_Duration": mission.get("nominal_duration"),
        "Paused": mission.get("paused"),
        "Priority": mission.get("priority"),
        "Result_Text": mission.get("result_text"),
        "Result_Text_Intl_Data": mission.get("result_text_intl_data"),
        "Result_Text_Intl_Key": mission.get("result_text_intl_key"),
        "Signature": mission.get("signature"),
        "Structure": mission.get("structure")
    }


def should_remove_completed_by_age(createdDate, cutoff):
    """
    Return True when a completed mission is older than the retention cutoff.
    """
    return bool(createdDate and cutoff and createdDate.before(cutoff))


def compute_completed_overflow(enrichedRows, maxCompleted, nowDate):
    """
    Return the oldest completed rows that exceed the retention count.
    """
    def sort_key(item):
        return item[2] if item[2] else nowDate

    enriched_sorted = sorted(list(enrichedRows or []), key=sort_key)
    if len(enriched_sorted) <= maxCompleted:
        return []
    return enriched_sorted[:-maxCompleted]


def write_mission_data(instancePath, mission):
    """
    Writes mission fields into api_Mission UDT
    """
    values = mission_to_tag_values(mission)

    paths = [instancePath + "/" + k for k in values]
    vals = [values[k] for k in values]

    writeTagValues(paths, vals)


def remove_instance(path, logger=None, debug=False, reason=None):
    """
    Deletes a UDT instance
    """
    try:
        deleteTagPath(path)
        if logger and debug:
            if reason:
                logger.info("Deleted {} ({})".format(path, reason))
            else:
                logger.info("Deleted {}".format(path))
    except Exception:
        pass

def _readRobotFolderMappings():
    """
    Build lookup maps for robot folder names and robot IDs.
    """
    try:
        inventory = readRobotInventoryMetadata(ROBOTS_PATH)
    except Exception:
        return {
            "name_by_lower": {},
            "name_by_id": {},
        }

    return {
        "name_by_lower": dict(inventory.get("robot_name_by_lower", {})),
        "name_by_id": dict(inventory.get("robot_name_by_id", {})),
    }


def build_robot_member_writes(robotMappings, valuesByFolder, memberName, transform=None):
    """
    Build robot-member writes for known robot folders.
    """
    def _identity(value):
        return value

    if transform is None:
        transform = _identity

    writes = []
    for robotFolder in sorted(robotMappings.get("name_by_lower", {}).values()):
        writes.append((
            ROBOTS_PATH + "/" + robotFolder + "/" + memberName,
            transform(valuesByFolder.get(robotFolder, 0))
        ))
    return writes


def resolve_mission_robot_folder(mission, robotMappings=None):
    """
    Resolve the mission's robot-specific folder name or return Unassigned.
    """
    if robotMappings is None:
        robotMappings = _readRobotFolderMappings()

    resolvedRobot = resolveMissionRobotId(mission)
    if not resolvedRobot:
        return UNASSIGNED_FOLDER

    resolvedRobot = str(resolvedRobot).strip().lower()
    if not resolvedRobot:
        return UNASSIGNED_FOLDER

    robotName = robotMappings["name_by_lower"].get(resolvedRobot)
    if robotName:
        return robotName

    robotName = robotMappings["name_by_id"].get(resolvedRobot)
    if robotName:
        return robotName

    return UNKNOWN_ROBOT_FOLDER


# ---------------------------------------------------------------------------
# COMPLETED CLEANUP
# ---------------------------------------------------------------------------

def cleanup_terminal_folder(folderPath, retentionDays, maxCount, label, logger, debug=False):
    """
    Enforces terminal mission retention and max count for the given folder.
    """
    now = system.date.now()
    cutoff = system.date.addDays(now, -retentionDays)

    instances = browseMissionInstances(folderPath)
    removed = []
    removedPaths = set()
    enriched = []

    readPaths = [fullPath + "/Created" for fullPath, _ in instances]
    readResults = []
    if readPaths:
        readResults = readTagValues(readPaths)

    for index, instance in enumerate(instances):
        fullPath, name = instance
        qualifiedValue = readResults[index]
        if not qualifiedValue.quality.isGood():
            logger.warn(
                "Skipping {} mission {} during cleanup - Created tag is not readable".format(
                    label,
                    fullPath
                )
            )
            continue

        createdDate = parse_date(qualifiedValue.value)
        if createdDate is None:
            logger.warn(
                "Skipping {} mission {} during cleanup - invalid Created value".format(
                    label,
                    fullPath
                )
            )
            continue
        enriched.append((fullPath, name, createdDate))

    removed_age = 0
    remaining = []
    for fullPath, name, createdDate in enriched:
        if should_remove_completed_by_age(createdDate, cutoff):
            remove_instance(
                fullPath,
                logger,
                debug,
                "older than {} days".format(retentionDays)
            )
            removed_age += 1
            removedPaths.add(fullPath)
            removed.append((fullPath, "age"))
        else:
            remaining.append((fullPath, name, createdDate))

    if debug:
        logger.info("{} cleanup: removed {} by age".format(label.title(), removed_age))

    excess = compute_completed_overflow(remaining, maxCount, now)
    for fullPath, name, ts in excess:
        if fullPath in removedPaths:
            continue
        remove_instance(
            fullPath,
            logger,
            debug,
            "pruned to max {}".format(maxCount)
        )
        removedPaths.add(fullPath)
        removed.append((fullPath, "max"))

    if debug and excess:
        logger.info(
            "{} cleanup: pruned {} to max {}".format(
                label.title(),
                len(excess),
                maxCount
            )
        )

    return removed


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run():
    """
    Main mission sorting entry point
    """
    logger = _log()
    debug = _debug_enabled()

    _dlog(logger, debug, "MissionSorting.run START")

    result = None

    try:
        missions = []
        nowDate = system.date.now()
        nowTimestamp = system.date.format(nowDate, "yyyy-MM-dd HH:mm:ss.SSS")

        # --- Fetch ACTIVE missions in one bulk call ---
        missions.extend(
            getMissions(
                logger,
                debug,
                mission_status=ACTIVE_STATUSES
            )
        )

        # --- Fetch COMPLETED missions in one capped bulk call ---
        missions.extend(
            getMissions(
                logger,
                debug,
                mission_status=TERMINAL_STATUSES,
                limit=MAX_COMPLETED
            )
        )

        # --- Fetch FAILED missions in one capped bulk call ---
        missions.extend(
            getMissions(
                logger,
                debug,
                mission_status=FAILED_STATUSES,
                limit=MAX_FAILED
            )
        )

        robotMappings = _readRobotFolderMappings()
        activeWanted = set()
        completedWanted = set()
        failedWanted = set()
        activeCountsByFolder = {}
        failedCountsByFolder = {}
        attachmentReadyByFolder = {}
        attachmentMissionIdByFolder = {}
        attachmentMissionNameByFolder = {}
        removed = []

        for mission in missions:
            status = mission.get("mission_status", "")
            instanceName = make_instance_name(mission)
            robotFolder = resolve_mission_robot_folder(mission, robotMappings)
            attachmentState = deriveMissionAttachmentState(mission)

            activePath = ACTIVE_PATH + "/" + robotFolder + "/" + instanceName
            completedPath = COMPLETED_PATH + "/" + robotFolder + "/" + instanceName
            failedPath = FAILED_PATH + "/" + robotFolder + "/" + instanceName

            bucket = classify_mission_bucket(status)

            if bucket == "completed":
                if tagExists(activePath):
                    remove_instance(
                        activePath,
                        logger,
                        debug,
                        "moved to Completed"
                    )
                    removed.append((activePath, "moved_to_completed"))
                if tagExists(failedPath):
                    remove_instance(
                        failedPath,
                        logger,
                        debug,
                        "moved to Completed"
                    )
                    removed.append((failedPath, "moved_to_completed"))

                targetFolder = COMPLETED_PATH + "/" + robotFolder
                completedWanted.add(completedPath)

            elif bucket == "failed":
                if tagExists(activePath):
                    remove_instance(
                        activePath,
                        logger,
                        debug,
                        "moved to Failed"
                    )
                    removed.append((activePath, "moved_to_failed"))
                if tagExists(completedPath):
                    remove_instance(
                        completedPath,
                        logger,
                        debug,
                        "moved to Failed"
                    )
                    removed.append((completedPath, "moved_to_failed"))

                targetFolder = FAILED_PATH + "/" + robotFolder
                failedWanted.add(failedPath)
                failedCountsByFolder[robotFolder] = failedCountsByFolder.get(robotFolder, 0) + 1

            else:
                if tagExists(completedPath):
                    remove_instance(
                        completedPath,
                        logger,
                        debug,
                        "moved to Active"
                    )
                    removed.append((completedPath, "moved_to_active"))
                if tagExists(failedPath):
                    remove_instance(
                        failedPath,
                        logger,
                        debug,
                        "moved to Active"
                    )
                    removed.append((failedPath, "moved_to_active"))

                targetFolder = ACTIVE_PATH + "/" + robotFolder
                activeWanted.add(activePath)
                activeCountsByFolder[robotFolder] = activeCountsByFolder.get(robotFolder, 0) + 1
                if attachmentState.get("ready_for_attachment") is True:
                    attachmentReadyByFolder[robotFolder] = True
                    attachmentMissionIdByFolder[robotFolder] = str(
                        attachmentState.get("attachment_mission_id") or mission.get("id") or ""
                    )
                    attachmentMissionNameByFolder[robotFolder] = str(
                        attachmentState.get("attachment_mission_name") or mission.get("name") or ""
                    )

            instancePath = targetFolder + "/" + instanceName
            if not tagExists(instancePath):
                ensureUdtInstancePath(instancePath, "api_Mission")
                if debug:
                    logger.info("Created mission instance: {}".format(instancePath))

            write_mission_data(instancePath, mission)

        missionCountWrites = (
            build_robot_member_writes(
                robotMappings,
                activeCountsByFolder,
                "ActiveMissionCount"
            ) +
            build_robot_member_writes(
                robotMappings,
                failedCountsByFolder,
                "FailedMissionCount"
            ) +
            build_robot_member_writes(
                robotMappings,
                attachmentReadyByFolder,
                "MissionReadyforAttachment",
                transform=lambda value: bool(value)
            ) +
            build_robot_member_writes(
                robotMappings,
                attachmentMissionIdByFolder,
                "MissionIdForAttacment",
                transform=lambda value: str(value or "")
            ) +
            build_robot_member_writes(
                robotMappings,
                attachmentMissionNameByFolder,
                "MissionNameForAttachment",
                transform=lambda value: str(value or "")
            )
        )
        if missionCountWrites:
            writeTagValues(
                [path for path, _ in missionCountWrites],
                [value for _, value in missionCountWrites]
            )

        # --- Cleanup ACTIVE ---
        for fullPath, name in browseMissionInstances(ACTIVE_PATH):
            if fullPath not in activeWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )
                removed.append((fullPath, "stale_active"))

        # --- Cleanup COMPLETED ---
        for fullPath, name in browseMissionInstances(COMPLETED_PATH):
            if fullPath not in completedWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )
                removed.append((fullPath, "stale_completed"))

        # --- Cleanup FAILED ---
        for fullPath, name in browseMissionInstances(FAILED_PATH):
            if fullPath not in failedWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )
                removed.append((fullPath, "stale_failed"))

        removed.extend(
            cleanup_terminal_folder(
                COMPLETED_PATH,
                COMPLETED_RETENTION_DAYS,
                MAX_COMPLETED,
                "completed",
                logger,
                debug
            )
        )
        removed.extend(
            cleanup_terminal_folder(
                FAILED_PATH,
                FAILED_RETENTION_DAYS,
                MAX_FAILED,
                "failed",
                logger,
                debug
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
            removed=removed
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
