from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import writeTagValues
from Otto_API.Fleet.ContentSync import sanitizeTagName
from Otto_API.Fleet.Get import getMissions
from Otto_API.Missions.MissionActions import resolveMissionRobotId

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

BASE = "[Otto_FleetManager]Missions"
ACTIVE_PATH = BASE + "/Active"
COMPLETED_PATH = BASE + "/Completed"
FAILED_PATH = BASE + "/Failed"
LAST_UPDATE_TS_PATH = BASE + "/LastUpdateTS"
LAST_UPDATE_SUCCESS_PATH = BASE + "/LastUpdateSuccess"

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
    "REVOKED",
    "FAILED"
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

    try:
        return system.date.parse(str(val))
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


def _writeMissionUpdateStatus(success, timestampValue):
    writeTagValues(
        [LAST_UPDATE_TS_PATH, LAST_UPDATE_SUCCESS_PATH],
        [timestampValue, bool(success)]
    )


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


def ensure_instance(parentFolder, instanceName, logger=None, debug=False):
    """
    Ensures api_Mission UDT instance exists and returns its path
    """
    instPath = parentFolder + "/" + instanceName

    if not system.tag.exists(instPath):
        tagDef = {
            "name": instanceName,
            "typeID": "api_Mission",
            "tagType": "UdtInstance"
        }
        system.tag.configure(parentFolder, [tagDef], "a")
        if logger and debug:
            logger.info("Created mission instance: {}".format(instPath))

    return instPath


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
        system.tag.deleteTags([path])
        if logger and debug:
            if reason:
                logger.info("Deleted {} ({})".format(path, reason))
            else:
                logger.info("Deleted {}".format(path))
    except Exception:
        pass


def browse_instances(folderPath):
    """
    Returns list of (fullPath, name) for UDT instances in folder
    """
    try:
        results = system.tag.browse(folderPath).getResults()
        out = []
        for t in results:
            if str(t.get("tagType")) == "UdtInstance":
                out.append((str(t.get("fullPath")), t.get("name")))
        return out
    except Exception:
        return []


def browse_instances_recursive(folderPath):
    """
    Returns list of (fullPath, name) for all UDT instances below the given folder.
    """
    instances = []
    pending = [folderPath]

    while pending:
        currentFolder = pending.pop(0)
        try:
            results = system.tag.browse(currentFolder).getResults()
        except Exception:
            continue

        for row in results:
            tagType = str(row.get("tagType"))
            fullPath = str(row.get("fullPath"))
            name = row.get("name")
            if tagType == "UdtInstance":
                instances.append((fullPath, name))
            elif tagType == "Folder":
                pending.append(fullPath)

    return instances


def _readRobotFolderMappings():
    """
    Build lookup maps for robot folder names and robot IDs.
    """
    nameByLower = {}
    idByName = {}
    idPaths = []
    robotNames = []

    try:
        results = system.tag.browse(ROBOTS_PATH).getResults()
    except Exception:
        return {
            "name_by_lower": nameByLower,
            "name_by_id": {},
        }

    for row in results:
        if str(row.get("tagType")) != "UdtInstance":
            continue
        robotName = str(row.get("name"))
        robotBasePath = str(row.get("fullPath"))
        robotNames.append(robotName)
        nameByLower[robotName.strip().lower()] = robotName
        idPaths.append(robotBasePath + "/ID")

    readResults = system.tag.readBlocking(idPaths) if idPaths else []
    nameById = {}
    for index, robotName in enumerate(robotNames):
        qualifiedValue = readResults[index]
        if not qualifiedValue.quality.isGood():
            continue
        value = qualifiedValue.value
        if value is None:
            continue
        normalizedId = str(value).strip().lower()
        if normalizedId:
            nameById[normalizedId] = robotName

    return {
        "name_by_lower": nameByLower,
        "name_by_id": nameById,
    }


def build_active_mission_count_writes(robotMappings, activeCountsByFolder):
    """
    Build ActiveMissionCount writes for known robot folders.
    """
    writes = []
    for robotFolder in sorted(robotMappings.get("name_by_lower", {}).values()):
        writes.append((
            ROBOTS_PATH + "/" + robotFolder + "/ActiveMissionCount",
            int(activeCountsByFolder.get(robotFolder, 0))
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

    instances = browse_instances_recursive(folderPath)
    removed = []
    removedPaths = set()
    enriched = []

    readPaths = [fullPath + "/Created" for fullPath, _ in instances]
    readResults = []
    if readPaths:
        readResults = system.tag.readBlocking(readPaths)

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

        robotMappings = _readRobotFolderMappings()
        activeWanted = set()
        completedWanted = set()
        failedWanted = set()
        activeCountsByFolder = {}
        removed = []

        for mission in missions:
            status = mission.get("mission_status", "")
            instanceName = make_instance_name(mission)
            robotFolder = resolve_mission_robot_folder(mission, robotMappings)

            activePath = ACTIVE_PATH + "/" + robotFolder + "/" + instanceName
            completedPath = COMPLETED_PATH + "/" + robotFolder + "/" + instanceName
            failedPath = FAILED_PATH + "/" + robotFolder + "/" + instanceName

            bucket = classify_mission_bucket(status)

            if bucket == "completed":
                if system.tag.exists(activePath):
                    remove_instance(
                        activePath,
                        logger,
                        debug,
                        "moved to Completed"
                    )
                    removed.append((activePath, "moved_to_completed"))
                if system.tag.exists(failedPath):
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
                if system.tag.exists(activePath):
                    remove_instance(
                        activePath,
                        logger,
                        debug,
                        "moved to Failed"
                    )
                    removed.append((activePath, "moved_to_failed"))
                if system.tag.exists(completedPath):
                    remove_instance(
                        completedPath,
                        logger,
                        debug,
                        "moved to Failed"
                    )
                    removed.append((completedPath, "moved_to_failed"))

                targetFolder = FAILED_PATH + "/" + robotFolder
                failedWanted.add(failedPath)

            else:
                if system.tag.exists(completedPath):
                    remove_instance(
                        completedPath,
                        logger,
                        debug,
                        "moved to Active"
                    )
                    removed.append((completedPath, "moved_to_active"))
                if system.tag.exists(failedPath):
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

            instancePath = ensure_instance(
                targetFolder,
                instanceName,
                logger,
                debug
            )

            write_mission_data(instancePath, mission)

        activeMissionCountWrites = build_active_mission_count_writes(
            robotMappings,
            activeCountsByFolder
        )
        if activeMissionCountWrites:
            writeTagValues(
                [path for path, _ in activeMissionCountWrites],
                [value for _, value in activeMissionCountWrites]
            )

        # --- Cleanup ACTIVE ---
        for fullPath, name in browse_instances_recursive(ACTIVE_PATH):
            if fullPath not in activeWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )
                removed.append((fullPath, "stale_active"))

        # --- Cleanup COMPLETED ---
        for fullPath, name in browse_instances_recursive(COMPLETED_PATH):
            if fullPath not in completedWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )
                removed.append((fullPath, "stale_completed"))

        # --- Cleanup FAILED ---
        for fullPath, name in browse_instances_recursive(FAILED_PATH):
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
        _writeMissionUpdateStatus(True, nowTimestamp)
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
                system.date.format(system.date.now(), "yyyy-MM-dd HH:mm:ss.SSS")
            )
        except Exception:
            pass
        logger.error("MissionSorting.run FAILED: {}".format(e))
        result = _buildSyncResult(
            False,
            "error",
            "Mission sorting failed: {}".format(e)
        )

    _dlog(logger, debug, "MissionSorting.run END")
    return result
