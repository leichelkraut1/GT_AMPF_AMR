from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.TagHelpers import writeTagValues
from Otto_API.Fleet.ContentSync import sanitizeTagName
from Otto_API.Fleet.Get import getMissions

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

BASE = "[Otto_FleetManager]Missions"
ACTIVE_PATH = BASE + "/Active"
COMPLETED_PATH = BASE + "/Completed"

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

MAX_COMPLETED = 50
COMPLETED_RETENTION_DAYS = 5

DEBUG_TAG_PATH = "[Otto_FleetManager]Missions/DebugEnabled"


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


def _buildSyncResult(ok, level, message, activeWanted=None, completedWanted=None, removed=None):
    activeWanted = sorted(list(activeWanted or []))
    completedWanted = sorted(list(completedWanted or []))
    removed = list(removed or [])
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "active_wanted": activeWanted,
            "completed_wanted": completedWanted,
            "removed": removed,
        },
        active_wanted=activeWanted,
        completed_wanted=completedWanted,
        removed=removed,
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


def classify_mission_bucket(missionStatus, terminalStatuses=None):
    """
    Classify a mission into the Active or Completed bucket.
    """
    if terminalStatuses is None:
        terminalStatuses = TERMINAL_STATUSES

    status = str(missionStatus or "").upper()
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


# ---------------------------------------------------------------------------
# COMPLETED CLEANUP
# ---------------------------------------------------------------------------

def cleanup_completed(logger, debug=False):
    """
    Enforces completed mission retention and max count
    """
    now = system.date.now()
    cutoff = system.date.addDays(now, -COMPLETED_RETENTION_DAYS)

    instances = browse_instances(COMPLETED_PATH)
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
                "Skipping completed mission {} during cleanup - Created tag is not readable".format(
                    fullPath
                )
            )
            continue

        createdDate = parse_date(qualifiedValue.value)
        if createdDate is None:
            logger.warn(
                "Skipping completed mission {} during cleanup - invalid Created value".format(
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
                "older than {} days".format(COMPLETED_RETENTION_DAYS)
            )
            removed_age += 1
            removedPaths.add(fullPath)
            removed.append((fullPath, "age"))
        else:
            remaining.append((fullPath, name, createdDate))

    if debug:
        logger.info("Completed cleanup: removed {} by age".format(removed_age))

    excess = compute_completed_overflow(remaining, MAX_COMPLETED, now)
    for fullPath, name, ts in excess:
        if fullPath in removedPaths:
            continue
        remove_instance(
            fullPath,
            logger,
            debug,
            "pruned to max {}".format(MAX_COMPLETED)
        )
        removedPaths.add(fullPath)
        removed.append((fullPath, "max"))

    if debug and excess:
        logger.info(
            "Completed cleanup: pruned {} to max {}".format(
                len(excess),
                MAX_COMPLETED
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

        activeWanted = set()
        completedWanted = set()
        removed = []

        for mission in missions:
            status = mission.get("mission_status", "")
            instanceName = make_instance_name(mission)

            activePath = ACTIVE_PATH + "/" + instanceName
            completedPath = COMPLETED_PATH + "/" + instanceName

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

                targetFolder = COMPLETED_PATH
                completedWanted.add(instanceName)

            else:
                if system.tag.exists(completedPath):
                    remove_instance(
                        completedPath,
                        logger,
                        debug,
                        "moved to Active"
                    )
                    removed.append((completedPath, "moved_to_active"))

                targetFolder = ACTIVE_PATH
                activeWanted.add(instanceName)

            instancePath = ensure_instance(
                targetFolder,
                instanceName,
                logger,
                debug
            )

            write_mission_data(instancePath, mission)

        # --- Cleanup ACTIVE ---
        for fullPath, name in browse_instances(ACTIVE_PATH):
            if name not in activeWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )
                removed.append((fullPath, "stale_active"))

        # --- Cleanup COMPLETED ---
        for fullPath, name in browse_instances(COMPLETED_PATH):
            if name not in completedWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )
                removed.append((fullPath, "stale_completed"))

        removed.extend(cleanup_completed(logger, debug))
        result = _buildSyncResult(
            True,
            "info",
            "Mission sorting completed for {} mission(s)".format(len(missions)),
            activeWanted=activeWanted,
            completedWanted=completedWanted,
            removed=removed
        )

    except Exception as e:
        logger.error("MissionSorting.run FAILED: {}".format(e))
        result = _buildSyncResult(
            False,
            "error",
            "Mission sorting failed: {}".format(e)
        )

    _dlog(logger, debug, "MissionSorting.run END")
    return result
