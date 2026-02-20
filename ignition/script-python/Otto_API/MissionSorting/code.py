import json
from java.util import Date

from Otto_API.Get import getMissions
from Otto_API.Get import sanitizeTagName

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
        return bool(system.tag.read(DEBUG_TAG_PATH).value)
    except:
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

    if isinstance(val, Date):
        return val

    try:
        return system.date.parse(str(val))
    except:
        return None


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
    values = {
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

    paths = [instancePath + "/" + k for k in values]
    vals = [values[k] for k in values]

    system.tag.writeBlocking(paths, vals)


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
    except:
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
    except:
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
    enriched = []

    for fullPath, name in instances:
        createdVal = system.tag.read(fullPath + "/Created").value
        createdDate = parse_date(createdVal)
        enriched.append((fullPath, name, createdDate))

    removed_age = 0
    for fullPath, name, createdDate in enriched:
        if createdDate and createdDate.before(cutoff):
            remove_instance(
                fullPath,
                logger,
                debug,
                "older than {} days".format(COMPLETED_RETENTION_DAYS)
            )
            removed_age += 1

    if debug:
        logger.info("Completed cleanup: removed {} by age".format(removed_age))

    instances = browse_instances(COMPLETED_PATH)
    enriched = []

    for fullPath, name in instances:
        createdVal = system.tag.read(fullPath + "/Created").value
        createdDate = parse_date(createdVal)
        enriched.append((fullPath, name, createdDate))

    def sort_key(item):
        """Sort by Created timestamp, falling back to current time for missing values."""
        return item[2] if item[2] else now

    enriched_sorted = sorted(enriched, key=sort_key)

    if len(enriched_sorted) > MAX_COMPLETED:
        excess = enriched_sorted[:-MAX_COMPLETED]
        for fullPath, name, ts in excess:
            remove_instance(
                fullPath,
                logger,
                debug,
                "pruned to max {}".format(MAX_COMPLETED)
            )

        if debug:
            logger.info(
                "Completed cleanup: pruned {} to max {}".format(
                    len(excess),
                    MAX_COMPLETED
                )
            )


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

    try:
        missions = []

        # --- Fetch ACTIVE missions (unlimited) ---
        for status in ACTIVE_STATUSES:
            missions.extend(
                getMissions(
                    logger,
                    debug,
                    mission_status=status
                )
            )

        # --- Fetch COMPLETED missions (capped total) ---
        remaining = MAX_COMPLETED

        for status in TERMINAL_STATUSES:
            if remaining <= 0:
                break

            batch = getMissions(
                logger,
                debug,
                mission_status=status,
                limit=remaining
            )

            missions.extend(batch)
            remaining -= len(batch)

        activeWanted = set()
        completedWanted = set()

        for mission in missions:
            status = mission.get("mission_status", "")
            instanceName = make_instance_name(mission)

            activePath = ACTIVE_PATH + "/" + instanceName
            completedPath = COMPLETED_PATH + "/" + instanceName

            if status in TERMINAL_STATUSES:
                if system.tag.exists(activePath):
                    remove_instance(
                        activePath,
                        logger,
                        debug,
                        "moved to Completed"
                    )

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

        # --- Cleanup COMPLETED ---
        for fullPath, name in browse_instances(COMPLETED_PATH):
            if name not in completedWanted:
                remove_instance(
                    fullPath,
                    logger,
                    debug,
                    "stale (not returned)"
                )

        cleanup_completed(logger, debug)

    except Exception as e:
        logger.error("MissionSorting.run FAILED: {}".format(e))

    _dlog(logger, debug, "MissionSorting.run END")
