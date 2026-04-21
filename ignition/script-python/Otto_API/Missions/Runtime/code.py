import json
import re

from Otto_API.Common.RuntimeHistory import appendRuntimeDatasetRow
from Otto_API.Common.RuntimeHistory import MISSION_STATE_HISTORY_HEADERS
from Otto_API.Common.RuntimeHistory import MISSION_STATE_HISTORY_MAX_ROWS
from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.TagHelpers import writeRequiredTagValues

from Otto_API.Missions.Buckets import UNASSIGNED_FOLDER


WORKFLOW_NAME_RE = re.compile(r"^WF(\d+)_")
MISSION_LAST_LOGGED_STATUS_MEMBER = "_LastLoggedStatus"
MISSION_LAST_WRITE_SIGNATURE_MEMBER = "_LastWriteSignature"
_WARNED_MISSING_MISSION_RUNTIME_MEMBERS = set()


def warn_missing_mission_runtime_member(memberName, logger):
    """
    Warn once per runtime helper member when api_Mission does not define it.
    """
    memberName = str(memberName or "")
    if not memberName or memberName in _WARNED_MISSING_MISSION_RUNTIME_MEMBERS:
        return
    _WARNED_MISSING_MISSION_RUNTIME_MEMBERS.add(memberName)
    if logger is not None:
        logger.warn(
            "api_Mission is missing [{}]; mission anti-repeat behavior will be degraded until the UDT includes it".format(
                memberName
            )
        )


def mission_runtime_paths(instancePath):
    """
    Return the mission-instance runtime helper tags used for anti-repeat behavior.
    """
    return {
        "last_logged_status": instancePath + "/" + MISSION_LAST_LOGGED_STATUS_MEMBER,
        "last_write_signature": instancePath + "/" + MISSION_LAST_WRITE_SIGNATURE_MEMBER,
    }


def build_mission_write_signature(tagValues):
    """
    Build a stable signature for the current mission tag payload.
    """
    return json.dumps(dict(tagValues or {}), sort_keys=True, default=str)


def parse_workflow_number_from_mission_name(missionName):
    text = str(missionName or "").strip()
    if not text:
        return None
    match = WORKFLOW_NAME_RE.match(text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def record_mission_state_change(nowTimestamp, robotFolder, mission, oldStatus, newStatus):
    appendRuntimeDatasetRow(
        "mission_state_history",
        MISSION_STATE_HISTORY_HEADERS,
        [
            nowTimestamp,
            robotFolder,
            str(mission.get("id") or ""),
            str(mission.get("name") or ""),
            str(oldStatus or ""),
            str(newStatus or ""),
            parse_workflow_number_from_mission_name(mission.get("name")) or 0,
        ],
        maxRows=MISSION_STATE_HISTORY_MAX_ROWS,
    )


def read_previous_mission_value(candidatePaths, memberName, defaultValue=None, allowEmptyString=False):
    """
    Read a value from whichever candidate mission instance currently exists.
    """
    for path in list(candidatePaths or []):
        if not tagExists(path):
            continue
        return readOptionalTagValue(
            path + "/" + memberName,
            defaultValue,
            allowEmptyString=allowEmptyString
        )
    return defaultValue


def read_previous_mission_status(candidatePaths):
    """
    Read the prior mission status from whichever mission instance currently exists.
    """
    return read_previous_mission_value(
        candidatePaths,
        "Mission_Status",
        None,
        allowEmptyString=False
    )


def record_mission_status_if_changed(
    instancePath,
    mission,
    robotFolder,
    previousStatus,
    nowTimestamp,
    lastLoggedStatus,
    logger
):
    """
    Append mission-state history only when the mission instance's last logged state changes.
    """
    newStatus = str(mission.get("mission_status") or "")
    hasLastLoggedStatusTag = tagExists(
        mission_runtime_paths(instancePath)["last_logged_status"]
    )
    if not hasLastLoggedStatusTag:
        warn_missing_mission_runtime_member(
            MISSION_LAST_LOGGED_STATUS_MEMBER,
            logger
        )
    if hasLastLoggedStatusTag:
        if str(lastLoggedStatus or "") == newStatus:
            return False
    elif previousStatus is not None and str(previousStatus) == newStatus:
        return False

    record_mission_state_change(
        nowTimestamp,
        robotFolder,
        mission,
        previousStatus,
        newStatus
    )
    if hasLastLoggedStatusTag:
        writeRequiredTagValues(
            [mission_runtime_paths(instancePath)["last_logged_status"]],
            [newStatus],
            labels=["MissionSorting last logged status"]
        )
    return True


def carry_forward_last_logged_status(instancePath, lastLoggedStatus):
    """
    Seed the new bucket instance with the prior logged status when it moves folders.
    """
    if not str(lastLoggedStatus or ""):
        return False

    runtimePath = mission_runtime_paths(instancePath)["last_logged_status"]
    if not tagExists(runtimePath):
        return False

    currentValue = readOptionalTagValue(
        runtimePath,
        "",
        allowEmptyString=True
    )
    if str(currentValue or ""):
        return False

    writeRequiredTagValues(
        [runtimePath],
        [str(lastLoggedStatus or "")],
        labels=["MissionSorting carry-forward status"]
    )
    return True


def record_removed_mission_if_needed(instancePath, folderPath, nowTimestamp, logger=None):
    """
    Log a stale mission disappearing from OTTO as REMOVED before deleting the tag.
    """
    runtimePaths = mission_runtime_paths(instancePath)
    hasLastLoggedStatusTag = tagExists(runtimePaths["last_logged_status"])
    lastLoggedStatus = ""
    if hasLastLoggedStatusTag:
        lastLoggedStatus = readOptionalTagValue(
            runtimePaths["last_logged_status"],
            "",
            allowEmptyString=True
        )
    if hasLastLoggedStatusTag and str(lastLoggedStatus or "") == "REMOVED":
        return False

    missionId = str(readOptionalTagValue(instancePath + "/ID", "", allowEmptyString=True) or "")
    missionName = str(readOptionalTagValue(instancePath + "/Name", "", allowEmptyString=True) or "")
    previousStatus = str(readOptionalTagValue(instancePath + "/Mission_Status", "", allowEmptyString=True) or "")

    if not (missionId or missionName or previousStatus):
        return False

    suffix = str(instancePath or "")
    prefix = str(folderPath or "").rstrip("/") + "/"
    robotFolder = ""
    if suffix.startswith(prefix):
        remainder = suffix[len(prefix):]
        robotFolder = remainder.split("/", 1)[0] if remainder else ""
    if not robotFolder:
        robotFolder = UNASSIGNED_FOLDER

    record_mission_state_change(
        nowTimestamp,
        robotFolder,
        {
            "id": missionId,
            "name": missionName,
        },
        previousStatus,
        "REMOVED"
    )
    if hasLastLoggedStatusTag:
        writeRequiredTagValues(
            [runtimePaths["last_logged_status"]],
            ["REMOVED"],
            labels=["MissionSorting removed status"]
        )
    return True
