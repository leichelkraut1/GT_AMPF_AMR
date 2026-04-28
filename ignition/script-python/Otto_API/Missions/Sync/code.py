from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagIO import writeRequiredTagValues
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Missions.Buckets import build_mission_bucket_paths
from Otto_API.Missions.Buckets import make_instance_name
from Otto_API.Missions.Maintenance import remove_instance
from Otto_API.Missions.Runtime import build_mission_write_signature
from Otto_API.Missions.Runtime import carry_forward_last_logged_status
from Otto_API.Missions.Runtime import mission_runtime_tag_path
from Otto_API.Missions.Runtime import read_previous_mission_status
from Otto_API.Missions.Runtime import read_previous_mission_value
from Otto_API.Missions.Runtime import record_mission_status_if_changed


MISSION_RAW_TAG_FIELDS = (
    ("Client_Reference_ID", "client_reference_id"),
    ("Created", "created"),
    ("Current_Task", "current_task"),
    ("Description", "description"),
    ("Due_State", "due_state"),
    ("Execution_End", "execution_end"),
    ("Execution_Start", "execution_start"),
    ("Execution_Time", "execution_time"),
    ("Finalized", "finalized"),
    ("Force_Team", "force_team"),
    ("Max_Duration", "max_duration"),
    ("Metadata", "metadata"),
    ("Nominal_Duration", "nominal_duration"),
    ("Paused", "paused"),
    ("Priority", "priority"),
    ("Result_Text", "result_text"),
    ("Result_Text_Intl_Data", "result_text_intl_data"),
    ("Result_Text_Intl_Key", "result_text_intl_key"),
    ("Signature", "signature"),
    ("Structure", "structure"),
)


def _rawMissionValue(missionRecord, fieldName):
    return missionRecord.get(fieldName)


def mission_to_tag_values(mission):
    """
    Convert a mission record into api_Mission field values.
    """
    values = {
        "ID": mission.id,
        "Assigned_Robot": mission.assigned_robot,
        "Force_Robot": mission.force_robot,
        "Mission_Status": mission.mission_status,
        "Name": mission.name,
    }

    for tagName, fieldName in MISSION_RAW_TAG_FIELDS:
        values[tagName] = _rawMissionValue(mission, fieldName)

    return values


def write_mission_data(instancePath, mission, logger):
    """
    Write mission fields into api_Mission only when the payload actually changed.
    """
    values = mission_to_tag_values(mission)
    signature = build_mission_write_signature(values)
    runtimePath = mission_runtime_tag_path(
        instancePath,
        "last_write_signature",
        "_LastWriteSignature",
        logger=logger,
        warn=True
    )
    if runtimePath:
        currentSignature = read_previous_mission_value(
            [instancePath],
            "_LastWriteSignature",
            "",
            allowEmptyString=True
        )
        if str(currentSignature or "") == signature:
            return False
    paths = [instancePath + "/" + k for k in values]
    vals = [values[k] for k in values]
    if runtimePath:
        paths.append(runtimePath)
        vals.append(signature)
    writeRequiredTagValues(
        paths,
        vals,
        labels=["MissionSorting mission write"] * len(paths)
    )
    return True

def sync_mission_into_bucket(
    mission,
    robotFolder,
    bucket,
    nowTimestamp,
    logger,
    activePath,
    completedPath,
    failedPath,
    debug=False
):
    """
    Move a mission into the requested bucket, write its data, and record history.
    """
    def removalReason():
        return "moved to {}".format(bucket.capitalize())

    instanceName = make_instance_name(mission)
    paths = build_mission_bucket_paths(
        activePath,
        completedPath,
        failedPath,
        robotFolder,
        instanceName
    )
    previousStatus = read_previous_mission_status([
        paths["active"],
        paths["completed"],
        paths["failed"],
    ])
    lastLoggedStatus = read_previous_mission_value(
        [
            paths["active"],
            paths["completed"],
            paths["failed"],
        ],
        "_LastLoggedStatus",
        "",
        allowEmptyString=True
    )
    removed = []

    for otherBucket in ["active", "completed", "failed"]:
        if otherBucket == bucket:
            continue
        if tagExists(paths[otherBucket]):
            remove_instance(
                paths[otherBucket],
                logger,
                debug,
                removalReason()
            )
            removed.append((paths[otherBucket], "moved_to_" + bucket))

    targetPath = paths[bucket]
    if not tagExists(targetPath):
        ensureUdtInstancePath(targetPath, "api_Mission")
        if debug:
            logger.info("Created mission instance: {}".format(targetPath))

    carry_forward_last_logged_status(targetPath, lastLoggedStatus)
    write_mission_data(targetPath, mission, logger)
    record_mission_status_if_changed(
        targetPath,
        mission,
        robotFolder,
        previousStatus,
        nowTimestamp,
        lastLoggedStatus,
        logger
    )

    return {
        "bucket": bucket,
        "instance_name": instanceName,
        "target_path": targetPath,
        "paths": paths,
        "removed": removed,
    }
