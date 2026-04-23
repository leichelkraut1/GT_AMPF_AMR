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
