from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureUdtInstancePath
from Otto_API.Common.TagHelpers import getFleetRobotsPath
from Otto_API.Common.TagHelpers import getMainControlRobotsPath
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.TagHelpers import writeRequiredTagValues
from Otto_API.Missions.Buckets import build_mission_bucket_paths
from Otto_API.Missions.Buckets import make_instance_name
from Otto_API.Missions.Maintenance import remove_instance
from Otto_API.Missions.Runtime import build_mission_write_signature
from Otto_API.Missions.Runtime import carry_forward_last_logged_status
from Otto_API.Missions.Runtime import mission_runtime_paths
from Otto_API.Missions.Runtime import read_previous_mission_status
from Otto_API.Missions.Runtime import read_previous_mission_value
from Otto_API.Missions.Runtime import record_mission_status_if_changed
from Otto_API.Missions.Runtime import warn_missing_mission_runtime_member


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
    runtimePaths = mission_runtime_paths(instancePath)
    hasWriteSignatureTag = tagExists(runtimePaths["last_write_signature"])
    if hasWriteSignatureTag:
        currentSignature = read_previous_mission_value(
            [instancePath],
            "_LastWriteSignature",
            "",
            allowEmptyString=True
        )
        if str(currentSignature or "") == signature:
            return False
    else:
        warn_missing_mission_runtime_member(
            "_LastWriteSignature",
            logger
        )

    paths = [instancePath + "/" + k for k in values]
    vals = [values[k] for k in values]
    if hasWriteSignatureTag:
        paths.append(runtimePaths["last_write_signature"])
        vals.append(signature)
    writeRequiredTagValues(
        paths,
        vals,
        labels=["MissionSorting mission write"] * len(paths)
    )
    return True


def build_robot_member_writes(robotMappings, valuesByFolder, memberName, transform=None, basePath=None):
    """
    Build robot-member writes for known robot folders.
    """
    def _identity(value):
        return value

    if transform is None:
        transform = _identity
    if basePath is None:
        basePath = getFleetRobotsPath()

    writes = []
    for robotFolder in sorted(robotMappings.get("name_by_lower", {}).values()):
        writes.append((
            basePath + "/" + robotFolder + "/" + memberName,
            transform(valuesByFolder.get(robotFolder, 0))
        ))
    return writes


def ensure_maincontrol_robot_attachment_tags(robotMappings):
    """
    Ensure MainControl/Robots UDT instances exist for known robots.
    """
    mainControlRobotsPath = getMainControlRobotsPath()
    ensureFolder(mainControlRobotsPath)
    for robotFolder in sorted(robotMappings.get("name_by_lower", {}).values()):
        robotPath = mainControlRobotsPath + "/" + robotFolder
        ensureUdtInstancePath(robotPath, "MainControl_Robot")


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

    moveReasons = {
        "active": {
            "completed": "moved_to_active",
            "failed": "moved_to_active",
        },
        "completed": {
            "active": "moved_to_completed",
            "failed": "moved_to_completed",
        },
        "failed": {
            "active": "moved_to_failed",
            "completed": "moved_to_failed",
        },
    }
    logReasons = {
        "moved_to_active": "moved to Active",
        "moved_to_completed": "moved to Completed",
        "moved_to_failed": "moved to Failed",
    }

    for otherBucket in ["active", "completed", "failed"]:
        if otherBucket == bucket:
            continue
        removalKey = moveReasons[bucket][otherBucket]
        if tagExists(paths[otherBucket]):
            remove_instance(
                paths[otherBucket],
                logger,
                debug,
                logReasons[removalKey]
            )
            removed.append((paths[otherBucket], removalKey))

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
