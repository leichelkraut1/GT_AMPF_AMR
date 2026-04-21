from Otto_API.Common.RuntimeHistory import appendRobotStateHistoryRow
from Otto_API.Common.RuntimeHistory import buildRobotStateLogSignature


ROBOT_STATE_LOG_SIGNATURE_MEMBER = "LastRobotStateLogSignature"
_MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS = set()


def warnMissingRobotStateLogSignaturePath(logger, tagPath):
    tagPath = str(tagPath or "")
    if not tagPath or tagPath in _MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS:
        return
    _MISSING_ROBOT_STATE_LOG_SIGNATURE_PATHS.add(tagPath)
    logger.warn(
        "Robot state history dedupe is disabled until [{}] is added to api_Robot".format(
            tagPath
        )
    )


def buildRobotStateHistoryUpdate(
    robotName,
    previousSystemState,
    effectiveSystemState,
    previousSubSystemState,
    effectiveSubSystemState,
    previousActivityState,
    effectiveActivity,
    previousRobotStateLogSignature,
    robotStateLogSignaturePath,
    hasSignatureTag,
    logger,
):
    if not (
        previousSystemState != effectiveSystemState or
        previousSubSystemState != effectiveSubSystemState or
        previousActivityState != effectiveActivity
    ):
        return None

    if not hasSignatureTag:
        warnMissingRobotStateLogSignaturePath(logger, robotStateLogSignaturePath)
        return None

    signature = buildRobotStateLogSignature(
        robotName,
        previousSystemState,
        effectiveSystemState,
        previousSubSystemState,
        effectiveSubSystemState,
        previousActivityState,
        effectiveActivity
    )
    if previousRobotStateLogSignature == signature:
        return None

    return {
        "pending_row": {
            "robot_name": robotName,
            "old_system_state": previousSystemState,
            "new_system_state": effectiveSystemState,
            "old_sub_system_state": previousSubSystemState,
            "new_sub_system_state": effectiveSubSystemState,
            "old_activity_state": previousActivityState,
            "new_activity_state": effectiveActivity,
        },
        "signature_write": (robotStateLogSignaturePath, signature),
    }


def appendPendingRobotStateHistoryRows(nowTimestamp, pendingRows):
    for row in list(pendingRows or []):
        appendRobotStateHistoryRow(
            nowTimestamp,
            row["robot_name"],
            row["old_system_state"],
            row["new_system_state"],
            row["old_sub_system_state"],
            row["new_sub_system_state"],
            row["old_activity_state"],
            row["new_activity_state"],
        )
