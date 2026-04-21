from Otto_API.Common.TagHelpers import readOptionalTagValue
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.TagHelpers import writeRequiredTagValues
from Otto_API.Common.RuntimeHistory import appendRuntimeDatasetRow
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_HEADERS
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_MAX_ROWS
from Otto_API.Common.RuntimeHistory import timestampString
from MainController.Robot.Actions import callMissionCommand
from MainController.WorkflowConfig import normalizeWorkflowNumber


MISSION_LAST_ISSUED_COMMAND_SIGNATURE_MEMBER = "_LastIssuedCommandSignature"
MISSION_LAST_COMMAND_LOG_SIGNATURE_MEMBER = "_LastCommandLogSignature"
_WARNED_MISSING_MISSION_RUNTIME_MEMBERS = set()


def _log():
    return system.util.getLogger("MainController_WorkflowRunner")


def _missionRuntimePaths(instancePath):
    """Return optional mission-instance runtime members used by controller clear actions."""
    return {
        "last_issued_command_signature": instancePath + "/" + MISSION_LAST_ISSUED_COMMAND_SIGNATURE_MEMBER,
        "last_command_log_signature": instancePath + "/" + MISSION_LAST_COMMAND_LOG_SIGNATURE_MEMBER,
    }


def _warnMissingMissionRuntimeMember(memberName):
    """Warn once when api_Mission is missing a controller-side runtime helper member."""
    memberName = str(memberName or "")
    if not memberName or memberName in _WARNED_MISSING_MISSION_RUNTIME_MEMBERS:
        return
    _WARNED_MISSING_MISSION_RUNTIME_MEMBERS.add(memberName)
    _log().warn(
        (
            "api_Mission is missing [{}]; mission clear anti-repeat/log dedupe will "
            "be degraded until the UDT includes it"
        ).format(memberName)
    )


def _missionRuntimeTagPath(instancePath, keyName, memberName, warn=True):
    """Return one mission runtime helper tag path when the UDT member exists."""
    if not instancePath:
        return None
    tagPath = _missionRuntimePaths(instancePath).get(keyName)
    if not tagPath or not tagExists(tagPath):
        if warn:
            _warnMissingMissionRuntimeMember(memberName)
        return None
    return tagPath


def _missionInstancePath(missionRecord):
    """Return the mission instance path used for controller-side runtime helpers."""
    missionRecord = dict(missionRecord or {})
    return str(missionRecord.get("instance_path") or missionRecord.get("path") or "")


def _readMissionRuntimeValue(instancePath, keyName, memberName):
    """Read an optional mission runtime helper tag, warning once if the UDT member is absent."""
    tagPath = _missionRuntimeTagPath(instancePath, keyName, memberName)
    if not tagPath:
        return ""
    return str(readOptionalTagValue(tagPath, "", allowEmptyString=True) or "")


def _writeMissionRuntimeValue(instancePath, keyName, memberName, value):
    """Write an optional mission runtime helper tag when the UDT member exists."""
    tagPath = _missionRuntimeTagPath(instancePath, keyName, memberName)
    if not tagPath:
        return False
    writeRequiredTagValues(
        [tagPath],
        [str(value or "")],
        labels=["MainController mission runtime helper"]
    )
    return True


def _buildMissionIssuedCommandSignature(actionName):
    """Build the per-mission signature that suppresses repeated clear commands after success."""
    return str(actionName or "")


def _buildMissionCommandLogSignature(actionName, result):
    """Build the per-mission signature that suppresses duplicate command-history rows."""
    return "|".join([
        str(actionName or ""),
        "ok" if bool((result or {}).get("ok")) else "fail",
        str((result or {}).get("level") or ""),
    ])


def _hasMissionCommandAlreadyIssued(missionRecord, actionName):
    """Return True when this mission already accepted the same clear command in a prior scan."""
    instancePath = _missionInstancePath(missionRecord)
    signature = _readMissionRuntimeValue(
        instancePath,
        "last_issued_command_signature",
        MISSION_LAST_ISSUED_COMMAND_SIGNATURE_MEMBER
    )
    return signature == _buildMissionIssuedCommandSignature(actionName)


def _markMissionCommandIssued(missionRecord, actionName):
    """Persist the successful per-mission clear command so later scans wait instead of repeating it."""
    instancePath = _missionInstancePath(missionRecord)
    return _writeMissionRuntimeValue(
        instancePath,
        "last_issued_command_signature",
        MISSION_LAST_ISSUED_COMMAND_SIGNATURE_MEMBER,
        _buildMissionIssuedCommandSignature(actionName)
    )


def _recordMissionCommandHistory(
    nowEpochMs,
    robotName,
    missionRecord,
    requestedWorkflowNumber,
    activeWorkflowNumber,
    actionName,
    result,
    stateName="mission_active"
):
    """Append one explicit mission cancel/finalize row unless the mission-side log signature already matches."""
    missionRecord = dict(missionRecord or {})
    instancePath = _missionInstancePath(missionRecord)
    logSignature = _buildMissionCommandLogSignature(actionName, result)
    currentSignature = _readMissionRuntimeValue(
        instancePath,
        "last_command_log_signature",
        MISSION_LAST_COMMAND_LOG_SIGNATURE_MEMBER
    )
    if currentSignature == logSignature:
        return

    missionName = str(missionRecord.get("mission_name") or missionRecord.get("name") or "")
    missionId = str(missionRecord.get("id") or "")
    message = str((result or {}).get("message") or "")
    missionBits = []
    if missionName:
        missionBits.append(missionName)
    if missionId:
        missionBits.append(missionId)
    if missionBits:
        missionText = ", ".join(missionBits)
        message = "{} ({})".format(message, missionText) if message else missionText

    appendRuntimeDatasetRow(
        "command_history",
        COMMAND_HISTORY_HEADERS,
        [
            timestampString(nowEpochMs),
            robotName,
            normalizeWorkflowNumber(requestedWorkflowNumber) or 0,
            normalizeWorkflowNumber(missionRecord.get("workflow_number"))
            or normalizeWorkflowNumber(activeWorkflowNumber)
            or 0,
            actionName,
            (result or {}).get("level") or "",
            stateName,
            message,
        ],
        maxRows=COMMAND_HISTORY_MAX_ROWS,
    )
    _writeMissionRuntimeValue(
        instancePath,
        "last_command_log_signature",
        MISSION_LAST_COMMAND_LOG_SIGNATURE_MEMBER,
        logSignature
    )


def _clearTargetActionName(missionRecord):
    """Choose the clear action for one active mission record."""
    missionStatus = str(dict(missionRecord or {}).get("mission_status") or "").upper()
    if missionStatus == "STARVED":
        return "finalize_mission"
    return "cancel_mission"


def _clearMissionMessageParts(finalizedCount, canceledCount, skippedCount, failedMessages):
    """Build a stable human-readable summary for one clear-reconcile pass."""
    parts = []
    if finalizedCount:
        parts.append("finalized {} mission(s)".format(finalizedCount))
    if canceledCount:
        parts.append("canceled {} mission(s)".format(canceledCount))
    if skippedCount:
        parts.append("waiting on {} previously issued clear command(s)".format(skippedCount))
    if failedMessages:
        parts.append(
            "failed {} mission(s): {}".format(
                len(failedMessages),
                "; ".join(list(failedMessages or [])[:2])
            )
        )
    if not parts:
        parts.append("waiting for cleared missions to disappear")
    return "; ".join(parts)


def issueMissionCommands(
    robotName,
    missionRecords,
    requestedWorkflowNumber,
    activeWorkflowNumber,
    nowEpochMs,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Issue explicit finalize/cancel commands for the supplied mission records."""
    finalizedCount = 0
    canceledCount = 0
    skippedCount = 0
    failedLevels = []
    failedMessages = []

    for missionRecord in list(missionRecords or []):
        missionRecord = dict(missionRecord or {})
        actionName = _clearTargetActionName(missionRecord)
        if _hasMissionCommandAlreadyIssued(missionRecord, actionName):
            skippedCount += 1
            continue

        missionId = str(missionRecord.get("id") or "")
        if not missionId:
            missionLabel = (
                missionRecord.get("mission_name")
                or missionRecord.get("name")
                or missionRecord.get("instance_path")
                or missionRecord.get("path")
                or "mission"
            )
            result = {
                "ok": False,
                "level": "warn",
                "message": "Cannot {} because [{}] is missing its mission id".format(
                    actionName.replace("_", " "),
                    missionLabel
                ),
            }
        else:
            result = callMissionCommand(
                actionName,
                missionId,
                finalizeMissionId=finalizeMissionId,
                cancelMissionIds=cancelMissionIds,
            )

        _recordMissionCommandHistory(
            nowEpochMs,
            robotName,
            missionRecord,
            requestedWorkflowNumber,
            activeWorkflowNumber,
            actionName,
            result,
        )

        if result.get("ok"):
            _markMissionCommandIssued(missionRecord, actionName)
            if actionName == "finalize_mission":
                finalizedCount += 1
            else:
                canceledCount += 1
        else:
            failedLevels.append(str(result.get("level") or "warn"))
            failedMessages.append(str(result.get("message") or "mission clear failed"))

    return {
        "finalized_count": finalizedCount,
        "canceled_count": canceledCount,
        "skipped_count": skippedCount,
        "failed_messages": failedMessages,
        "failed_levels": failedLevels,
        "issued_count": finalizedCount + canceledCount,
        "any_failures": bool(failedMessages),
        "message": _clearMissionMessageParts(
            finalizedCount,
            canceledCount,
            skippedCount,
            failedMessages
        ),
        "robot_name": robotName,
        "requested_workflow_number": requestedWorkflowNumber,
        "active_workflow_number": activeWorkflowNumber,
        "now_epoch_ms": nowEpochMs,
    }
