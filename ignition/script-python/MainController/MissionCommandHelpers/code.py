from Otto_API.Common.RuntimeHistory import appendRuntimeDatasetRow
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_HEADERS
from Otto_API.Common.RuntimeHistory import COMMAND_HISTORY_MAX_ROWS
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagIO import writeRequiredTagValues
from MainController.Robot.Actions import callMissionCommand
from MainController.WorkflowConfig import normalizeWorkflowNumber


MISSION_LAST_ISSUED_COMMAND_SIGNATURE_MEMBER = "_LastIssuedCommandSignature"
MISSION_LAST_COMMAND_LOG_SIGNATURE_MEMBER = "_LastCommandLogSignature"

_MISSION_RUNTIME_KEYS = {
    "last_issued_command_signature": MISSION_LAST_ISSUED_COMMAND_SIGNATURE_MEMBER,
    "last_command_log_signature": MISSION_LAST_COMMAND_LOG_SIGNATURE_MEMBER,
}
_WARNED_MISSING_MISSION_RUNTIME_MEMBERS = set()


def _log():
    return system.util.getLogger("MainController_WorkflowRunner")


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


def _missionRuntimeTagPath(instancePath, keyName, warn=True):
    """Return one optional mission runtime helper tag path when the UDT member exists."""
    instancePath = str(instancePath or "")
    memberName = _MISSION_RUNTIME_KEYS.get(str(keyName or ""))
    if not instancePath or not memberName:
        return None

    tagPath = instancePath + "/" + memberName
    if tagExists(tagPath):
        return tagPath
    if warn:
        _warnMissingMissionRuntimeMember(memberName)
    return None


def _readMissionRuntimeValue(context, keyName):
    tagPath = _missionRuntimeTagPath(context.get("instance_path"), keyName)
    if not tagPath:
        return ""
    return str(readOptionalTagValue(tagPath, "", allowEmptyString=True) or "")


def _writeMissionRuntimeValue(context, keyName, value):
    tagPath = _missionRuntimeTagPath(context.get("instance_path"), keyName)
    if not tagPath:
        return False
    writeRequiredTagValues(
        [tagPath],
        [str(value or "")],
        labels=["MainController mission runtime helper"]
    )
    return True


def _missionContext(missionRecord):
    missionName = str(missionRecord.name or "")
    missionId = str(missionRecord.id or "")
    instancePath = str(missionRecord.instance_path or missionRecord.path or "")
    return {
        "record": missionRecord,
        "instance_path": instancePath,
        "mission_name": missionName,
        "mission_id": missionId,
        "mission_label": missionName or missionId or instancePath or "mission",
        "workflow_number": normalizeWorkflowNumber(missionRecord.workflow_number) or 0,
    }


def _commandLogSignature(actionName, result):
    return "|".join([
        str(actionName or ""),
        "ok" if bool((result or {}).get("ok")) else "fail",
        str((result or {}).get("level") or ""),
    ])


def _hasCommandAlreadyIssued(context, actionName):
    signature = _readMissionRuntimeValue(
        context,
        "last_issued_command_signature",
    )
    return signature == str(actionName or "")


def _markCommandIssued(context, actionName):
    return _writeMissionRuntimeValue(
        context,
        "last_issued_command_signature",
        str(actionName or ""),
    )


def _recordMissionCommandHistory(
    nowEpochMs,
    robotName,
    context,
    requestedWorkflowNumber,
    activeWorkflowNumber,
    actionName,
    result,
):
    """Append one explicit mission cancel/finalize row unless the log signature matches."""
    logSignature = _commandLogSignature(actionName, result)
    currentSignature = _readMissionRuntimeValue(
        context,
        "last_command_log_signature",
    )
    if currentSignature == logSignature:
        return

    message = str((result or {}).get("message") or "")
    missionBits = []
    if context["mission_name"]:
        missionBits.append(context["mission_name"])
    if context["mission_id"]:
        missionBits.append(context["mission_id"])
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
            context["workflow_number"]
            or normalizeWorkflowNumber(activeWorkflowNumber)
            or 0,
            actionName,
            (result or {}).get("level") or "",
            "mission_active",
            message,
        ],
        maxRows=COMMAND_HISTORY_MAX_ROWS,
    )
    _writeMissionRuntimeValue(
        context,
        "last_command_log_signature",
        logSignature,
    )


def _clearTargetActionName(missionRecord):
    """Choose the clear action for one active mission record."""
    missionStatus = str(missionRecord.mission_status or "").upper()
    if missionStatus == "STARVED":
        return "finalize_mission"
    return "cancel_mission"


def _dispatchMissionCommand(context, actionName, finalizeMissionId=None, cancelMissionIds=None):
    if not context["mission_id"]:
        return {
            "ok": False,
            "level": "warn",
            "message": "Cannot {} because [{}] is missing its mission id".format(
                actionName.replace("_", " "),
                context["mission_label"]
            ),
        }
    return callMissionCommand(
        actionName,
        context["mission_id"],
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )


def _summaryMessage(summary):
    finalizedCount = int(summary.get("finalized_count") or 0)
    canceledCount = int(summary.get("canceled_count") or 0)
    skippedCount = int(summary.get("skipped_count") or 0)
    failedMessages = list(summary.get("failed_messages") or [])
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
                "; ".join(failedMessages[:2])
            )
        )
    return "; ".join(parts or ["waiting for cleared missions to disappear"])


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
    summary = {
        "finalized_count": 0,
        "canceled_count": 0,
        "skipped_count": 0,
        "failed_messages": [],
        "failed_levels": [],
    }

    for missionRecord in list(missionRecords or []):
        context = _missionContext(missionRecord)
        actionName = _clearTargetActionName(context["record"])
        if _hasCommandAlreadyIssued(context, actionName):
            summary["skipped_count"] += 1
            continue

        result = _dispatchMissionCommand(
            context,
            actionName,
            finalizeMissionId=finalizeMissionId,
            cancelMissionIds=cancelMissionIds,
        )
        _recordMissionCommandHistory(
            nowEpochMs,
            robotName,
            context,
            requestedWorkflowNumber,
            activeWorkflowNumber,
            actionName,
            result,
        )

        if result.get("ok"):
            _markCommandIssued(context, actionName)
            if actionName == "finalize_mission":
                summary["finalized_count"] += 1
            else:
                summary["canceled_count"] += 1
        else:
            summary["failed_levels"].append(str(result.get("level") or "warn"))
            summary["failed_messages"].append(
                str(result.get("message") or "mission clear failed")
            )

    finalizedCount = int(summary.get("finalized_count") or 0)
    canceledCount = int(summary.get("canceled_count") or 0)
    failedMessages = list(summary.get("failed_messages") or [])
    return dict(
        summary,
        issued_count=finalizedCount + canceledCount,
        any_failures=bool(failedMessages),
        message=_summaryMessage(summary),
        robot_name=robotName,
        requested_workflow_number=requestedWorkflowNumber,
        active_workflow_number=activeWorkflowNumber,
        now_epoch_ms=nowEpochMs,
    )
