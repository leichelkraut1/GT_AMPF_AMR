from MainController.Robot.Commands import createWorkflowMission
from MainController.Robot.Commands import emptyMissionCommandSummary
from MainController.Robot.Commands import issueClearMissionCommands
from MainController.Robot.Commands import reserveActiveWorkflow
from MainController.Robot.PlcMirror import buildOutputs
from MainController.State.Paths import RETRY_DELAY_MS
from MainController.State.RobotStore import normalizeRobotState
from MainController.WorkflowConfig import getWorkflowDef
from MainController.WorkflowConfig import isWorkflowAllowedForRobot
from MainController.WorkflowConfig import normalizeWorkflowNumber
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagPaths import getPendingCreateMissionTimeoutMsPath


_DEFAULT_PENDING_CREATE_TIMEOUT_MS = 30000


def _log():
    return system.util.getLogger("MainController.Robot.Decision")


def _requestCleared(workflowNumber):
    return not normalizeWorkflowNumber(workflowNumber)


def _latchedRequestMatches(snapshot):
    currentState = snapshot["current_state"]
    return bool(
        currentState.get("request_latched")
        and normalizeWorkflowNumber(currentState.get("selected_workflow_number"))
        == normalizeWorkflowNumber(snapshot["selected_workflow_number"])
    )


def _createBackoffActive(snapshot):
    currentState = snapshot["current_state"]
    return bool(
        str(currentState.get("last_attempt_action") or "") == "create"
        and int(currentState.get("next_action_allowed_epoch_ms") or 0) > int(snapshot["now_epoch_ms"] or 0)
    )


def _remainingCreateBackoffMs(snapshot):
    currentState = snapshot["current_state"]
    return max(
        0,
        int(currentState.get("next_action_allowed_epoch_ms") or 0) - int(snapshot["now_epoch_ms"] or 0)
    )


def _mergeMessages(*messages):
    normalized = []
    for message in list(messages or []):
        text = str(message or "").strip()
        if text:
            normalized.append(text)
    return "; ".join(normalized)


def _pendingCreateTimeoutMs():
    rawValue = readOptionalTagValue(
        getPendingCreateMissionTimeoutMsPath(),
        _DEFAULT_PENDING_CREATE_TIMEOUT_MS,
    )
    try:
        return max(0, int(rawValue or 0))
    except Exception:
        return _DEFAULT_PENDING_CREATE_TIMEOUT_MS


def _pendingCreateStartEpochMs(snapshot):
    currentState = dict(snapshot.get("current_state") or {})
    startEpochMs = int(currentState.get("pending_create_start_epoch_ms") or 0)
    return startEpochMs if startEpochMs > 0 else None


def _decisionTimestamp(snapshot):
    return timestampString(snapshot["now_epoch_ms"])


def _classifyActiveMissions(snapshot):
    selectedWorkflowNumber = normalizeWorkflowNumber(snapshot["selected_workflow_number"]) or 0
    matching = []
    queuedMismatches = []
    blockingMismatches = []

    for missionRecord in list(snapshot["active_summary"].get("missions") or []):
        missionRecord = dict(missionRecord or {})
        missionWorkflowNumber = normalizeWorkflowNumber(missionRecord.get("workflow_number")) or 0
        missionStatus = str(missionRecord.get("mission_status") or "").upper()

        if selectedWorkflowNumber and missionWorkflowNumber == selectedWorkflowNumber:
            matching.append(missionRecord)
        elif missionStatus == "QUEUED":
            queuedMismatches.append(missionRecord)
        else:
            blockingMismatches.append(missionRecord)

    return {
        "matching": matching,
        "queued_mismatches": queuedMismatches,
        "blocking_mismatches": blockingMismatches,
    }


def _stateUpdates(snapshot, stateName, statePatch=None):
    mergedState = dict(snapshot.get("current_state") or {})
    mergedState.update(dict(statePatch or {}))
    mergedState["state"] = str(stateName or "idle")
    return normalizeRobotState(mergedState)


def _plcOutputs(snapshot, activeWorkflowNumber=None, **flags):
    return buildOutputs(
        snapshot["mirror_inputs"],
        snapshot["active_workflow_number"] if activeWorkflowNumber is None else activeWorkflowNumber,
        **flags
    )


def _withTimestamp(snapshot, patch, recordTimestamp=False):
    patch = dict(patch or {})
    if recordTimestamp:
        patch["last_command_ts"] = _decisionTimestamp(snapshot)
    return patch


def _clearRequestPatch(snapshot, lastResult="", recordTimestamp=False):
    patch = {
        "selected_workflow_number": snapshot["selected_workflow_number"],
        "request_latched": False,
        "mission_created": False,
        "mission_needs_finalized": False,
        "pending_create_start_epoch_ms": 0,
        "last_result": lastResult,
        "last_command_id": "",
        "next_action_allowed_epoch_ms": 0,
        "last_attempt_action": "",
        "retry_count": 0,
    }
    return _withTimestamp(snapshot, patch, recordTimestamp=recordTimestamp)


def _requestPatch(
    snapshot,
    lastResult,
    requestLatched=False,
    missionCreated=False,
    missionNeedsFinalized=False,
    pendingCreateStartEpochMs=None,
    clearPendingCreate=False,
    recordTimestamp=False,
    extra=None,
):
    patch = {
        "selected_workflow_number": snapshot["selected_workflow_number"],
        "request_latched": bool(requestLatched),
        "mission_created": bool(missionCreated),
        "mission_needs_finalized": bool(missionNeedsFinalized),
        "last_result": lastResult,
    }
    if pendingCreateStartEpochMs is not None:
        patch["pending_create_start_epoch_ms"] = pendingCreateStartEpochMs
    elif clearPendingCreate:
        patch["pending_create_start_epoch_ms"] = 0
    patch.update(dict(extra or {}))
    return _withTimestamp(snapshot, patch, recordTimestamp=recordTimestamp)


def _outcome(
    snapshot,
    action,
    message,
    stateName,
    statePatch=None,
    ok=True,
    level="info",
    plcFlags=None,
    activeWorkflowNumber=None,
    commandResult=None,
    missionOps=None,
    data=None,
):
    stateUpdates = _stateUpdates(snapshot, stateName, statePatch)
    normalizedActiveWorkflow = normalizeWorkflowNumber(
        snapshot["active_workflow_number"] if activeWorkflowNumber is None else activeWorkflowNumber
    ) or 0
    flags = {
        "requestReceived": bool(snapshot["selected_workflow_number"]),
        "requestSuccess": False,
        "requestInvalid": False,
        "requestConflict": False,
        "requestRobotNotReady": False,
        "missionNeedsFinalized": bool(stateUpdates.get("mission_needs_finalized")),
    }
    flags.update(dict(plcFlags or {}))
    return {
        "ok": bool(ok),
        "level": str(level or "info"),
        "message": str(message or ""),
        "action": str(action or ""),
        "state": str(stateUpdates.get("state") or ""),
        "selected_workflow_number": normalizeWorkflowNumber(
            stateUpdates.get("selected_workflow_number")
        ) or 0,
        "active_workflow_number": normalizedActiveWorkflow,
        "plc_outputs": _plcOutputs(
            snapshot,
            activeWorkflowNumber=activeWorkflowNumber,
            **flags
        ),
        "command_result": commandResult,
        "mission_ops": missionOps,
        "next_action_allowed_epoch_ms": int(stateUpdates.get("next_action_allowed_epoch_ms") or 0),
        "last_attempt_action": str(stateUpdates.get("last_attempt_action") or ""),
        "retry_count": int(stateUpdates.get("retry_count") or 0),
        "request_latched": bool(stateUpdates.get("request_latched")),
        "mission_created": bool(stateUpdates.get("mission_created")),
        "mission_needs_finalized": bool(stateUpdates.get("mission_needs_finalized")),
        "state_updates": stateUpdates,
        "data": dict(data or {}),
    }


def _plcFaultOutcome(snapshot):
    return _outcome(
        snapshot,
        "plc_comm_fault",
        "Robot [{}] PLC inputs are unhealthy; skipping command evaluation".format(
            snapshot["robot_name"]
        ),
        "fault",
        {
            "last_result": snapshot["plc_inputs"].get("fault_reason") or "plc_input_quality_bad",
            "last_command_ts": _decisionTimestamp(snapshot),
        },
        ok=False,
        level="warn",
        plcFlags={
            "fleetFault": False,
            "plcCommFault": True,
        },
    )


def _activeClearPendingMessage(snapshot):
    activeWorkflowNumber = snapshot["active_workflow_number"]
    selectedWorkflowNumber = snapshot["selected_workflow_number"]
    if _requestCleared(selectedWorkflowNumber):
        return "waiting for FinalizeOk before clearing active missions"
    if activeWorkflowNumber:
        return "waiting for FinalizeOk before clearing workflow {} and switching to {}".format(
            activeWorkflowNumber,
            selectedWorkflowNumber
        )
    return "waiting for FinalizeOk before clearing active missions and starting {}".format(
        selectedWorkflowNumber
    )


def _holdDisabledMessage(snapshot, hasBlockingMismatches=False, hasQueuedMismatches=False):
    if hasBlockingMismatches:
        if _requestCleared(snapshot["selected_workflow_number"]):
            return "Ignition control disabled; active mission clear suppressed"
        return "Ignition control disabled; workflow switch clear suppressed"
    if hasQueuedMismatches:
        return "Ignition control disabled; queued mission cleanup suppressed"
    return "Ignition control disabled; create suppressed"


def _failedSummaryLevel(failedLevels):
    levels = [str(level or "").lower() for level in list(failedLevels or [])]
    return "error" if "error" in levels else "warn"


def _decideActiveMissions(snapshot):
    currentState = snapshot["current_state"]
    selectedWorkflowNumber = snapshot["selected_workflow_number"]
    activeWorkflowNumber = snapshot["active_workflow_number"]
    activeSplit = _classifyActiveMissions(snapshot)
    matching = list(activeSplit["matching"] or [])
    queuedMismatches = list(activeSplit["queued_mismatches"] or [])
    blockingMismatches = list(activeSplit["blocking_mismatches"] or [])
    hasQueuedMismatches = bool(queuedMismatches)
    hasBlockingMismatches = bool(blockingMismatches)
    queuedSummary = emptyMissionCommandSummary()

    if (hasQueuedMismatches or hasBlockingMismatches) and currentState.get("disable_ignition_control"):
        disabledMessage = _holdDisabledMessage(
            snapshot,
            hasBlockingMismatches=hasBlockingMismatches,
            hasQueuedMismatches=hasQueuedMismatches,
        )
        return _outcome(
            snapshot,
            "hold_control_disabled",
            disabledMessage,
            "mission_active",
            _requestPatch(
                snapshot,
                disabledMessage,
                missionCreated=True,
                missionNeedsFinalized=hasBlockingMismatches,
                clearPendingCreate=True,
            ),
            level="warn",
            missionOps={"queued_summary": queuedSummary},
        )

    queuedSummary = issueClearMissionCommands(
        snapshot,
        queuedMismatches,
        selectedWorkflowNumber,
        activeWorkflowNumber,
    )
    queuedMessage = str(queuedSummary.get("message") or "")

    if hasBlockingMismatches:
        if not snapshot["plc_inputs"].get("finalize_ok"):
            pendingMessage = _mergeMessages(queuedMessage, _activeClearPendingMessage(snapshot))
            return _outcome(
                snapshot,
                "hold_clear_pending",
                pendingMessage,
                "mission_active",
                _requestPatch(
                    snapshot,
                    pendingMessage,
                    missionCreated=True,
                    missionNeedsFinalized=True,
                    clearPendingCreate=True,
                ),
                missionOps={"queued_summary": queuedSummary},
            )

        blockingSummary = issueClearMissionCommands(
            snapshot,
            blockingMismatches,
            selectedWorkflowNumber,
            activeWorkflowNumber,
        )
        anyFailures = bool(queuedSummary.get("any_failures") or blockingSummary.get("any_failures"))
        failedLevels = list(queuedSummary.get("failed_levels") or []) + list(blockingSummary.get("failed_levels") or [])
        issuedCount = int(queuedSummary.get("issued_count") or 0) + int(blockingSummary.get("issued_count") or 0)
        mergedMessage = _mergeMessages(queuedMessage, blockingSummary.get("message"))
        return _outcome(
            snapshot,
            "clear_reconcile_failed" if anyFailures else ("clear_reconcile" if issuedCount else "hold_clear_inflight"),
            mergedMessage,
            "fault" if anyFailures else "mission_active",
            _requestPatch(
                snapshot,
                mergedMessage,
                missionCreated=True,
                missionNeedsFinalized=True,
                clearPendingCreate=True,
                recordTimestamp=bool(issuedCount or anyFailures),
            ),
            ok=not anyFailures,
            level=_failedSummaryLevel(failedLevels) if anyFailures else "info",
            missionOps={
                "queued_summary": queuedSummary,
                "blocking_summary": blockingSummary,
            },
        )

    if queuedSummary.get("any_failures"):
        message = queuedMessage or "queued mission cleanup failed"
        return _outcome(
            snapshot,
            "clear_reconcile_failed",
            message,
            "fault",
            _requestPatch(
                snapshot,
                message,
                missionCreated=True,
                clearPendingCreate=True,
                recordTimestamp=True,
            ),
            ok=False,
            level=_failedSummaryLevel(queuedSummary.get("failed_levels")),
            missionOps={"queued_summary": queuedSummary},
        )

    if matching:
        lastResult = _mergeMessages("active mission matches requested workflow", queuedMessage)
        return _outcome(
            snapshot,
            "hold_active",
            "Robot [{}] active workflow {} is in progress".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            "mission_active",
            _requestPatch(
                snapshot,
                lastResult,
                requestLatched=True,
                missionCreated=True,
                clearPendingCreate=True,
            ),
            plcFlags={"requestSuccess": True},
            missionOps={"queued_summary": queuedSummary},
            activeWorkflowNumber=selectedWorkflowNumber,
        )

    message = queuedMessage or "active mission present"
    return _outcome(
        snapshot,
        "hold_clear_inflight",
        message,
        "mission_active",
        _requestPatch(
            snapshot,
            message,
            missionCreated=True,
            clearPendingCreate=True,
            recordTimestamp=bool(queuedSummary.get("issued_count")),
        ),
        missionOps={"queued_summary": queuedSummary},
    )


def _decideNoActiveMissions(snapshot):
    currentState = dict(snapshot["current_state"] or {})
    selectedWorkflowNumber = snapshot["selected_workflow_number"]
    if currentState.get("mission_needs_finalized"):
        currentState["request_latched"] = False
        currentState["mission_created"] = False
        currentState["mission_needs_finalized"] = False
        currentState["pending_create_start_epoch_ms"] = 0
        currentState["last_result"] = ""
        currentState["last_command_id"] = ""
        snapshot = dict(snapshot or {})
        snapshot["current_state"] = currentState

    if _requestCleared(selectedWorkflowNumber):
        return _outcome(
            snapshot,
            "idle",
            "Robot [{}] idle".format(snapshot["robot_name"]),
            "idle",
            _clearRequestPatch(snapshot, ""),
        )

    if currentState.get("disable_ignition_control"):
        return _outcome(
            snapshot,
            "hold_control_disabled",
            "Robot [{}] create suppressed while Ignition control is disabled".format(snapshot["robot_name"]),
            "mission_requested",
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": _holdDisabledMessage(snapshot, False),
            },
            level="warn",
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, snapshot["robot_name"]):
        return _outcome(
            snapshot,
            "request_invalid",
            "Robot [{}] requested invalid workflow {}".format(snapshot["robot_name"], selectedWorkflowNumber),
            "fault",
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "workflow {} invalid for {}".format(selectedWorkflowNumber, snapshot["robot_name"]),
            },
            ok=False,
            level="warn",
            plcFlags={"requestInvalid": True},
        )

    owner = snapshot["reserved_workflows"].get(selectedWorkflowNumber)
    if owner and owner != snapshot["robot_name"]:
        return _outcome(
            snapshot,
            "request_conflict",
            "Robot [{}] workflow {} conflicts with {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber,
                owner
            ),
            "fault",
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
            },
            ok=False,
            level="warn",
            plcFlags={"requestConflict": True},
        )

    if _latchedRequestMatches(snapshot):
        if currentState.get("mission_created"):
            pendingCreateStartEpochMs = _pendingCreateStartEpochMs(snapshot)
            if pendingCreateStartEpochMs is None:
                pendingCreateStartEpochMs = int(snapshot.get("now_epoch_ms") or 0)
            pendingCreateAgeMs = max(
                0,
                int(snapshot.get("now_epoch_ms") or 0) - int(pendingCreateStartEpochMs or 0)
            )
            pendingCreateTimeoutMs = _pendingCreateTimeoutMs()
            if pendingCreateAgeMs is None or pendingCreateAgeMs < pendingCreateTimeoutMs:
                return _outcome(
                    snapshot,
                    "hold_request",
                    "Robot [{}] waiting for created mission to appear in fleet".format(
                        snapshot["robot_name"]
                    ),
                    "mission_requested",
                    _requestPatch(
                        snapshot,
                        "waiting for created mission to appear in fleet",
                        requestLatched=True,
                        missionCreated=True,
                        pendingCreateStartEpochMs=pendingCreateStartEpochMs,
                    ),
                    plcFlags={"requestSuccess": True},
                )

            timeoutMessage = (
                "created mission did not appear within {} ms; cleared stale request latch"
            ).format(pendingCreateTimeoutMs)
            _log().warn(
                "Robot [{}] {}".format(
                    snapshot["robot_name"],
                    timeoutMessage,
                )
            )
            return _outcome(
                snapshot,
                "hold_request_timeout",
                "Robot [{}] {}".format(snapshot["robot_name"], timeoutMessage),
                "fault",
                dict(
                    _clearRequestPatch(snapshot, timeoutMessage, recordTimestamp=True),
                    next_action_allowed_epoch_ms=int(snapshot["now_epoch_ms"] or 0) + RETRY_DELAY_MS,
                    last_attempt_action="create",
                    retry_count=int(currentState.get("retry_count") or 0) + 1,
                ),
                level="warn",
            )

        return _outcome(
            snapshot,
            "hold_request",
            "Robot [{}] holding requested workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            "mission_requested",
            _requestPatch(
                snapshot,
                currentState.get("last_result") or "waiting for active mission reconciliation",
                requestLatched=True,
                missionCreated=bool(currentState.get("mission_created")),
            ),
            plcFlags={"requestSuccess": bool(currentState.get("mission_created"))},
        )

    if not snapshot["controller_available_for_work"]:
        return _outcome(
            snapshot,
            "waiting_available",
            "Robot [{}] is not available for workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            "fault",
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "robot not available for work",
            },
            ok=False,
            level="warn",
            plcFlags={"requestRobotNotReady": True},
        )

    if _createBackoffActive(snapshot):
        return _outcome(
            snapshot,
            "hold_create_backoff",
            "Robot [{}] waiting before retrying create".format(snapshot["robot_name"]),
            "fault",
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "waiting {} ms before retrying create".format(_remainingCreateBackoffMs(snapshot)),
            },
            ok=False,
            level="warn",
        )

    commandId = str(snapshot["now_epoch_ms"])
    commandResult = createWorkflowMission(snapshot, selectedWorkflowNumber)
    createSucceeded = bool(commandResult.get("ok"))
    return _outcome(
        snapshot,
        "create" if createSucceeded else "create_failed",
        commandResult.get("message", ""),
        "mission_requested" if createSucceeded else "fault",
        _requestPatch(
            snapshot,
            commandResult.get("message", ""),
            requestLatched=createSucceeded,
            missionCreated=createSucceeded,
            pendingCreateStartEpochMs=snapshot["now_epoch_ms"] if createSucceeded else 0,
            recordTimestamp=True,
            extra={
                "last_command_id": commandId,
                "next_action_allowed_epoch_ms": 0
                if createSucceeded
                else snapshot["now_epoch_ms"] + RETRY_DELAY_MS,
                "last_attempt_action": "" if createSucceeded else "create",
                "retry_count": 0 if createSucceeded else int(currentState.get("retry_count") or 0) + 1,
            },
        ),
        ok=createSucceeded,
        level=commandResult.get("level", "info"),
        plcFlags={"requestSuccess": createSucceeded},
        commandResult=commandResult,
        data={"workflow_number": selectedWorkflowNumber},
    )


def decideRobotWorkflowCycleSnapshot(snapshot):
    """Return one normalized robot workflow outcome without applying tag writes."""
    snapshot = dict(snapshot or {})
    reserveActiveWorkflow(snapshot)

    if not snapshot["plc_healthy"]:
        return _plcFaultOutcome(snapshot)
    if list(snapshot["active_summary"].get("missions") or []):
        return _decideActiveMissions(snapshot)
    return _decideNoActiveMissions(snapshot)
