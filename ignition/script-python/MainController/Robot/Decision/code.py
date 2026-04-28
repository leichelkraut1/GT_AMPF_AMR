from MainController.Robot.PlcMirror import buildOutputs
from MainController.Robot.Records import _coerceRobotCycleSnapshot
from MainController.State.Paths import RETRY_DELAY_MS
from MainController.State.RobotStore import normalizeRobotState
from MainController.WorkflowConfig import getWorkflowDef
from MainController.WorkflowConfig import isWorkflowAllowedForRobot
from MainController.WorkflowConfig import normalizeWorkflowNumber
from Otto_API.Common.RuntimeHistory import timestampString


_DEFAULT_PENDING_CREATE_TIMEOUT_MS = 30000


def _log():
    return system.util.getLogger("MainController.Robot.Decision")


def _requestCleared(workflowNumber):
    return not normalizeWorkflowNumber(workflowNumber)


def _latchedRequestMatches(snapshot):
    currentState = snapshot.current_state
    return bool(
        currentState.request_latched
        and normalizeWorkflowNumber(currentState.selected_workflow_number)
        == normalizeWorkflowNumber(snapshot.selected_workflow_number)
    )


def _createBackoffActive(snapshot):
    currentState = snapshot.current_state
    return bool(
        str(currentState.last_attempt_action or "") == "create"
        and int(currentState.next_action_allowed_epoch_ms or 0) > int(snapshot.now_epoch_ms or 0)
    )


def _remainingCreateBackoffMs(snapshot):
    currentState = snapshot.current_state
    return max(
        0,
        int(currentState.next_action_allowed_epoch_ms or 0) - int(snapshot.now_epoch_ms or 0)
    )


def _mergeMessages(*messages):
    normalized = []
    for message in list(messages or []):
        text = str(message or "").strip()
        if text:
            normalized.append(text)
    return "; ".join(normalized)


def _emptyMissionCommandSummary():
    return {
        "finalized_count": 0,
        "canceled_count": 0,
        "skipped_count": 0,
        "failed_messages": [],
        "failed_levels": [],
        "issued_count": 0,
        "any_failures": False,
        "message": "",
    }


def _commandSummary(commandResults, requestName):
    return dict(
        dict(commandResults or {}).get(requestName)
        or _emptyMissionCommandSummary()
    )


def _pendingCreateTimeoutMs(snapshot):
    try:
        return max(0, int(snapshot.pending_create_timeout_ms or 0))
    except Exception:
        return _DEFAULT_PENDING_CREATE_TIMEOUT_MS


def _pendingCreateStartEpochMs(snapshot):
    startEpochMs = int(snapshot.current_state.pending_create_start_epoch_ms or 0)
    return startEpochMs if startEpochMs > 0 else None


def _decisionTimestamp(snapshot):
    return timestampString(snapshot.now_epoch_ms)


def _classifyActiveMissions(snapshot):
    selectedWorkflowNumber = normalizeWorkflowNumber(snapshot.selected_workflow_number) or 0
    matching = []
    queuedMismatches = []
    blockingMismatches = []

    for missionRecord in list(snapshot.active_summary.missions or []):
        missionWorkflowNumber = normalizeWorkflowNumber(missionRecord.workflow_number) or 0
        missionStatus = str(missionRecord.mission_status or "").upper()

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
    mergedState = snapshot.current_state.toDict()
    mergedState.update(dict(statePatch or {}))
    mergedState["state"] = str(stateName or "idle")
    return normalizeRobotState(mergedState)


def _plcOutputs(snapshot, activeWorkflowNumber=None, **flags):
    return buildOutputs(
        snapshot.mirror_inputs,
        snapshot.active_workflow_number if activeWorkflowNumber is None else activeWorkflowNumber,
        **flags
    )


def _withTimestamp(snapshot, patch, recordTimestamp=False):
    patch = dict(patch or {})
    if recordTimestamp:
        patch["last_command_ts"] = _decisionTimestamp(snapshot)
    return patch


def _clearRequestPatch(snapshot, lastResult="", recordTimestamp=False):
    patch = {
        "selected_workflow_number": snapshot.selected_workflow_number,
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


def _selectedWorkflowPatch(snapshot, lastResult):
    return {
        "selected_workflow_number": snapshot.selected_workflow_number,
        "last_result": lastResult,
    }


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
        "selected_workflow_number": snapshot.selected_workflow_number,
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
        snapshot.active_workflow_number if activeWorkflowNumber is None else activeWorkflowNumber
    ) or 0
    flags = {
        "requestReceived": bool(snapshot.selected_workflow_number),
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


def _plan(outcome=None, commandRequests=None, resolver=None, data=None):
    if resolver is None and outcome is not None:
        resolver = _resolveStaticOutcome
    return {
        "outcome": outcome,
        "command_requests": list(commandRequests or []),
        "resolver": resolver,
        "data": dict(data or {}),
    }


def _clearMissionsRequest(
    requestName,
    missions,
    selectedWorkflowNumber,
    activeWorkflowNumber
):
    return {
        "name": str(requestName or ""),
        "type": "clear_missions",
        "missions": list(missions or []),
        "selected_workflow_number": selectedWorkflowNumber,
        "active_workflow_number": activeWorkflowNumber,
    }


def _createWorkflowMissionRequest(requestName, workflowNumber):
    return {
        "name": str(requestName or ""),
        "type": "create_workflow_mission",
        "workflow_number": workflowNumber,
    }


def _resolveStaticOutcome(snapshot, plan, commandResults):
    return dict(plan.get("outcome") or {})


def _outcomePlan(*args, **kwargs):
    return _plan(outcome=_outcome(*args, **kwargs))


def _plcFaultOutcome(snapshot):
    return _outcome(
        snapshot,
        "plc_comm_fault",
        "Robot [{}] PLC inputs are unhealthy; skipping command evaluation".format(
            snapshot.robot_name
        ),
        "fault",
        {
            "last_result": snapshot.plc_inputs.fault_reason or "plc_input_quality_bad",
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
    activeWorkflowNumber = snapshot.active_workflow_number
    selectedWorkflowNumber = snapshot.selected_workflow_number
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
        if _requestCleared(snapshot.selected_workflow_number):
            return "Ignition control disabled; active mission clear suppressed"
        return "Ignition control disabled; workflow switch clear suppressed"
    if hasQueuedMismatches:
        return "Ignition control disabled; queued mission cleanup suppressed"
    return "Ignition control disabled; create suppressed"


def _failedSummaryLevel(failedLevels):
    levels = [str(level or "").lower() for level in list(failedLevels or [])]
    return "error" if "error" in levels else "warn"


def _activeCommandRequests(
    queuedMismatches,
    blockingMismatches,
    selectedWorkflowNumber,
    activeWorkflowNumber,
    includeBlocking=False
):
    requests = []
    if queuedMismatches:
        requests.append(
            _clearMissionsRequest(
                "queued_clear",
                queuedMismatches,
                selectedWorkflowNumber,
                activeWorkflowNumber,
            )
        )
    if includeBlocking and blockingMismatches:
        requests.append(
            _clearMissionsRequest(
                "blocking_clear",
                blockingMismatches,
                selectedWorkflowNumber,
                activeWorkflowNumber,
            )
        )
    return requests


def _activePlanData(plan):
    return dict(dict(plan or {}).get("data") or {})


def _activeQueuedSummary(commandResults):
    return _commandSummary(commandResults, "queued_clear")


def _resolveActiveClearPending(snapshot, plan, commandResults):
    queuedSummary = _commandSummary(commandResults, "queued_clear")
    queuedMessage = str(queuedSummary.get("message") or "")
    pendingMessage = _mergeMessages(
        queuedMessage,
        _activeClearPendingMessage(snapshot),
    )
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


def _resolveActiveBlockingClear(snapshot, plan, commandResults):
    queuedSummary = _commandSummary(commandResults, "queued_clear")
    blockingSummary = _commandSummary(commandResults, "blocking_clear")
    queuedMessage = str(queuedSummary.get("message") or "")
    anyFailures = bool(
        queuedSummary.get("any_failures")
        or blockingSummary.get("any_failures")
    )
    failedLevels = (
        list(queuedSummary.get("failed_levels") or [])
        + list(blockingSummary.get("failed_levels") or [])
    )
    issuedCount = (
        int(queuedSummary.get("issued_count") or 0)
        + int(blockingSummary.get("issued_count") or 0)
    )
    mergedMessage = _mergeMessages(queuedMessage, blockingSummary.get("message"))
    return _outcome(
        snapshot,
        "clear_reconcile_failed" if anyFailures else (
            "clear_reconcile" if issuedCount else "hold_clear_inflight"
        ),
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


def _resolveActiveNoBlocking(snapshot, plan, commandResults):
    selectedWorkflowNumber = snapshot.selected_workflow_number
    queuedSummary = _activeQueuedSummary(commandResults)
    queuedMessage = str(queuedSummary.get("message") or "")
    matching = list(_activePlanData(plan).get("matching") or [])

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
        lastResult = _mergeMessages(
            "active mission matches requested workflow",
            queuedMessage,
        )
        return _outcome(
            snapshot,
            "hold_active",
            "Robot [{}] active workflow {} is in progress".format(
                snapshot.robot_name,
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


def _planActiveMissions(snapshot):
    currentState = snapshot.current_state
    selectedWorkflowNumber = snapshot.selected_workflow_number
    activeWorkflowNumber = snapshot.active_workflow_number
    activeSplit = _classifyActiveMissions(snapshot)
    matching = list(activeSplit["matching"] or [])
    queuedMismatches = list(activeSplit["queued_mismatches"] or [])
    blockingMismatches = list(activeSplit["blocking_mismatches"] or [])
    hasQueuedMismatches = bool(queuedMismatches)
    hasBlockingMismatches = bool(blockingMismatches)
    queuedSummary = _emptyMissionCommandSummary()

    if (hasQueuedMismatches or hasBlockingMismatches) and currentState.disable_ignition_control:
        disabledMessage = _holdDisabledMessage(
            snapshot,
            hasBlockingMismatches=hasBlockingMismatches,
            hasQueuedMismatches=hasQueuedMismatches,
        )
        return _outcomePlan(
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

    commandRequests = _activeCommandRequests(
        queuedMismatches,
        blockingMismatches,
        selectedWorkflowNumber,
        activeWorkflowNumber,
        includeBlocking=hasBlockingMismatches and bool(snapshot.plc_inputs.finalize_ok),
    )

    if hasBlockingMismatches:
        if not snapshot.plc_inputs.finalize_ok:
            return _plan(
                commandRequests=commandRequests,
                resolver=_resolveActiveClearPending,
            )
        return _plan(
            commandRequests=commandRequests,
            resolver=_resolveActiveBlockingClear,
        )

    return _plan(
        commandRequests=commandRequests,
        resolver=_resolveActiveNoBlocking,
        data={"matching": matching},
    )


def _resolveCreateWorkflowMission(snapshot, plan, commandResults):
    currentState = snapshot.current_state
    selectedWorkflowNumber = snapshot.selected_workflow_number
    commandResult = dict(dict(commandResults or {}).get("create") or {})
    createSucceeded = bool(commandResult.get("ok"))
    commandId = str(snapshot.now_epoch_ms)
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
            pendingCreateStartEpochMs=snapshot.now_epoch_ms if createSucceeded else 0,
            recordTimestamp=True,
            extra={
                "last_command_id": commandId,
                "next_action_allowed_epoch_ms": 0
                if createSucceeded
                else snapshot.now_epoch_ms + RETRY_DELAY_MS,
                "last_attempt_action": "" if createSucceeded else "create",
                "retry_count": 0 if createSucceeded else int(currentState.retry_count or 0) + 1,
            },
        ),
        ok=createSucceeded,
        level=commandResult.get("level", "info"),
        plcFlags={"requestSuccess": createSucceeded},
        commandResult=commandResult,
        data={"workflow_number": selectedWorkflowNumber},
    )


def _planNoActiveMissions(snapshot):
    currentState = snapshot.current_state
    selectedWorkflowNumber = snapshot.selected_workflow_number
    if currentState.mission_needs_finalized:
        currentStatePatch = currentState.toDict()
        currentStatePatch["request_latched"] = False
        currentStatePatch["mission_created"] = False
        currentStatePatch["mission_needs_finalized"] = False
        currentStatePatch["pending_create_start_epoch_ms"] = 0
        currentStatePatch["last_result"] = ""
        currentStatePatch["last_command_id"] = ""
        snapshot = snapshot.cloneWith(current_state=currentStatePatch)
        currentState = snapshot.current_state

    if _requestCleared(selectedWorkflowNumber):
        return _outcomePlan(
            snapshot,
            "idle",
            "Robot [{}] idle".format(snapshot.robot_name),
            "idle",
            _clearRequestPatch(snapshot, ""),
        )

    if currentState.disable_ignition_control:
        return _outcomePlan(
            snapshot,
            "hold_control_disabled",
            "Robot [{}] create suppressed while Ignition control is disabled".format(snapshot.robot_name),
            "mission_requested",
            _selectedWorkflowPatch(snapshot, _holdDisabledMessage(snapshot, False)),
            level="warn",
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, snapshot.robot_name):
        return _outcomePlan(
            snapshot,
            "request_invalid",
            "Robot [{}] requested invalid workflow {}".format(snapshot.robot_name, selectedWorkflowNumber),
            "fault",
            _selectedWorkflowPatch(
                snapshot,
                "workflow {} invalid for {}".format(selectedWorkflowNumber, snapshot.robot_name),
            ),
            ok=False,
            level="warn",
            plcFlags={"requestInvalid": True},
        )

    owner = snapshot.reserved_workflows.get(selectedWorkflowNumber)
    if owner and owner != snapshot.robot_name:
        return _outcomePlan(
            snapshot,
            "request_conflict",
            "Robot [{}] workflow {} conflicts with {}".format(
                snapshot.robot_name,
                selectedWorkflowNumber,
                owner
            ),
            "fault",
            _selectedWorkflowPatch(
                snapshot,
                "workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
            ),
            ok=False,
            level="warn",
            plcFlags={"requestConflict": True},
        )

    if _latchedRequestMatches(snapshot):
        if currentState.mission_created:
            pendingCreateStartEpochMs = _pendingCreateStartEpochMs(snapshot)
            if pendingCreateStartEpochMs is None:
                pendingCreateStartEpochMs = int(snapshot.now_epoch_ms or 0)
            pendingCreateAgeMs = max(
                0,
                int(snapshot.now_epoch_ms or 0) - int(pendingCreateStartEpochMs or 0)
            )
            pendingCreateTimeoutMs = _pendingCreateTimeoutMs(snapshot)
            if pendingCreateAgeMs is None or pendingCreateAgeMs < pendingCreateTimeoutMs:
                return _outcomePlan(
                    snapshot,
                    "hold_request",
                    "Robot [{}] waiting for created mission to appear in fleet".format(
                        snapshot.robot_name
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
                    snapshot.robot_name,
                    timeoutMessage,
                )
            )
            return _outcomePlan(
                snapshot,
                "hold_request_timeout",
                "Robot [{}] {}".format(snapshot.robot_name, timeoutMessage),
                "fault",
                dict(
                    _clearRequestPatch(snapshot, timeoutMessage, recordTimestamp=True),
                    next_action_allowed_epoch_ms=int(snapshot.now_epoch_ms or 0) + RETRY_DELAY_MS,
                    last_attempt_action="create",
                    retry_count=int(currentState.retry_count or 0) + 1,
                ),
                level="warn",
            )

        return _outcomePlan(
            snapshot,
            "hold_request",
            "Robot [{}] holding requested workflow {}".format(
                snapshot.robot_name,
                selectedWorkflowNumber
            ),
            "mission_requested",
            _requestPatch(
                snapshot,
                currentState.last_result or "waiting for active mission reconciliation",
                requestLatched=True,
                missionCreated=bool(currentState.mission_created),
            ),
            plcFlags={"requestSuccess": bool(currentState.mission_created)},
        )

    if not snapshot.controller_available_for_work:
        return _outcomePlan(
            snapshot,
            "waiting_available",
            "Robot [{}] is not available for workflow {}".format(
                snapshot.robot_name,
                selectedWorkflowNumber
            ),
            "fault",
            _selectedWorkflowPatch(snapshot, "robot not available for work"),
            ok=False,
            level="warn",
            plcFlags={"requestRobotNotReady": True},
        )

    if _createBackoffActive(snapshot):
        return _outcomePlan(
            snapshot,
            "hold_create_backoff",
            "Robot [{}] waiting before retrying create".format(snapshot.robot_name),
            "fault",
            _selectedWorkflowPatch(
                snapshot,
                "waiting {} ms before retrying create".format(_remainingCreateBackoffMs(snapshot)),
            ),
            ok=False,
            level="warn",
        )

    return _plan(
        commandRequests=[
            _createWorkflowMissionRequest("create", selectedWorkflowNumber),
        ],
        resolver=_resolveCreateWorkflowMission,
    )


def planRobotWorkflowCycleSnapshot(snapshot):
    """Return one pure robot workflow plan without executing mission commands."""
    snapshot = _coerceRobotCycleSnapshot(snapshot)

    if not snapshot.plc_inputs.healthy:
        return _plan(outcome=_plcFaultOutcome(snapshot))
    if list(snapshot.active_summary.missions or []):
        return _planActiveMissions(snapshot)
    return _planNoActiveMissions(snapshot)


def resolveRobotWorkflowDecision(snapshot, plan, commandResults=None):
    """Resolve one plan plus command results into the normalized apply outcome."""
    snapshot = _coerceRobotCycleSnapshot(snapshot)
    plan = dict(plan or {})
    resolver = plan.get("resolver") or _resolveStaticOutcome
    return resolver(snapshot, plan, dict(commandResults or {}))
