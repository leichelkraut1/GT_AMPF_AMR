from MainController.MissionCommandHelpers import issueMissionCommands
from MainController.Robot.Actions import callCreateMission
from MainController.Robot.Apply import applyRobotOutcome
from MainController.Robot.PlcMirror import buildOutputs
from MainController.Robot.Snapshot import readRobotCycleSnapshot
from MainController.State.RobotStore import normalizeRobotState
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.TagHelpers import getPendingCreateMissionTimeoutMsPath
from Otto_API.Common.TagHelpers import readOptionalTagValue
from MainController.State.Paths import RETRY_DELAY_MS
from MainController.WorkflowConfig import getWorkflowDef
from MainController.WorkflowConfig import isWorkflowAllowedForRobot
from MainController.WorkflowConfig import normalizeWorkflowNumber


_DEFAULT_PENDING_CREATE_TIMEOUT_MS = 30000


def _log():
    return system.util.getLogger("MainController.Robot.Cycle")


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


def _pendingCreateAgeMs(snapshot):
    startEpochMs = _pendingCreateStartEpochMs(snapshot)
    if startEpochMs is None:
        return None
    return max(0, int(snapshot.get("now_epoch_ms") or 0) - startEpochMs)


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


def _buildOutcome(
    snapshot,
    ok,
    level,
    message,
    action,
    stateName,
    stateUpdates=None,
    plcOutputs=None,
    plcHealthOutputs=None,
    activeWorkflowNumber=None,
    commandResult=None,
    missionOps=None,
    data=None,
):
    normalizedActiveWorkflow = normalizeWorkflowNumber(
        snapshot["active_workflow_number"] if activeWorkflowNumber is None else activeWorkflowNumber
    ) or 0
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
        "plc_outputs": plcOutputs,
        "plc_health_outputs": plcHealthOutputs,
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
    stateUpdates = _stateUpdates(
        snapshot,
        "fault",
        {
            "last_result": snapshot["plc_inputs"].get("fault_reason") or "plc_input_quality_bad",
            "last_command_ts": _decisionTimestamp(snapshot),
        },
    )
    return _buildOutcome(
        snapshot,
        False,
        "warn",
        "Robot [{}] PLC inputs are unhealthy; skipping command evaluation".format(snapshot["robot_name"]),
        "plc_comm_fault",
        "fault",
        stateUpdates=stateUpdates,
        plcHealthOutputs={
            "fleetFault": False,
            "plcCommFault": True,
            "controlHealthy": False,
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


def _activeMissionOutcome(
    snapshot,
    action,
    message,
    statePatch,
    stateName="mission_active",
    level="info",
    ok=True,
    requestSuccess=False,
    missionOps=None,
    activeWorkflowNumber=None,
):
    if activeWorkflowNumber is None:
        activeWorkflowNumber = snapshot["active_workflow_number"]
    stateUpdates = _stateUpdates(
        snapshot,
        stateName,
        statePatch,
    )
    return _buildOutcome(
        snapshot,
        ok,
        level,
        message,
        action,
        stateName,
        stateUpdates=stateUpdates,
        plcOutputs=_plcOutputs(
            snapshot,
            requestReceived=bool(snapshot["selected_workflow_number"]),
            requestSuccess=requestSuccess,
            missionNeedsFinalized=bool(stateUpdates.get("mission_needs_finalized"))
        ),
        activeWorkflowNumber=activeWorkflowNumber,
        missionOps=missionOps,
    )


def _noActiveMissionOutcome(
    snapshot,
    action,
    message,
    stateName,
    statePatch,
    level="info",
    ok=True,
    requestSuccess=False,
    requestInvalid=False,
    requestConflict=False,
    requestRobotNotReady=False,
):
    stateUpdates = _stateUpdates(
        snapshot,
        stateName,
        statePatch,
    )
    return _buildOutcome(
        snapshot,
        ok,
        level,
        message,
        action,
        stateName,
        stateUpdates=stateUpdates,
        plcOutputs=_plcOutputs(
            snapshot,
            requestReceived=bool(snapshot["selected_workflow_number"]),
            requestSuccess=requestSuccess,
            requestInvalid=requestInvalid,
            requestConflict=requestConflict,
            requestRobotNotReady=requestRobotNotReady,
            missionNeedsFinalized=bool(stateUpdates.get("mission_needs_finalized"))
        ),
    )


def _evaluateActiveMissions(snapshot):
    currentState = snapshot["current_state"]
    selectedWorkflowNumber = snapshot["selected_workflow_number"]
    activeWorkflowNumber = snapshot["active_workflow_number"]
    activeSplit = _classifyActiveMissions(snapshot)
    matching = list(activeSplit["matching"] or [])
    queuedMismatches = list(activeSplit["queued_mismatches"] or [])
    blockingMismatches = list(activeSplit["blocking_mismatches"] or [])
    hasQueuedMismatches = bool(queuedMismatches)
    hasBlockingMismatches = bool(blockingMismatches)
    queuedSummary = {
        "finalized_count": 0,
        "canceled_count": 0,
        "skipped_count": 0,
        "failed_messages": [],
        "failed_levels": [],
        "issued_count": 0,
        "any_failures": False,
        "message": "",
    }

    def runMissionCommands(missions):
        if not missions:
            return dict(queuedSummary)
        return issueMissionCommands(
            snapshot["robot_name"],
            missions,
            selectedWorkflowNumber,
            activeWorkflowNumber,
            snapshot["now_epoch_ms"],
            finalizeMissionId=snapshot["finalize_mission_id"],
            cancelMissionIds=snapshot["cancel_mission_ids"],
        )

    def _activeStatePatch(lastResult, requestLatched=False, missionNeedsFinalized=False, recordTimestamp=False):
        patch = {
            "selected_workflow_number": selectedWorkflowNumber,
            "request_latched": requestLatched,
            "mission_created": True,
            "mission_needs_finalized": missionNeedsFinalized,
            "pending_create_start_epoch_ms": 0,
            "last_result": lastResult,
        }
        if recordTimestamp:
            patch["last_command_ts"] = _decisionTimestamp(snapshot)
        return patch

    def _activeOutcome(
        action,
        message,
        statePatch,
        stateName="mission_active",
        level="info",
        ok=True,
        requestSuccess=False,
        missionOps=None,
        activeWorkflowNumberOverride=None,
    ):
        combinedMissionOps = {"queued_summary": queuedSummary}
        combinedMissionOps.update(dict(missionOps or {}))
        return _activeMissionOutcome(
            snapshot,
            action,
            message,
            statePatch,
            stateName=stateName,
            level=level,
            ok=ok,
            requestSuccess=requestSuccess,
            missionOps=combinedMissionOps,
            activeWorkflowNumber=activeWorkflowNumber if activeWorkflowNumberOverride is None else activeWorkflowNumberOverride,
        )

    if (hasQueuedMismatches or hasBlockingMismatches) and currentState.get("disable_ignition_control"):
        return _activeOutcome(
            "hold_control_disabled",
            _holdDisabledMessage(
                snapshot,
                hasBlockingMismatches=hasBlockingMismatches,
                hasQueuedMismatches=hasQueuedMismatches,
            ),
            _activeStatePatch(
                _holdDisabledMessage(
                    snapshot,
                    hasBlockingMismatches=hasBlockingMismatches,
                    hasQueuedMismatches=hasQueuedMismatches,
                ),
                missionNeedsFinalized=hasBlockingMismatches,
            ),
            level="warn",
        )

    queuedSummary = runMissionCommands(queuedMismatches)
    queuedMessage = str(queuedSummary.get("message") or "")

    if hasBlockingMismatches:
        if not snapshot["plc_inputs"].get("finalize_ok"):
            return _activeOutcome(
                "hold_clear_pending",
                _mergeMessages(queuedMessage, _activeClearPendingMessage(snapshot)),
                _activeStatePatch(
                    _mergeMessages(queuedMessage, _activeClearPendingMessage(snapshot)),
                    missionNeedsFinalized=True,
                ),
            )

        blockingSummary = runMissionCommands(blockingMismatches)
        anyFailures = bool(queuedSummary.get("any_failures") or blockingSummary.get("any_failures"))
        failedLevels = list(queuedSummary.get("failed_levels") or []) + list(blockingSummary.get("failed_levels") or [])
        issuedCount = int(queuedSummary.get("issued_count") or 0) + int(blockingSummary.get("issued_count") or 0)
        mergedMessage = _mergeMessages(queuedMessage, blockingSummary.get("message"))
        return _activeOutcome(
            "clear_reconcile_failed" if anyFailures else ("clear_reconcile" if issuedCount else "hold_clear_inflight"),
            mergedMessage,
            _activeStatePatch(
                mergedMessage,
                missionNeedsFinalized=True,
                recordTimestamp=bool(issuedCount or anyFailures),
            ),
            stateName="fault" if anyFailures else "mission_active",
            level=_failedSummaryLevel(failedLevels) if anyFailures else "info",
            ok=not anyFailures,
            missionOps={
                "blocking_summary": blockingSummary,
            },
        )

    if queuedSummary.get("any_failures"):
        message = queuedMessage or "queued mission cleanup failed"
        return _activeOutcome(
            "clear_reconcile_failed",
            message,
            _activeStatePatch(message, recordTimestamp=True),
            stateName="fault",
            level=_failedSummaryLevel(queuedSummary.get("failed_levels")),
            ok=False,
        )

    if matching:
        lastResult = _mergeMessages("active mission matches requested workflow", queuedMessage)
        return _activeOutcome(
            "hold_active",
            "Robot [{}] active workflow {} is in progress".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            _activeStatePatch(lastResult, requestLatched=True),
            requestSuccess=True,
            activeWorkflowNumberOverride=selectedWorkflowNumber,
        )

    return _activeOutcome(
        "hold_clear_inflight",
        queuedMessage or "active mission present",
        _activeStatePatch(
            queuedMessage or "active mission present",
            recordTimestamp=bool(queuedSummary.get("issued_count")),
        ),
    )


def _evaluateNoActiveMissions(snapshot):
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

    def _clearRequestStatePatch(lastResult, recordTimestamp=False):
        patch = {
            "selected_workflow_number": selectedWorkflowNumber,
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
        if recordTimestamp:
            patch["last_command_ts"] = _decisionTimestamp(snapshot)
        return patch

    def _requestedStatePatch(lastResult, requestLatched=False, missionCreated=False, pendingCreateStartEpochMs=None):
        patch = {
            "selected_workflow_number": selectedWorkflowNumber,
            "request_latched": requestLatched,
            "mission_created": missionCreated,
            "mission_needs_finalized": False,
            "last_result": lastResult,
        }
        if pendingCreateStartEpochMs is not None:
            patch["pending_create_start_epoch_ms"] = pendingCreateStartEpochMs
        return patch

    def _outcome(action, message, stateName, statePatch, **kwargs):
        return _noActiveMissionOutcome(snapshot, action, message, stateName, statePatch, **kwargs)

    def _faultOutcome(action, message, statePatch, **kwargs):
        return _outcome(action, message, "fault", statePatch, level="warn", ok=False, **kwargs)

    def _requestedOutcome(action, message, statePatch, **kwargs):
        return _outcome(action, message, "mission_requested", statePatch, **kwargs)

    if _requestCleared(selectedWorkflowNumber):
        return _outcome(
            "idle",
            "Robot [{}] idle".format(snapshot["robot_name"]),
            "idle",
            _clearRequestStatePatch(""),
        )

    if currentState.get("disable_ignition_control"):
        return _requestedOutcome(
            "hold_control_disabled",
            "Robot [{}] create suppressed while Ignition control is disabled".format(snapshot["robot_name"]),
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": _holdDisabledMessage(snapshot, False),
            },
            level="warn",
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, snapshot["robot_name"]):
        return _faultOutcome(
            "request_invalid",
            "Robot [{}] requested invalid workflow {}".format(snapshot["robot_name"], selectedWorkflowNumber),
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "workflow {} invalid for {}".format(selectedWorkflowNumber, snapshot["robot_name"]),
            },
            requestInvalid=True,
        )

    owner = snapshot["reserved_workflows"].get(selectedWorkflowNumber)
    if owner and owner != snapshot["robot_name"]:
        return _faultOutcome(
            "request_conflict",
            "Robot [{}] workflow {} conflicts with {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber,
                owner
            ),
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
            },
            requestConflict=True,
        )

    snapshot["reserved_workflows"][selectedWorkflowNumber] = snapshot["robot_name"]

    if _latchedRequestMatches(snapshot):
        if currentState.get("mission_created"):
            pendingCreateAgeMs = _pendingCreateAgeMs(snapshot)
            pendingCreateTimeoutMs = _pendingCreateTimeoutMs()
            if pendingCreateAgeMs is None or pendingCreateAgeMs < pendingCreateTimeoutMs:
                return _requestedOutcome(
                    "hold_request",
                    "Robot [{}] waiting for created mission to appear in fleet".format(
                        snapshot["robot_name"]
                    ),
                _requestedStatePatch(
                    "waiting for created mission to appear in fleet",
                    requestLatched=True,
                    missionCreated=True,
                    pendingCreateStartEpochMs=currentState.get("pending_create_start_epoch_ms"),
                ),
                requestSuccess=True,
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
            return _requestedOutcome(
                "hold_request_timeout",
                "Robot [{}] {}".format(snapshot["robot_name"], timeoutMessage),
                _clearRequestStatePatch(timeoutMessage, recordTimestamp=True),
                level="warn",
                requestSuccess=False,
            )

        return _requestedOutcome(
            "hold_request",
            "Robot [{}] holding requested workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            _requestedStatePatch(
                currentState.get("last_result") or "waiting for active mission reconciliation",
                requestLatched=True,
                missionCreated=bool(currentState.get("mission_created")),
            ),
            requestSuccess=bool(currentState.get("mission_created")),
        )

    if not snapshot["controller_available_for_work"]:
        return _faultOutcome(
            "waiting_available",
            "Robot [{}] is not available for workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "robot not available for work",
            },
            requestRobotNotReady=True,
        )

    if _createBackoffActive(snapshot):
        return _faultOutcome(
            "hold_create_backoff",
            "Robot [{}] waiting before retrying create".format(snapshot["robot_name"]),
            {
                "selected_workflow_number": selectedWorkflowNumber,
                "last_result": "waiting {} ms before retrying create".format(_remainingCreateBackoffMs(snapshot)),
            },
        )

    commandId = str(snapshot["now_epoch_ms"])
    commandResult = callCreateMission(
        snapshot["robot_name"],
        selectedWorkflowNumber,
        createMission=snapshot["create_mission"],
    )
    createSucceeded = bool(commandResult.get("ok"))
    statePatch = {
        "selected_workflow_number": selectedWorkflowNumber,
        "request_latched": createSucceeded,
        "mission_created": createSucceeded,
        "mission_needs_finalized": False,
        "pending_create_start_epoch_ms": snapshot["now_epoch_ms"] if createSucceeded else 0,
        "last_command_ts": _decisionTimestamp(snapshot),
        "last_result": commandResult.get("message", ""),
        "last_command_id": commandId,
        "next_action_allowed_epoch_ms": 0 if createSucceeded else snapshot["now_epoch_ms"] + RETRY_DELAY_MS,
        "last_attempt_action": "" if createSucceeded else "create",
        "retry_count": 0 if createSucceeded else int(currentState.get("retry_count") or 0) + 1,
    }
    return _buildOutcome(
        snapshot,
        createSucceeded,
        commandResult.get("level", "info"),
        commandResult.get("message", ""),
        "create" if createSucceeded else "create_failed",
        "mission_requested" if createSucceeded else "fault",
        stateUpdates=_stateUpdates(
            snapshot,
            "mission_requested" if createSucceeded else "fault",
            statePatch,
        ),
        plcOutputs=_plcOutputs(
            snapshot,
            requestReceived=True,
            requestSuccess=createSucceeded
        ),
        commandResult=commandResult,
        data={"workflow_number": selectedWorkflowNumber},
    )


def runRobotWorkflowCycle(
    robotName,
    reservedWorkflows=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """Read one robot snapshot, build one plan, then apply it."""
    snapshot = readRobotCycleSnapshot(
        robotName,
        reservedWorkflows=reservedWorkflows,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )
    if snapshot["active_workflow_number"]:
        snapshot["reserved_workflows"][snapshot["active_workflow_number"]] = snapshot["robot_name"]

    if not snapshot["plc_healthy"]:
        outcome = _plcFaultOutcome(snapshot)
    elif list(snapshot["active_summary"].get("missions") or []):
        outcome = _evaluateActiveMissions(snapshot)
    else:
        outcome = _evaluateNoActiveMissions(snapshot)

    return applyRobotOutcome(snapshot, outcome)
