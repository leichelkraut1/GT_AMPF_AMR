from MainController.MissionCommandHelpers import issueMissionCommands
from MainController.Robot.Actions import callCreateMission
from MainController.Robot.Apply import applyRobotOutcome
from MainController.Robot.PlcMirror import buildOutputs
from MainController.Robot.Snapshot import readRobotCycleSnapshot
from Otto_API.Common.RuntimeHistory import timestampString
from MainController.State.Paths import RETRY_DELAY_MS
from MainController.WorkflowConfig import getWorkflowDef
from MainController.WorkflowConfig import isWorkflowAllowedForRobot
from MainController.WorkflowConfig import normalizeWorkflowNumber


_UNSET = object()


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


def _stateUpdates(
    snapshot,
    stateName,
    selectedWorkflowNumber=_UNSET,
    requestLatched=_UNSET,
    missionCreated=_UNSET,
    missionNeedsFinalized=_UNSET,
    lastResult=_UNSET,
    lastCommandId=_UNSET,
    nextActionAllowedEpochMs=_UNSET,
    lastAttemptAction=_UNSET,
    retryCount=_UNSET,
):
    currentState = dict(snapshot.get("current_state") or {})

    if selectedWorkflowNumber is _UNSET:
        selectedWorkflowNumber = currentState.get("selected_workflow_number")
    if requestLatched is _UNSET:
        requestLatched = currentState.get("request_latched")
    if missionCreated is _UNSET:
        missionCreated = currentState.get("mission_created")
    if missionNeedsFinalized is _UNSET:
        missionNeedsFinalized = currentState.get("mission_needs_finalized")
    if lastResult is _UNSET:
        lastResult = currentState.get("last_result")
    if lastCommandId is _UNSET:
        lastCommandId = currentState.get("last_command_id")
    if nextActionAllowedEpochMs is _UNSET:
        nextActionAllowedEpochMs = currentState.get("next_action_allowed_epoch_ms")
    if lastAttemptAction is _UNSET:
        lastAttemptAction = currentState.get("last_attempt_action")
    if retryCount is _UNSET:
        retryCount = currentState.get("retry_count")

    return {
        "selected_workflow_number": normalizeWorkflowNumber(selectedWorkflowNumber) or 0,
        "state": str(stateName or "idle"),
        "request_latched": bool(requestLatched),
        "mission_created": bool(missionCreated),
        "mission_needs_finalized": bool(missionNeedsFinalized),
        "last_command_ts": timestampString(snapshot["now_epoch_ms"]),
        "last_result": str(lastResult or ""),
        "last_command_id": str(lastCommandId or ""),
        "next_action_allowed_epoch_ms": int(nextActionAllowedEpochMs or 0),
        "last_attempt_action": str(lastAttemptAction or ""),
        "retry_count": int(retryCount or 0),
    }


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
    if stateUpdates is None:
        stateUpdates = _stateUpdates(snapshot, stateName, lastResult=message)

    normalizedActiveWorkflow = normalizeWorkflowNumber(
        snapshot["active_workflow_number"] if activeWorkflowNumber is None else activeWorkflowNumber
    ) or 0
    return {
        "ok": bool(ok),
        "level": str(level or "info"),
        "message": str(message or ""),
        "action": str(action or ""),
        "state": str(stateUpdates.get("state") or stateName or ""),
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


def _outcomeContext(
    snapshot,
    stateName,
    activeWorkflowNumber=None,
    lastResult=None,
    stateOverrides=None,
):
    currentState = snapshot["current_state"]
    if activeWorkflowNumber is None:
        activeWorkflowNumber = snapshot["active_workflow_number"]
    if lastResult is None:
        lastResult = ""

    overrides = {
        "selectedWorkflowNumber": snapshot["selected_workflow_number"],
        "lastCommandId": currentState.get("last_command_id"),
        "lastResult": lastResult,
    }
    overrides.update(dict(stateOverrides or {}))
    return {
        "state_updates": _stateUpdates(snapshot, stateName, **overrides),
        "active_workflow_number": activeWorkflowNumber,
    }


def _plcFaultOutcome(snapshot):
    currentState = snapshot["current_state"]
    context = _outcomeContext(
        snapshot,
        "fault",
        lastResult=snapshot["plc_inputs"].get("fault_reason") or "plc_input_quality_bad",
        stateOverrides={
            "selectedWorkflowNumber": currentState.get("selected_workflow_number"),
            "requestLatched": currentState.get("request_latched"),
            "missionCreated": currentState.get("mission_created"),
            "missionNeedsFinalized": currentState.get("mission_needs_finalized"),
        },
    )
    return _buildOutcome(
        snapshot,
        False,
        "warn",
        "Robot [{}] PLC inputs are unhealthy; skipping command evaluation".format(snapshot["robot_name"]),
        "plc_comm_fault",
        "fault",
        stateUpdates=context["state_updates"],
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
    stateName="mission_active",
    level="info",
    ok=True,
    requestLatched=False,
    missionNeedsFinalized=False,
    requestSuccess=False,
    missionOps=None,
    activeWorkflowNumber=None,
    lastResult=None,
):
    if activeWorkflowNumber is None:
        activeWorkflowNumber = snapshot["active_workflow_number"]
    if lastResult is None:
        lastResult = message

    context = _outcomeContext(
        snapshot,
        stateName,
        activeWorkflowNumber=activeWorkflowNumber,
        lastResult=lastResult,
        stateOverrides={
            "requestLatched": requestLatched,
            "missionCreated": True,
            "missionNeedsFinalized": missionNeedsFinalized,
        },
    )
    return _buildOutcome(
        snapshot,
        ok,
        level,
        message,
        action,
        stateName,
        stateUpdates=context["state_updates"],
        plcOutputs=_plcOutputs(
            snapshot,
            requestReceived=bool(snapshot["selected_workflow_number"]),
            requestSuccess=requestSuccess,
            missionNeedsFinalized=missionNeedsFinalized
        ),
        activeWorkflowNumber=context["active_workflow_number"],
        missionOps=missionOps,
    )


def _noActiveMissionOutcome(
    snapshot,
    action,
    message,
    stateName,
    level="info",
    ok=True,
    requestLatched=False,
    missionCreated=False,
    missionNeedsFinalized=False,
    requestSuccess=False,
    requestInvalid=False,
    requestConflict=False,
    requestRobotNotReady=False,
    nextActionAllowedEpochMs=_UNSET,
    lastAttemptAction=_UNSET,
    retryCount=_UNSET,
    lastCommandId=_UNSET,
    lastResult=None,
):
    if lastResult is None:
        lastResult = message
    currentState = snapshot["current_state"]
    if nextActionAllowedEpochMs is _UNSET:
        nextActionAllowedEpochMs = currentState.get("next_action_allowed_epoch_ms")
    if lastAttemptAction is _UNSET:
        lastAttemptAction = currentState.get("last_attempt_action")
    if retryCount is _UNSET:
        retryCount = currentState.get("retry_count")
    if lastCommandId is _UNSET:
        lastCommandId = currentState.get("last_command_id")

    context = _outcomeContext(
        snapshot,
        stateName,
        lastResult=lastResult,
        stateOverrides={
            "requestLatched": requestLatched,
            "missionCreated": missionCreated,
            "missionNeedsFinalized": missionNeedsFinalized,
            "lastCommandId": lastCommandId,
            "nextActionAllowedEpochMs": nextActionAllowedEpochMs,
            "lastAttemptAction": lastAttemptAction,
            "retryCount": retryCount,
        },
    )
    return _buildOutcome(
        snapshot,
        ok,
        level,
        message,
        action,
        stateName,
        stateUpdates=context["state_updates"],
        plcOutputs=_plcOutputs(
            snapshot,
            requestReceived=bool(snapshot["selected_workflow_number"]),
            requestSuccess=requestSuccess,
            requestInvalid=requestInvalid,
            requestConflict=requestConflict,
            requestRobotNotReady=requestRobotNotReady,
            missionNeedsFinalized=missionNeedsFinalized
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

    def runMissionCommands(missions):
        if not missions:
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
        return issueMissionCommands(
            snapshot["robot_name"],
            missions,
            selectedWorkflowNumber,
            activeWorkflowNumber,
            snapshot["now_epoch_ms"],
            finalizeMissionId=snapshot["finalize_mission_id"],
            cancelMissionIds=snapshot["cancel_mission_ids"],
        )

    if (hasQueuedMismatches or hasBlockingMismatches) and currentState.get("disable_ignition_control"):
        return _activeMissionOutcome(
            snapshot,
            "hold_control_disabled",
            _holdDisabledMessage(
                snapshot,
                hasBlockingMismatches=hasBlockingMismatches,
                hasQueuedMismatches=hasQueuedMismatches,
            ),
            level="warn",
            missionNeedsFinalized=hasBlockingMismatches,
        )

    queuedSummary = runMissionCommands(queuedMismatches)

    queuedMessage = str(queuedSummary.get("message") or "")

    def _activeOutcome(action, message, **kwargs):
        missionOps = dict(kwargs.pop("missionOps", {}) or {})
        missionOps.setdefault("queued_summary", queuedSummary)
        kwargs.setdefault("activeWorkflowNumber", activeWorkflowNumber)
        return _activeMissionOutcome(
            snapshot,
            action,
            message,
            missionOps=missionOps,
            **kwargs
        )

    if hasBlockingMismatches:
        if not snapshot["plc_inputs"].get("finalize_ok"):
            return _activeOutcome(
                "hold_clear_pending",
                _mergeMessages(queuedMessage, _activeClearPendingMessage(snapshot)),
                missionNeedsFinalized=True,
            )

        blockingSummary = runMissionCommands(blockingMismatches)
        anyFailures = bool(queuedSummary.get("any_failures") or blockingSummary.get("any_failures"))
        failedLevels = list(queuedSummary.get("failed_levels") or []) + list(blockingSummary.get("failed_levels") or [])
        issuedCount = int(queuedSummary.get("issued_count") or 0) + int(blockingSummary.get("issued_count") or 0)
        return _activeOutcome(
            "clear_reconcile_failed" if anyFailures else ("clear_reconcile" if issuedCount else "hold_clear_inflight"),
            _mergeMessages(queuedMessage, blockingSummary.get("message")),
            stateName="fault" if anyFailures else "mission_active",
            level=_failedSummaryLevel(failedLevels) if anyFailures else "info",
            ok=not anyFailures,
            missionNeedsFinalized=True,
            missionOps={
                "queued_summary": queuedSummary,
                "blocking_summary": blockingSummary,
            },
        )

    if queuedSummary.get("any_failures"):
        message = queuedMessage or "queued mission cleanup failed"
        return _activeOutcome(
            "clear_reconcile_failed",
            message,
            "fault",
            level=_failedSummaryLevel(queuedSummary.get("failed_levels")),
            ok=False,
        )

    if matching:
        return _activeOutcome(
            "hold_active",
            "Robot [{}] active workflow {} is in progress".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            requestLatched=True,
            requestSuccess=True,
            activeWorkflowNumber=selectedWorkflowNumber,
            lastResult=_mergeMessages("active mission matches requested workflow", queuedMessage),
        )

    if queuedSummary.get("issued_count") or queuedSummary.get("skipped_count"):
        return _activeOutcome(
            "hold_clear_inflight",
            queuedMessage or "waiting for active missions to clear before creating new work",
        )

    return _activeOutcome(
        "hold_clear_inflight",
        "active mission present",
    )


def _evaluateNoActiveMissions(snapshot):
    currentState = dict(snapshot["current_state"] or {})
    selectedWorkflowNumber = snapshot["selected_workflow_number"]
    if currentState.get("mission_needs_finalized"):
        currentState["request_latched"] = False
        currentState["mission_created"] = False
        currentState["mission_needs_finalized"] = False
        currentState["last_result"] = ""
        currentState["last_command_id"] = ""
        snapshot = dict(snapshot or {})
        snapshot["current_state"] = currentState

    def _outcome(action, message, stateName, **kwargs):
        return _noActiveMissionOutcome(snapshot, action, message, stateName, **kwargs)

    def _faultOutcome(action, message, **kwargs):
        kwargs.setdefault("stateName", "fault")
        kwargs.setdefault("level", "warn")
        kwargs.setdefault("ok", False)
        stateName = kwargs.pop("stateName")
        return _outcome(action, message, stateName, **kwargs)

    def _requestedOutcome(action, message, **kwargs):
        kwargs.setdefault("stateName", "mission_requested")
        stateName = kwargs.pop("stateName")
        return _outcome(action, message, stateName, **kwargs)

    if _requestCleared(selectedWorkflowNumber):
        return _outcome(
            "idle",
            "Robot [{}] idle".format(snapshot["robot_name"]),
            "idle",
            lastResult="",
            lastCommandId="",
            nextActionAllowedEpochMs=0,
            lastAttemptAction="",
            retryCount=0,
        )

    if currentState.get("disable_ignition_control"):
        return _requestedOutcome(
            "hold_control_disabled",
            "Robot [{}] create suppressed while Ignition control is disabled".format(snapshot["robot_name"]),
            level="warn",
            nextActionAllowedEpochMs=0,
            lastAttemptAction="",
            retryCount=0,
            lastResult=_holdDisabledMessage(snapshot, False),
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, snapshot["robot_name"]):
        return _faultOutcome(
            "request_invalid",
            "Robot [{}] requested invalid workflow {}".format(snapshot["robot_name"], selectedWorkflowNumber),
            requestInvalid=True,
            lastResult="workflow {} invalid for {}".format(selectedWorkflowNumber, snapshot["robot_name"]),
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
            requestConflict=True,
            lastResult="workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
        )

    snapshot["reserved_workflows"][selectedWorkflowNumber] = snapshot["robot_name"]

    if _latchedRequestMatches(snapshot):
        return _requestedOutcome(
            "hold_request",
            "Robot [{}] holding requested workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            requestLatched=True,
            missionCreated=bool(currentState.get("mission_created")),
            requestSuccess=bool(currentState.get("mission_created")),
            lastResult=currentState.get("last_result") or "waiting for active mission reconciliation",
        )

    if not snapshot["controller_available_for_work"]:
        return _faultOutcome(
            "waiting_available",
            "Robot [{}] is not available for workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            requestRobotNotReady=True,
            lastResult="robot not available for work",
        )

    if _createBackoffActive(snapshot):
        return _faultOutcome(
            "hold_create_backoff",
            "Robot [{}] waiting before retrying create".format(snapshot["robot_name"]),
            lastResult="waiting {} ms before retrying create".format(_remainingCreateBackoffMs(snapshot)),
        )

    def _createOutcome(commandResult, commandId):
        createSucceeded = bool(commandResult.get("ok"))
        stateName = "mission_requested" if createSucceeded else "fault"
        stateUpdates = _outcomeContext(
            snapshot,
            stateName,
            lastResult=commandResult.get("message", ""),
            stateOverrides={
                "requestLatched": createSucceeded,
                "missionCreated": createSucceeded,
                "missionNeedsFinalized": False,
                "lastCommandId": commandId,
                "nextActionAllowedEpochMs": 0 if createSucceeded else snapshot["now_epoch_ms"] + RETRY_DELAY_MS,
                "lastAttemptAction": "" if createSucceeded else "create",
                "retryCount": 0 if createSucceeded else int(currentState.get("retry_count") or 0) + 1,
            },
        )["state_updates"]
        return _buildOutcome(
            snapshot,
            createSucceeded,
            commandResult.get("level", "info"),
            commandResult.get("message", ""),
            "create" if createSucceeded else "create_failed",
            stateName,
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=True,
                requestSuccess=createSucceeded
            ),
            commandResult=commandResult,
            data={"workflow_number": selectedWorkflowNumber},
        )

    commandId = str(snapshot["now_epoch_ms"])
    commandResult = callCreateMission(
        snapshot["robot_name"],
        selectedWorkflowNumber,
        createMission=snapshot["create_mission"],
    )
    return _createOutcome(commandResult, commandId)


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
