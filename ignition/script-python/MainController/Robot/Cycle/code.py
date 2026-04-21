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


def _workflowsMatch(activeWorkflowNumber, selectedWorkflowNumber):
    activeWorkflowNumber = normalizeWorkflowNumber(activeWorkflowNumber)
    selectedWorkflowNumber = normalizeWorkflowNumber(selectedWorkflowNumber)
    return bool(activeWorkflowNumber and selectedWorkflowNumber and activeWorkflowNumber == selectedWorkflowNumber)


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


def _plcFaultOutcome(snapshot):
    currentState = snapshot["current_state"]
    stateUpdates = _stateUpdates(
        snapshot,
        "fault",
        selectedWorkflowNumber=currentState.get("selected_workflow_number"),
        requestLatched=currentState.get("request_latched"),
        missionCreated=currentState.get("mission_created"),
        missionNeedsFinalized=currentState.get("mission_needs_finalized"),
        lastResult=snapshot["plc_inputs"].get("fault_reason") or "plc_input_quality_bad",
        lastCommandId=currentState.get("last_command_id"),
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


def _holdDisabledMessage(snapshot, hasBlockingMismatches):
    if hasBlockingMismatches:
        if _requestCleared(snapshot["selected_workflow_number"]):
            return "Ignition control disabled; active mission clear suppressed"
        return "Ignition control disabled; workflow switch clear suppressed"
    return "Ignition control disabled; create suppressed"


def _evaluateActiveMissions(snapshot):
    currentState = snapshot["current_state"]
    selectedWorkflowNumber = snapshot["selected_workflow_number"]
    activeWorkflowNumber = snapshot["active_workflow_number"]
    activeSplit = _classifyActiveMissions(snapshot)
    matching = list(activeSplit["matching"] or [])
    queuedMismatches = list(activeSplit["queued_mismatches"] or [])
    blockingMismatches = list(activeSplit["blocking_mismatches"] or [])

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
    if queuedMismatches:
        queuedSummary = issueMissionCommands(
            snapshot["robot_name"],
            queuedMismatches,
            selectedWorkflowNumber,
            activeWorkflowNumber,
            snapshot["now_epoch_ms"],
            finalizeMissionId=snapshot["finalize_mission_id"],
            cancelMissionIds=snapshot["cancel_mission_ids"],
        )

    queuedMessage = str(queuedSummary.get("message") or "")
    hasBlockingMismatches = bool(blockingMismatches)

    if hasBlockingMismatches:
        if currentState.get("disable_ignition_control"):
            message = _mergeMessages(
                queuedMessage,
                _holdDisabledMessage(snapshot, True)
            )
            stateUpdates = _stateUpdates(
                snapshot,
                "mission_active",
                selectedWorkflowNumber=selectedWorkflowNumber,
                requestLatched=False,
                missionCreated=True,
                missionNeedsFinalized=True,
                lastResult=message,
                lastCommandId=currentState.get("last_command_id"),
            )
            return _buildOutcome(
                snapshot,
                True,
                "warn",
                message,
                "hold_control_disabled",
                "mission_active",
                stateUpdates=stateUpdates,
                plcOutputs=_plcOutputs(
                    snapshot,
                    requestReceived=bool(selectedWorkflowNumber),
                    missionNeedsFinalized=True
                ),
                activeWorkflowNumber=activeWorkflowNumber,
                missionOps={"queued_summary": queuedSummary},
            )

        if not snapshot["plc_inputs"].get("finalize_ok"):
            message = _mergeMessages(queuedMessage, _activeClearPendingMessage(snapshot))
            stateUpdates = _stateUpdates(
                snapshot,
                "mission_active",
                selectedWorkflowNumber=selectedWorkflowNumber,
                requestLatched=False,
                missionCreated=True,
                missionNeedsFinalized=True,
                lastResult=message,
                lastCommandId=currentState.get("last_command_id"),
            )
            return _buildOutcome(
                snapshot,
                True,
                "info",
                message,
                "hold_clear_pending",
                "mission_active",
                stateUpdates=stateUpdates,
                plcOutputs=_plcOutputs(
                    snapshot,
                    requestReceived=bool(selectedWorkflowNumber),
                    missionNeedsFinalized=True
                ),
                activeWorkflowNumber=activeWorkflowNumber,
                missionOps={"queued_summary": queuedSummary},
            )

        blockingSummary = issueMissionCommands(
            snapshot["robot_name"],
            blockingMismatches,
            selectedWorkflowNumber,
            activeWorkflowNumber,
            snapshot["now_epoch_ms"],
            finalizeMissionId=snapshot["finalize_mission_id"],
            cancelMissionIds=snapshot["cancel_mission_ids"],
        )
        anyFailures = bool(queuedSummary.get("any_failures") or blockingSummary.get("any_failures"))
        failedLevels = list(queuedSummary.get("failed_levels") or []) + list(blockingSummary.get("failed_levels") or [])
        issuedCount = int(queuedSummary.get("issued_count") or 0) + int(blockingSummary.get("issued_count") or 0)
        message = _mergeMessages(queuedMessage, blockingSummary.get("message"))
        stateName = "fault" if anyFailures else "mission_active"
        stateUpdates = _stateUpdates(
            snapshot,
            stateName,
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult=message,
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            not anyFailures,
            "error" if "error" in failedLevels else ("warn" if anyFailures else "info"),
            message,
            "clear_reconcile_failed" if anyFailures else ("clear_reconcile" if issuedCount else "hold_clear_inflight"),
            stateName,
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=bool(selectedWorkflowNumber),
                missionNeedsFinalized=True
            ),
            activeWorkflowNumber=activeWorkflowNumber,
            missionOps={
                "queued_summary": queuedSummary,
                "blocking_summary": blockingSummary,
            },
        )

    if queuedSummary.get("any_failures"):
        message = queuedMessage or "queued mission cleanup failed"
        stateUpdates = _stateUpdates(
            snapshot,
            "fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=message,
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            False,
            "error" if "error" in list(queuedSummary.get("failed_levels") or []) else "warn",
            message,
            "clear_reconcile_failed",
            "fault",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=bool(selectedWorkflowNumber)
            ),
            activeWorkflowNumber=activeWorkflowNumber,
            missionOps={"queued_summary": queuedSummary},
        )

    if matching:
        message = _mergeMessages("active mission matches requested workflow", queuedMessage)
        stateUpdates = _stateUpdates(
            snapshot,
            "mission_active",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=message,
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            True,
            "info",
            "Robot [{}] active workflow {} is in progress".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            "hold_active",
            "mission_active",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=bool(selectedWorkflowNumber),
                requestSuccess=True
            ),
            activeWorkflowNumber=selectedWorkflowNumber,
            missionOps={"queued_summary": queuedSummary},
        )

    if queuedSummary.get("issued_count") or queuedSummary.get("skipped_count"):
        message = queuedMessage or "waiting for active missions to clear before creating new work"
        stateUpdates = _stateUpdates(
            snapshot,
            "mission_active",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=message,
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            True,
            "info",
            message,
            "hold_clear_inflight",
            "mission_active",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=bool(selectedWorkflowNumber)
            ),
            activeWorkflowNumber=activeWorkflowNumber,
            missionOps={"queued_summary": queuedSummary},
        )

    message = "active mission present"
    stateUpdates = _stateUpdates(
        snapshot,
        "mission_active",
        selectedWorkflowNumber=selectedWorkflowNumber,
        requestLatched=False,
        missionCreated=True,
        missionNeedsFinalized=False,
        lastResult=message,
        lastCommandId=currentState.get("last_command_id"),
    )
    return _buildOutcome(
        snapshot,
        True,
        "info",
        message,
        "hold_clear_inflight",
        "mission_active",
        stateUpdates=stateUpdates,
        plcOutputs=_plcOutputs(
            snapshot,
            requestReceived=bool(selectedWorkflowNumber)
        ),
        activeWorkflowNumber=activeWorkflowNumber,
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

    if _requestCleared(selectedWorkflowNumber):
        stateUpdates = _stateUpdates(
            snapshot,
            "idle",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="",
            lastCommandId="",
            nextActionAllowedEpochMs=0,
            lastAttemptAction="",
            retryCount=0,
        )
        return _buildOutcome(
            snapshot,
            True,
            "info",
            "Robot [{}] idle".format(snapshot["robot_name"]),
            "idle",
            "idle",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=False,
                missionNeedsFinalized=False
            ),
        )

    if currentState.get("disable_ignition_control"):
        message = _holdDisabledMessage(snapshot, False)
        stateUpdates = _stateUpdates(
            snapshot,
            "mission_requested",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult=message,
            lastCommandId=currentState.get("last_command_id"),
            nextActionAllowedEpochMs=0,
            lastAttemptAction="",
            retryCount=0,
        )
        return _buildOutcome(
            snapshot,
            True,
            "warn",
            "Robot [{}] create suppressed while Ignition control is disabled".format(snapshot["robot_name"]),
            "hold_control_disabled",
            "mission_requested",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=True
            ),
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, snapshot["robot_name"]):
        stateUpdates = _stateUpdates(
            snapshot,
            "fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="workflow {} invalid for {}".format(selectedWorkflowNumber, snapshot["robot_name"]),
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            False,
            "warn",
            "Robot [{}] requested invalid workflow {}".format(snapshot["robot_name"], selectedWorkflowNumber),
            "request_invalid",
            "fault",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=True,
                requestInvalid=True
            ),
        )

    owner = snapshot["reserved_workflows"].get(selectedWorkflowNumber)
    if owner and owner != snapshot["robot_name"]:
        stateUpdates = _stateUpdates(
            snapshot,
            "fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            False,
            "warn",
            "Robot [{}] workflow {} conflicts with {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber,
                owner
            ),
            "request_conflict",
            "fault",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=True,
                requestConflict=True
            ),
        )

    snapshot["reserved_workflows"][selectedWorkflowNumber] = snapshot["robot_name"]

    if _latchedRequestMatches(snapshot):
        stateUpdates = _stateUpdates(
            snapshot,
            "mission_requested",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=bool(currentState.get("mission_created")),
            missionNeedsFinalized=False,
            lastResult=currentState.get("last_result") or "waiting for active mission reconciliation",
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            True,
            "info",
            "Robot [{}] holding requested workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            "hold_request",
            "mission_requested",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=True,
                requestSuccess=bool(stateUpdates.get("mission_created"))
            ),
        )

    if not snapshot["controller_available_for_work"]:
        stateUpdates = _stateUpdates(
            snapshot,
            "fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="robot not available for work",
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            False,
            "warn",
            "Robot [{}] is not available for workflow {}".format(
                snapshot["robot_name"],
                selectedWorkflowNumber
            ),
            "waiting_available",
            "fault",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=True,
                requestRobotNotReady=True
            ),
        )

    if _createBackoffActive(snapshot):
        stateUpdates = _stateUpdates(
            snapshot,
            "fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="waiting {} ms before retrying create".format(_remainingCreateBackoffMs(snapshot)),
            lastCommandId=currentState.get("last_command_id"),
        )
        return _buildOutcome(
            snapshot,
            True,
            "warn",
            "Robot [{}] waiting before retrying create".format(snapshot["robot_name"]),
            "hold_create_backoff",
            "fault",
            stateUpdates=stateUpdates,
            plcOutputs=_plcOutputs(
                snapshot,
                requestReceived=True
            ),
        )

    commandId = str(snapshot["now_epoch_ms"])
    commandResult = callCreateMission(
        snapshot["robot_name"],
        selectedWorkflowNumber,
        createMission=snapshot["create_mission"],
    )
    stateUpdates = _stateUpdates(
        snapshot,
        "mission_requested" if commandResult.get("ok") else "fault",
        selectedWorkflowNumber=selectedWorkflowNumber,
        requestLatched=commandResult.get("ok", False),
        missionCreated=commandResult.get("ok", False),
        missionNeedsFinalized=False,
        lastResult=commandResult.get("message", ""),
        lastCommandId=commandId,
        nextActionAllowedEpochMs=0 if commandResult.get("ok") else snapshot["now_epoch_ms"] + RETRY_DELAY_MS,
        lastAttemptAction="" if commandResult.get("ok") else "create",
        retryCount=0 if commandResult.get("ok") else int(currentState.get("retry_count") or 0) + 1,
    )
    return _buildOutcome(
        snapshot,
        commandResult.get("ok", False),
        commandResult.get("level", "info"),
        commandResult.get("message", ""),
        "create" if commandResult.get("ok") else "create_failed",
        stateUpdates.get("state"),
        stateUpdates=stateUpdates,
        plcOutputs=_plcOutputs(
            snapshot,
            requestReceived=True,
            requestSuccess=commandResult.get("ok", False)
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
