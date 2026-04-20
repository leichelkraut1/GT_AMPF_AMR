import time

from MainController.CommandHelpers import appendRuntimeDatasetRow
from MainController.CommandHelpers import buildCommandLogSignature
from MainController.CommandHelpers import buildCycleResult
from MainController.CommandHelpers import COMMAND_HISTORY_HEADERS
from MainController.CommandHelpers import COMMAND_HISTORY_MAX_ROWS
from MainController.CommandHelpers import ensureRobotRunnerTags
from MainController.CommandHelpers import readActiveMissionSummary
from MainController.CommandHelpers import readPlcInputs
from MainController.CommandHelpers import readRobotMirrorInputs
from MainController.CommandHelpers import readRobotState
from MainController.CommandHelpers import RETRY_DELAY_MS
from MainController.CommandHelpers import timestampString
from MainController.CommandHelpers import writePlcHealthOutputs
from MainController.CommandHelpers import writePlcOutputs
from MainController.CommandHelpers import writeRobotState
from MainController.MissionCommandHelpers import issueMissionCommands
from MainController.PlcMirror import buildOutputs
from MainController.RobotActions import callCreateMission
from MainController.RobotDecisions import isRequestCleared
from MainController.RobotDecisions import isWorkflowConflict
from MainController.RobotDecisions import isWorkflowRequestInvalid
from MainController.RobotDecisions import shouldHoldActive
from MainController.RobotDecisions import shouldHoldLatchedRequest
from MainController.WorkflowConfig import getWorkflowDef
from MainController.WorkflowConfig import isWorkflowAllowedForRobot
from MainController.WorkflowConfig import normalizeWorkflowNumber


def _newCycleState(currentState, nowEpochMs):
    """Start one mutable next-state snapshot for this robot cycle."""
    nextState = dict(currentState or {})
    nextState["last_command_ts"] = timestampString(nowEpochMs)
    return nextState


def _updateCycleState(
    nextState,
    stateName=None,
    selectedWorkflowNumber=None,
    requestLatched=None,
    missionCreated=None,
    missionNeedsFinalized=None,
    lastResult=None,
    lastCommandId=None,
    nextActionAllowedEpochMs=None,
    lastAttemptAction=None,
    retryCount=None
):
    """Mutate only the state fields owned by the current branch."""
    if stateName is not None:
        nextState["state"] = stateName
    if selectedWorkflowNumber is not None:
        nextState["selected_workflow_number"] = normalizeWorkflowNumber(selectedWorkflowNumber) or 0
    if requestLatched is not None:
        nextState["request_latched"] = requestLatched
    if missionCreated is not None:
        nextState["mission_created"] = missionCreated
    if missionNeedsFinalized is not None:
        nextState["mission_needs_finalized"] = missionNeedsFinalized
    if lastResult is not None:
        nextState["last_result"] = lastResult
    if lastCommandId is not None:
        nextState["last_command_id"] = lastCommandId
    if nextActionAllowedEpochMs is not None:
        nextState["next_action_allowed_epoch_ms"] = int(nextActionAllowedEpochMs or 0)
    if lastAttemptAction is not None:
        nextState["last_attempt_action"] = str(lastAttemptAction or "")
    if retryCount is not None:
        nextState["retry_count"] = int(retryCount or 0)
    return nextState


def _recordCommandHistory(nowEpochMs, cycleResult):
    """Log non-idle controller decisions to the runtime history dataset."""
    if not cycleResult:
        return
    action = str(cycleResult.get("action") or "")
    if action == "idle":
        return

    data = dict(cycleResult.get("data") or {})
    robotName = cycleResult.get("robot_name") or data.get("robot_name") or ""
    activeWorkflowNumber = (
        normalizeWorkflowNumber(data.get("active_workflow_number"))
        or normalizeWorkflowNumber(data.get("workflow_number"))
        or 0
    )
    signature = buildCommandLogSignature(
        robotName,
        data.get("requested_workflow_number"),
        activeWorkflowNumber,
        action,
        cycleResult.get("level") or "",
        cycleResult.get("state") or "",
        cycleResult.get("message") or "",
    )
    currentState = readRobotState(robotName)
    if currentState.get("last_logged_signature") == signature:
        writeRobotState(
            robotName,
            {
                "last_computed_log_signature": signature,
                "last_log_decision": "skip_duplicate",
            }
        )
        return

    appendRuntimeDatasetRow(
        "command_history",
        COMMAND_HISTORY_HEADERS,
        [
            timestampString(nowEpochMs),
            robotName,
            normalizeWorkflowNumber(data.get("requested_workflow_number")) or 0,
            activeWorkflowNumber,
            action,
            cycleResult.get("level") or "",
            cycleResult.get("state") or "",
            cycleResult.get("message") or "",
        ],
        maxRows=COMMAND_HISTORY_MAX_ROWS,
    )
    writeRobotState(
        robotName,
        {
            "last_logged_signature": signature,
            "last_computed_log_signature": signature,
            "last_log_decision": "append",
        }
    )


def _finishCycle(
    robotName,
    nextState,
    outputs,
    returnCycle,
    ok,
    level,
    message,
    action,
    data=None
):
    """Persist the branch result, mirror PLC outputs, then build the cycle result."""
    if action not in ["create_failed", "hold_create_backoff"]:
        _updateCycleState(
            nextState,
            nextActionAllowedEpochMs=0,
            lastAttemptAction="",
            retryCount=0,
        )
    writeRobotState(robotName, nextState)
    if outputs is not None:
        writePlcOutputs(robotName, outputs)
    return returnCycle(
        ok,
        level,
        message,
        robotName=robotName,
        state=nextState.get("state") or "",
        action=action,
        data=data,
    )


def _isActionBackoffActive(currentState, actionName, nowEpochMs):
    """Return True when a failed action is still inside its retry delay window."""
    return bool(
        str(currentState.get("last_attempt_action") or "") == str(actionName or "")
        and int(currentState.get("next_action_allowed_epoch_ms") or 0) > int(nowEpochMs or 0)
    )


def _remainingActionBackoffMs(currentState, nowEpochMs):
    """Return the remaining delay before the next retry is allowed."""
    return max(0, int(currentState.get("next_action_allowed_epoch_ms") or 0) - int(nowEpochMs or 0))


def _workflowData(workflowNumber):
    if workflowNumber is None:
        return None
    return {"workflow_number": workflowNumber}


def _clearPendingMessage(activeWorkflowNumber, selectedWorkflowNumber):
    """Build the stable wait message for FinalizeOk-gated robot clears."""
    if isRequestCleared(selectedWorkflowNumber):
        return "waiting for FinalizeOk before clearing active missions for workflow {}".format(
            activeWorkflowNumber or "unknown"
        )
    if activeWorkflowNumber:
        return "waiting for FinalizeOk before clearing workflow {} and switching to {}".format(
            activeWorkflowNumber,
            selectedWorkflowNumber
        )
    return "waiting for FinalizeOk before clearing active missions and starting {}".format(
        selectedWorkflowNumber
    )


def _buildCycleContext(robotName, **kwargs):
    """Bundle the mutable per-cycle state so phase handlers stay focused."""
    return {
        "robot_name": robotName,
        "plc_inputs": kwargs.get("plcInputs"),
        "mirror_inputs": kwargs.get("mirrorInputs"),
        "current_state": kwargs.get("currentState"),
        "active_summary": kwargs.get("activeSummary"),
        "active_workflow_number": kwargs.get("activeWorkflowNumber"),
        "selected_workflow_number": kwargs.get("selectedWorkflowNumber"),
        "reserved_workflows": kwargs.get("reservedWorkflows"),
        "controller_available_for_work": kwargs.get("controllerAvailableForWork"),
        "next_state": kwargs.get("nextState"),
        "return_cycle": kwargs.get("returnCycle"),
        "now_epoch_ms": kwargs.get("nowEpochMs"),
        "create_mission": kwargs.get("createMission"),
        "finalize_mission_id": kwargs.get("finalizeMissionId"),
        "cancel_mission_ids": kwargs.get("cancelMissionIds"),
    }

def _splitActiveMissionsByRequest(activeMissions, selectedWorkflowNumber):
    """Split active missions into matching work, queued mismatches, and FinalizeOk-gated mismatches."""
    selectedWorkflowNumber = normalizeWorkflowNumber(selectedWorkflowNumber)
    matchingMissions = []
    queuedMismatches = []
    gatedMismatches = []

    for missionRecord in list(activeMissions or []):
        missionRecord = dict(missionRecord or {})
        missionWorkflowNumber = normalizeWorkflowNumber(missionRecord.get("workflow_number"))
        missionStatus = str(missionRecord.get("mission_status") or "").upper()
        matchesRequestedWorkflow = bool(
            selectedWorkflowNumber
            and missionWorkflowNumber
            and selectedWorkflowNumber == missionWorkflowNumber
        )

        if matchesRequestedWorkflow:
            matchingMissions.append(missionRecord)
        elif missionStatus == "QUEUED":
            queuedMismatches.append(missionRecord)
        else:
            gatedMismatches.append(missionRecord)

    return {
        "matching": matchingMissions,
        "queued_mismatches": queuedMismatches,
        "gated_mismatches": gatedMismatches,
    }


def _mergeMissionCommandMessages(*messages):
    """Join non-empty reconcile messages without creating noisy empty separators."""
    normalized = []
    for message in list(messages or []):
        message = str(message or "").strip()
        if message:
            normalized.append(message)
    return "; ".join(normalized)


def _outputs(ctx, **kwargs):
    """Build PLC outputs for the robot using the current mirrored fleet facts."""
    return buildOutputs(
        ctx["mirror_inputs"],
        ctx["active_workflow_number"],
        **kwargs
    )


def _complete(ctx, ok, level, message, action, outputs=None, workflowNumber=None):
    """Write the cycle result once after a phase decides the robot's outcome."""
    return _finishCycle(
        ctx["robot_name"],
        ctx["next_state"],
        outputs,
        ctx["return_cycle"],
        ok,
        level,
        message,
        action,
        data=_workflowData(workflowNumber),
    )


def _handleHealthGate(ctx):
    """
    Short-circuit when PLC inputs are unhealthy for this robot.

    This intentionally bypasses _complete/_finishCycle because PLC comm fault
    handling writes PLC health tags through writePlcHealthOutputs(...), and we
    want any in-flight retry/backoff state to survive the unhealthy cycle.
    """
    robotName = ctx["robot_name"]
    currentState = ctx["current_state"]
    nextState = ctx["next_state"]
    plcInputs = ctx["plc_inputs"]

    if plcInputs.get("healthy", True):
        return None

    _updateCycleState(
        nextState,
        stateName="fault",
        selectedWorkflowNumber=currentState["selected_workflow_number"],
        requestLatched=currentState["request_latched"],
        missionCreated=currentState["mission_created"],
        missionNeedsFinalized=currentState["mission_needs_finalized"],
        lastResult=plcInputs.get("fault_reason") or "PLC input quality bad",
        lastCommandId=currentState["last_command_id"],
    )
    writeRobotState(robotName, nextState)
    writePlcHealthOutputs(
        robotName,
        fleetFault=False,
        plcCommFault=True,
        controlHealthy=False,
    )
    return ctx["return_cycle"](
        False,
        "warn",
        "Robot [{}] PLC inputs are unhealthy; skipping command evaluation".format(robotName),
        robotName=robotName,
        state=nextState["state"],
        action="plc_comm_fault",
    )


def _handleClearIntent(ctx):
    """Keep matching active work, cancel queued mismatches immediately, and gate non-queued clears on FinalizeOk."""
    currentState = ctx["current_state"]
    nextState = ctx["next_state"]
    activeSummary = dict(ctx.get("active_summary") or {})
    activeWorkflowNumber = ctx["active_workflow_number"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]
    currentCommandId = currentState["last_command_id"]
    activeMissions = list(activeSummary.get("missions") or [])
    hasActiveMissions = bool(activeMissions)

    if not hasActiveMissions:
        staleClearState = bool(
            currentState.get("mission_needs_finalized")
            or str(currentState.get("state") or "").startswith("clear_")
        )
        _updateCycleState(
            nextState,
            stateName="idle",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False if staleClearState else currentState["request_latched"],
            missionCreated=False if staleClearState else currentState["mission_created"],
            missionNeedsFinalized=False,
            lastResult="" if staleClearState else currentState["last_result"],
            lastCommandId="" if staleClearState else currentState["last_command_id"],
        )
        if isRequestCleared(selectedWorkflowNumber):
            return _complete(
                ctx,
                True,
                "info",
                "Robot [{}] idle".format(ctx["robot_name"]),
                action="idle",
                outputs=_outputs(
                    ctx,
                    requestReceived=False,
                    missionNeedsFinalized=False
                ),
            )
        return None

    activeSplit = _splitActiveMissionsByRequest(activeMissions, selectedWorkflowNumber)
    queuedSummary = issueMissionCommands(
        ctx["robot_name"],
        activeSplit["queued_mismatches"],
        selectedWorkflowNumber,
        activeWorkflowNumber,
        ctx["now_epoch_ms"],
        finalizeMissionId=ctx["finalize_mission_id"],
        cancelMissionIds=ctx["cancel_mission_ids"],
    )
    queuedMessage = queuedSummary.get("message") if (
        queuedSummary.get("issued_count")
        or queuedSummary.get("skipped_count")
        or queuedSummary.get("any_failures")
    ) else ""

    if activeSplit["gated_mismatches"]:
        outputs = _outputs(
            ctx,
            requestReceived=bool(selectedWorkflowNumber),
            missionNeedsFinalized=True
        )
        if not ctx["plc_inputs"]["finalize_ok"]:
            pendingMessage = _mergeMissionCommandMessages(
                queuedMessage,
                _clearPendingMessage(activeWorkflowNumber, selectedWorkflowNumber)
            )
            _updateCycleState(
                nextState,
                stateName="mission_active",
                selectedWorkflowNumber=selectedWorkflowNumber,
                requestLatched=False,
                missionCreated=True,
                missionNeedsFinalized=True,
                lastResult=pendingMessage,
                lastCommandId=currentCommandId,
            )
            return _complete(
                ctx,
                True,
                "info",
                "Robot [{}] waiting for FinalizeOk".format(ctx["robot_name"]),
                action="hold_clear_pending",
                outputs=outputs,
                workflowNumber=activeWorkflowNumber,
            )

        gatedSummary = issueMissionCommands(
            ctx["robot_name"],
            activeSplit["gated_mismatches"],
            selectedWorkflowNumber,
            activeWorkflowNumber,
            ctx["now_epoch_ms"],
            finalizeMissionId=ctx["finalize_mission_id"],
            cancelMissionIds=ctx["cancel_mission_ids"],
        )
        clearMessage = _mergeMissionCommandMessages(
            queuedMessage,
            gatedSummary.get("message")
        )
        anyFailures = queuedSummary.get("any_failures") or gatedSummary.get("any_failures")
        failedLevels = list(queuedSummary.get("failed_levels") or []) + list(gatedSummary.get("failed_levels") or [])
        issuedCount = int(queuedSummary.get("issued_count") or 0) + int(gatedSummary.get("issued_count") or 0)

        _updateCycleState(
            nextState,
            stateName="fault" if anyFailures else "mission_active",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult=clearMessage,
            lastCommandId=currentCommandId,
        )
        return _complete(
            ctx,
            not anyFailures,
            "error" if "error" in failedLevels else ("warn" if anyFailures else "info"),
            clearMessage,
            action=(
                "clear_reconcile_failed"
                if anyFailures
                else ("clear_reconcile" if issuedCount else "hold_clear_inflight")
            ),
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    if queuedSummary.get("any_failures"):
        _updateCycleState(
            nextState,
            stateName="fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=queuedMessage,
            lastCommandId=currentCommandId,
        )
        return _complete(
            ctx,
            False,
            "error" if "error" in list(queuedSummary.get("failed_levels") or []) else "warn",
            queuedMessage,
            action="clear_reconcile_failed",
            outputs=_outputs(
                ctx,
                requestReceived=bool(selectedWorkflowNumber)
            ),
            workflowNumber=activeWorkflowNumber,
        )

    if shouldHoldActive(activeWorkflowNumber, selectedWorkflowNumber):
        activeMessage = _mergeMissionCommandMessages(
            "active mission matches requested workflow",
            queuedMessage
        )
        _updateCycleState(
            nextState,
            stateName="mission_active",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=activeMessage,
            lastCommandId=currentCommandId,
        )
        return _complete(
            ctx,
            True,
            "info",
            "Robot [{}] active workflow {} is in progress".format(
                ctx["robot_name"],
                selectedWorkflowNumber
            ),
            action="hold_active",
            outputs=_outputs(
                ctx,
                requestReceived=True,
                requestSuccess=True
            ),
            workflowNumber=selectedWorkflowNumber,
        )

    if queuedSummary.get("issued_count") or queuedSummary.get("skipped_count"):
        inflightMessage = queuedMessage or "waiting for active missions to clear before creating new work"
        _updateCycleState(
            nextState,
            stateName="mission_active",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=inflightMessage,
            lastCommandId=currentCommandId,
        )
        return _complete(
            ctx,
            True,
            "info",
            inflightMessage,
            action="hold_clear_inflight",
            outputs=_outputs(
                ctx,
                requestReceived=bool(selectedWorkflowNumber)
            ),
            workflowNumber=activeWorkflowNumber,
        )

    if currentState["mission_needs_finalized"]:
        _updateCycleState(
            nextState,
            stateName="mission_active",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult="clear request withdrawn",
            lastCommandId=currentCommandId,
        )

    return None


def _handleValidation(ctx):
    """Validate the requested workflow before we treat it like a create candidate."""
    robotName = ctx["robot_name"]
    nextState = ctx["next_state"]
    currentState = ctx["current_state"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]
    reservedWorkflows = ctx["reserved_workflows"]

    if isRequestCleared(selectedWorkflowNumber):
        return None

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if isWorkflowRequestInvalid(
        workflowDef,
        selectedWorkflowNumber,
        robotName,
        isWorkflowAllowedForRobot,
    ):
        _updateCycleState(
            nextState,
            stateName="fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="workflow {} invalid for {}".format(selectedWorkflowNumber, robotName),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            False,
            "warn",
            "Robot [{}] requested invalid workflow {}".format(robotName, selectedWorkflowNumber),
            action="request_invalid",
            outputs=_outputs(
                ctx,
                requestReceived=True,
                requestInvalid=True
            ),
            workflowNumber=selectedWorkflowNumber,
        )

    owner = reservedWorkflows.get(selectedWorkflowNumber)
    if isWorkflowConflict(owner, robotName):
        _updateCycleState(
            nextState,
            stateName="fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            False,
            "warn",
            "Robot [{}] workflow {} conflicts with {}".format(robotName, selectedWorkflowNumber, owner),
            action="request_conflict",
            outputs=_outputs(
                ctx,
                requestReceived=True,
                requestConflict=True
            ),
            workflowNumber=selectedWorkflowNumber,
        )

    reservedWorkflows[selectedWorkflowNumber] = robotName
    return None


def _handleHolds(ctx):
    """Hold stable request/mission states instead of retriggering OTTO actions."""
    nextState = ctx["next_state"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]

    # Intentionally use the mutable next-state snapshot here so same-scan
    # mutations from earlier phases (for example clear-intent reconciliation)
    # can suppress a duplicate create in this pass.
    if shouldHoldLatchedRequest(nextState, selectedWorkflowNumber):
        _updateCycleState(
            nextState,
            stateName="mission_requested",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=nextState["mission_created"],
            missionNeedsFinalized=False,
            lastResult=nextState["last_result"] or "waiting for active mission reconciliation",
            lastCommandId=nextState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "info",
            "Robot [{}] holding requested workflow {}".format(
                ctx["robot_name"],
                selectedWorkflowNumber
            ),
            action="hold_request",
            outputs=_outputs(
                ctx,
                requestReceived=True,
                requestSuccess=bool(nextState["mission_created"])
            ),
            workflowNumber=selectedWorkflowNumber,
        )

    return None


def _handleCreate(ctx):
    """Create a mission when the robot is ready and no earlier phase claimed the cycle."""
    robotName = ctx["robot_name"]
    currentState = ctx["current_state"]
    nextState = ctx["next_state"]
    activeWorkflowNumber = ctx["active_workflow_number"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]
    nowEpochMs = ctx["now_epoch_ms"]

    if not ctx["controller_available_for_work"]:
        _updateCycleState(
            nextState,
            stateName="fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="robot not available for work",
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            False,
            "warn",
            "Robot [{}] is not available for workflow {}".format(robotName, selectedWorkflowNumber),
            action="waiting_available",
            outputs=_outputs(
                ctx,
                requestReceived=True,
                requestRobotNotReady=True
            ),
            workflowNumber=selectedWorkflowNumber,
        )

    if _isActionBackoffActive(currentState, "create", nowEpochMs):
        _updateCycleState(
            nextState,
            stateName="fault",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="waiting {} ms before retrying create".format(
                _remainingActionBackoffMs(currentState, nowEpochMs)
            ),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "warn",
            "Robot [{}] waiting before retrying create".format(robotName),
            action="hold_create_backoff",
            outputs=_outputs(
                ctx,
                requestReceived=True
            ),
            workflowNumber=selectedWorkflowNumber,
        )

    commandId = currentState["last_command_id"] or str(nowEpochMs)
    result = callCreateMission(robotName, selectedWorkflowNumber, ctx["create_mission"])
    _updateCycleState(
        nextState,
        stateName="mission_requested" if result.get("ok") else "fault",
        selectedWorkflowNumber=selectedWorkflowNumber,
        requestLatched=result.get("ok", False),
        missionCreated=result.get("ok", False),
        missionNeedsFinalized=False,
        lastResult=result.get("message", ""),
        lastCommandId=commandId,
        nextActionAllowedEpochMs=None if result.get("ok") else nowEpochMs + RETRY_DELAY_MS,
        lastAttemptAction=None if result.get("ok") else "create",
        retryCount=None if result.get("ok") else int(currentState.get("retry_count") or 0) + 1,
    )
    return _complete(
        ctx,
        result.get("ok", False),
        result.get("level", "info"),
        result.get("message", ""),
        action="create" if result.get("ok") else "create_failed",
        outputs=_outputs(
            ctx,
            requestReceived=True,
            requestSuccess=result.get("ok", False)
        ),
        workflowNumber=selectedWorkflowNumber,
    )


def runRobotWorkflowCycle(
    robotName,
    reservedWorkflows=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMissionId=None,
    cancelMissionIds=None
):
    """
    Evaluate one robot for one loop cycle.

    This is the main orchestration decision point: read PLC demand, reconcile the
    current Active mission set, then either hold or create.
    """
    def _returnCycle(*args, **kwargs):
        result = buildCycleResult(*args, **kwargs)
        payload = dict(result.get("data") or {})
        payload["requested_workflow_number"] = selectedWorkflowNumber
        payload["active_workflow_number"] = activeWorkflowNumber
        result["data"] = payload
        _recordCommandHistory(nowEpochMs, result)
        return result

    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)
    if reservedWorkflows is None:
        reservedWorkflows = {}

    ensureRobotRunnerTags(robotName)

    plcInputs = readPlcInputs(robotName)
    mirrorInputs = readRobotMirrorInputs(robotName)
    currentState = readRobotState(robotName)
    activeSummary = readActiveMissionSummary(robotName)
    activeWorkflowNumber = activeSummary.get("workflow_number")
    selectedWorkflowNumber = plcInputs["requested_workflow_number"]
    controllerAvailableForWork = (
        bool(mirrorInputs.get("available_for_work"))
        or bool(currentState.get("force_robot_ready"))
    )
    nextState = _newCycleState(currentState, nowEpochMs)
    ctx = _buildCycleContext(
        robotName,
        plcInputs=plcInputs,
        mirrorInputs=mirrorInputs,
        currentState=currentState,
        activeSummary=activeSummary,
        activeWorkflowNumber=activeWorkflowNumber,
        selectedWorkflowNumber=selectedWorkflowNumber,
        reservedWorkflows=reservedWorkflows,
        controllerAvailableForWork=controllerAvailableForWork,
        nextState=nextState,
        returnCycle=_returnCycle,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMissionId=finalizeMissionId,
        cancelMissionIds=cancelMissionIds,
    )

    result = _handleHealthGate(ctx)
    if result is not None:
        return result

    if activeWorkflowNumber:
        reservedWorkflows[activeWorkflowNumber] = robotName

    # The command runner is intentionally phased. Each handler either claims the
    # cycle and returns a result or yields to the next phase.
    phaseHandlers = [_handleClearIntent, _handleValidation, _handleHolds]
    for handler in phaseHandlers:
        result = handler(ctx)
        if result is not None:
            return result

    return _handleCreate(ctx)
