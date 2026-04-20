import time

from MainController.CommandHelpers import appendRuntimeDatasetRow
from MainController.CommandHelpers import buildCommandLogSignature
from MainController.CommandHelpers import buildCycleResult
from MainController.CommandHelpers import COMMAND_HISTORY_HEADERS
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
from MainController.PlcMirror import buildOutputs
from MainController.RobotActions import callCancelMission
from MainController.RobotActions import callCreateMission
from MainController.RobotActions import callFinalizeMission
from MainController.RobotDecisions import buildChangedRequestMessage
from MainController.RobotDecisions import buildSwitchPendingMessage
from MainController.RobotDecisions import isRequestCleared
from MainController.RobotDecisions import isSwitchingWorkflow
from MainController.RobotDecisions import isWorkflowConflict
from MainController.RobotDecisions import isWorkflowRequestInvalid
from MainController.RobotDecisions import shouldClearFinalizeBecauseNoActiveMission
from MainController.RobotDecisions import shouldHoldActive
from MainController.RobotDecisions import shouldHoldCancelRequest
from MainController.RobotDecisions import shouldHoldLatchedRequest
from MainController.RobotDecisions import shouldHoldSwitchCancel
from MainController.RobotDecisions import shouldWaitForStarved
from MainController.WorkflowConfig import getWorkflowDef
from MainController.WorkflowConfig import isWorkflowAllowedForRobot
from MainController.WorkflowConfig import normalizeWorkflowNumber


def _robotLogger():
    return system.util.getLogger("MainController_WorkflowRunner")


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
    if action not in [
        "create_failed",
        "cancel_for_clear_request_failed",
        "cancel_for_switch_failed",
        "finalize_failed",
        "hold_create_backoff",
        "hold_cancel_backoff",
        "hold_switch_cancel_backoff",
        "hold_finalize_backoff",
    ]:
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


def _buildCycleContext(
    robotName,
    *,
    logger,
    plcInputs,
    mirrorInputs,
    currentState,
    activeWorkflowNumber,
    selectedWorkflowNumber,
    reservedWorkflows,
    controllerAvailableForWork,
    nextState,
    returnCycle,
    nowEpochMs,
    createMission,
    finalizeMission,
    cancelMission
):
    """Bundle the mutable per-cycle state so phase handlers stay focused."""
    return {
        "robot_name": robotName,
        "logger": logger,
        "plc_inputs": plcInputs,
        "mirror_inputs": mirrorInputs,
        "current_state": currentState,
        "active_workflow_number": activeWorkflowNumber,
        "selected_workflow_number": selectedWorkflowNumber,
        "reserved_workflows": reservedWorkflows,
        "controller_available_for_work": controllerAvailableForWork,
        "next_state": nextState,
        "return_cycle": returnCycle,
        "now_epoch_ms": nowEpochMs,
        "create_mission": createMission,
        "finalize_mission": finalizeMission,
        "cancel_mission": cancelMission,
    }


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
        stateName="plc_comm_fault",
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


def _handleCancelForClearRequest(ctx, outputs):
    """Cancel the current mission because the PLC request has been cleared."""
    robotName = ctx["robot_name"]
    currentState = ctx["current_state"]
    nextState = ctx["next_state"]
    activeWorkflowNumber = ctx["active_workflow_number"]
    nowEpochMs = ctx["now_epoch_ms"]

    if shouldHoldCancelRequest(currentState):
        _updateCycleState(
            nextState,
            stateName="cancel_requested",
            selectedWorkflowNumber=0,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=currentState["last_result"] or "waiting for canceled mission to clear",
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "info",
            "Robot [{}] waiting for canceled mission to clear".format(robotName),
            action="hold_cancel_request",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    if _isActionBackoffActive(currentState, "cancel_clear", nowEpochMs):
        _updateCycleState(
            nextState,
            stateName="cancel_backoff",
            selectedWorkflowNumber=0,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult="waiting {} ms before retrying cancel".format(
                _remainingActionBackoffMs(currentState, nowEpochMs)
            ),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "warn",
            "Robot [{}] waiting before retrying cancel".format(robotName),
            action="hold_cancel_backoff",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    result = callCancelMission(robotName, ctx["cancel_mission"])
    if result.get("ok"):
        _updateCycleState(
            nextState,
            stateName="cancel_requested",
            selectedWorkflowNumber=0,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult=result.get("message", ""),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            result.get("level", "info"),
            result.get("message", ""),
            action="cancel_for_clear_request",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    _updateCycleState(
        nextState,
        stateName="failed",
        selectedWorkflowNumber=0,
        requestLatched=False,
        missionCreated=currentState["mission_created"] or bool(activeWorkflowNumber),
        missionNeedsFinalized=False,
        lastResult=result.get("message", ""),
        lastCommandId=currentState["last_command_id"],
        nextActionAllowedEpochMs=nowEpochMs + RETRY_DELAY_MS,
        lastAttemptAction="cancel_clear",
        retryCount=int(currentState.get("retry_count") or 0) + 1,
    )
    return _complete(
        ctx,
        False,
        result.get("level", "error"),
        result.get("message", ""),
        action="cancel_for_clear_request_failed",
        outputs=outputs,
        workflowNumber=activeWorkflowNumber,
    )


def _handleSwitchCancel(ctx, outputs):
    """Cancel the old mission after the PLC authorizes switching workflows."""
    robotName = ctx["robot_name"]
    currentState = ctx["current_state"]
    nextState = ctx["next_state"]
    activeWorkflowNumber = ctx["active_workflow_number"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]
    nowEpochMs = ctx["now_epoch_ms"]

    if shouldHoldSwitchCancel(currentState):
        _updateCycleState(
            nextState,
            stateName="switch_cancel_requested",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult=currentState["last_result"] or "waiting for canceled mission to clear",
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "info",
            "Robot [{}] waiting for canceled workflow {} to clear".format(
                robotName,
                activeWorkflowNumber
            ),
            action="hold_switch_cancel",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    if _isActionBackoffActive(currentState, "cancel_switch", nowEpochMs):
        _updateCycleState(
            nextState,
            stateName="switch_cancel_backoff",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult="waiting {} ms before retrying switch cancel".format(
                _remainingActionBackoffMs(currentState, nowEpochMs)
            ),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "warn",
            "Robot [{}] waiting before retrying switch cancel".format(robotName),
            action="hold_switch_cancel_backoff",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    result = callCancelMission(robotName, ctx["cancel_mission"])
    if result.get("ok"):
        _updateCycleState(
            nextState,
            stateName="switch_cancel_requested",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult=result.get("message", ""),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            result.get("level", "info"),
            result.get("message", ""),
            action="cancel_for_switch",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    _updateCycleState(
        nextState,
        stateName="failed",
        selectedWorkflowNumber=selectedWorkflowNumber,
        requestLatched=False,
        missionCreated=currentState["mission_created"] or bool(activeWorkflowNumber),
        missionNeedsFinalized=True,
        lastResult=result.get("message", ""),
        lastCommandId=currentState["last_command_id"],
        nextActionAllowedEpochMs=nowEpochMs + RETRY_DELAY_MS,
        lastAttemptAction="cancel_switch",
        retryCount=int(currentState.get("retry_count") or 0) + 1,
    )
    return _complete(
        ctx,
        False,
        result.get("level", "error"),
        result.get("message", ""),
        action="cancel_for_switch_failed",
        outputs=outputs,
        workflowNumber=activeWorkflowNumber,
    )


def _handleFinalize(ctx, outputs):
    """Finalize the current mission once STARVED and PLC-approved."""
    robotName = ctx["robot_name"]
    logger = ctx["logger"]
    currentState = ctx["current_state"]
    nextState = ctx["next_state"]
    mirrorInputs = ctx["mirror_inputs"]
    activeWorkflowNumber = ctx["active_workflow_number"]
    nowEpochMs = ctx["now_epoch_ms"]

    if shouldWaitForStarved(mirrorInputs):
        if currentState["state"] != "finalize_waiting_starved":
            logger.info(
                "Robot [{}] waiting for mission to become STARVED before finalizing".format(robotName)
            )
        _updateCycleState(
            nextState,
            stateName="finalize_waiting_starved",
            selectedWorkflowNumber=currentState["selected_workflow_number"],
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult="waiting for mission to become STARVED before finalizing",
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "info",
            "Robot [{}] waiting for mission to become STARVED before finalizing".format(robotName),
            action="hold_finalize_waiting_starved",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    if _isActionBackoffActive(currentState, "finalize", nowEpochMs):
        _updateCycleState(
            nextState,
            stateName="finalize_backoff",
            selectedWorkflowNumber=currentState["selected_workflow_number"],
            requestLatched=currentState["request_latched"],
            missionCreated=currentState["mission_created"],
            missionNeedsFinalized=True,
            lastResult="waiting {} ms before retrying finalize".format(
                _remainingActionBackoffMs(currentState, nowEpochMs)
            ),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "warn",
            "Robot [{}] waiting before retrying finalize".format(robotName),
            action="hold_finalize_backoff",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    result = callFinalizeMission(robotName, ctx["finalize_mission"])
    if result.get("ok"):
        _updateCycleState(
            nextState,
            stateName="success",
            selectedWorkflowNumber=currentState["selected_workflow_number"],
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult=result.get("message", ""),
            lastCommandId=currentState["last_command_id"],
        )
        outputs["mission_needs_finalized"] = False
        return _complete(
            ctx,
            True,
            result.get("level", "info"),
            result.get("message", ""),
            action="finalize",
            outputs=outputs,
            workflowNumber=activeWorkflowNumber,
        )

    _updateCycleState(
        nextState,
        stateName="failed",
        selectedWorkflowNumber=currentState["selected_workflow_number"],
        requestLatched=currentState["request_latched"],
        missionCreated=currentState["mission_created"],
        missionNeedsFinalized=True,
        lastResult=result.get("message", ""),
        lastCommandId=currentState["last_command_id"],
        nextActionAllowedEpochMs=nowEpochMs + RETRY_DELAY_MS,
        lastAttemptAction="finalize",
        retryCount=int(currentState.get("retry_count") or 0) + 1,
    )
    return _complete(
        ctx,
        False,
        result.get("level", "error"),
        result.get("message", ""),
        action="finalize_failed",
        outputs=outputs,
        workflowNumber=activeWorkflowNumber,
    )


def _handleReconcile(ctx):
    """Resolve robots that already owe a finalize or cancel outcome."""
    currentState = ctx["current_state"]
    nextState = ctx["next_state"]
    activeWorkflowNumber = ctx["active_workflow_number"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]
    currentCommandId = currentState["last_command_id"]

    if not currentState["mission_needs_finalized"]:
        return None

    if isRequestCleared(selectedWorkflowNumber):
        if shouldClearFinalizeBecauseNoActiveMission(activeWorkflowNumber):
            _updateCycleState(
                nextState,
                stateName="idle",
                selectedWorkflowNumber=0,
                requestLatched=False,
                missionCreated=False,
                missionNeedsFinalized=False,
                lastResult="request cleared; no active mission remained",
                lastCommandId=currentCommandId,
            )
            return _complete(
                ctx,
                True,
                "info",
                "Robot [{}] cleared request with no active mission remaining".format(ctx["robot_name"]),
                action="clear_cancel_pending",
                outputs=_outputs(
                    ctx,
                    requestReceived=False,
                    missionNeedsFinalized=False
                ),
            )

        return _handleCancelForClearRequest(
            ctx,
            _outputs(
                ctx,
                requestReceived=False,
                missionNeedsFinalized=False
            )
        )

    switchingWorkflow = isSwitchingWorkflow(
        selectedWorkflowNumber,
        activeWorkflowNumber,
        currentState,
    )

    if shouldClearFinalizeBecauseNoActiveMission(activeWorkflowNumber):
        if shouldHoldSwitchCancel(currentState) and selectedWorkflowNumber:
            _updateCycleState(
                nextState,
                stateName="idle",
                selectedWorkflowNumber=selectedWorkflowNumber,
                requestLatched=False,
                missionCreated=False,
                missionNeedsFinalized=False,
                lastResult="prior workflow canceled; ready to request {}".format(selectedWorkflowNumber),
                lastCommandId=currentCommandId,
            )
            return None

        _updateCycleState(
            nextState,
            stateName="idle",
            selectedWorkflowNumber=0,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="finalize cleared; no active mission remained",
            lastCommandId=currentCommandId,
        )
        return _complete(
            ctx,
            True,
            "info",
            "Robot [{}] finalize cleared because no active mission remained".format(ctx["robot_name"]),
            action="clear_finalize_pending",
            outputs=_outputs(
                ctx,
                requestReceived=bool(selectedWorkflowNumber),
                missionNeedsFinalized=False
            ),
        )

    outputs = _outputs(
        ctx,
        requestReceived=bool(selectedWorkflowNumber),
        missionNeedsFinalized=True
    )
    if ctx["plc_inputs"]["finalize_ok"]:
        if switchingWorkflow:
            return _handleSwitchCancel(ctx, outputs)
        return _handleFinalize(ctx, outputs)

    pendingStateName = "switch_pending" if switchingWorkflow else "finalize_pending"
    pendingMessage = (
        buildSwitchPendingMessage(activeWorkflowNumber, selectedWorkflowNumber)
        if switchingWorkflow
        else (currentState["last_result"] or "waiting for FinalizeOk")
    )
    _updateCycleState(
        nextState,
        stateName=pendingStateName,
        selectedWorkflowNumber=(
            currentState["selected_workflow_number"]
            or selectedWorkflowNumber
            or activeWorkflowNumber
        ),
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
        action="hold_finalize",
        outputs=outputs,
        workflowNumber=activeWorkflowNumber,
    )


def _handleNoRequest(ctx):
    """Either stay idle or clear active work when the PLC requests workflow 0."""
    nextState = ctx["next_state"]
    currentState = ctx["current_state"]
    activeWorkflowNumber = ctx["active_workflow_number"]

    if not isRequestCleared(ctx["selected_workflow_number"]):
        return None

    if activeWorkflowNumber:
        return _handleCancelForClearRequest(
            ctx,
            _outputs(
                ctx,
                requestReceived=False,
                missionNeedsFinalized=False
            )
        )

    _updateCycleState(
        nextState,
        stateName="idle",
        selectedWorkflowNumber=0,
        requestLatched=False,
        missionCreated=False,
        missionNeedsFinalized=False,
        lastResult="",
        lastCommandId="",
    )
    return _complete(
        ctx,
        True,
        "info",
        "Robot [{}] idle".format(ctx["robot_name"]),
        action="idle",
        outputs=_outputs(ctx),
    )


def _handleValidation(ctx):
    """Validate the requested workflow before we treat it like a create candidate."""
    robotName = ctx["robot_name"]
    nextState = ctx["next_state"]
    currentState = ctx["current_state"]
    activeWorkflowNumber = ctx["active_workflow_number"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]
    reservedWorkflows = ctx["reserved_workflows"]

    if activeWorkflowNumber and activeWorkflowNumber != selectedWorkflowNumber:
        _updateCycleState(
            nextState,
            stateName="finalize_pending",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult=buildChangedRequestMessage(activeWorkflowNumber, selectedWorkflowNumber),
            lastCommandId=currentState["last_command_id"],
        )
        return _complete(
            ctx,
            True,
            "info",
            "Robot [{}] requested workflow changed; finalize current workflow {} before starting {}".format(
                robotName,
                activeWorkflowNumber,
                selectedWorkflowNumber
            ),
            action="finalize_pending_changed_request",
            outputs=_outputs(
                ctx,
                requestReceived=True,
                missionNeedsFinalized=True
            ),
            workflowNumber=activeWorkflowNumber,
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if isWorkflowRequestInvalid(
        workflowDef,
        selectedWorkflowNumber,
        robotName,
        isWorkflowAllowedForRobot,
    ):
        _updateCycleState(
            nextState,
            stateName="request_invalid",
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
            stateName="request_conflict",
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
    currentState = ctx["current_state"]
    activeWorkflowNumber = ctx["active_workflow_number"]
    selectedWorkflowNumber = ctx["selected_workflow_number"]

    if shouldHoldActive(activeWorkflowNumber, selectedWorkflowNumber):
        _updateCycleState(
            nextState,
            stateName="mission_active",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult="active mission matches requested workflow",
            lastCommandId=currentState["last_command_id"],
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

    if shouldHoldLatchedRequest(currentState, selectedWorkflowNumber):
        _updateCycleState(
            nextState,
            stateName="mission_requested",
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=currentState["mission_created"],
            missionNeedsFinalized=False,
            lastResult=currentState["last_result"] or "waiting for active mission reconciliation",
            lastCommandId=currentState["last_command_id"],
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
                requestSuccess=bool(currentState["mission_created"])
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
            stateName="waiting_available",
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
            stateName="create_backoff",
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
        stateName="mission_requested" if result.get("ok") else "failed",
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
    finalizeMission=None,
    cancelMission=None
):
    """
    Evaluate one robot for one loop cycle.

    This is the main orchestration decision point: read PLC demand, compare it to
    fleet state, then either hold, create, finalize, or cancel/switch.
    """
    logger = _robotLogger()

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
        logger=logger,
        plcInputs=plcInputs,
        mirrorInputs=mirrorInputs,
        currentState=currentState,
        activeWorkflowNumber=activeWorkflowNumber,
        selectedWorkflowNumber=selectedWorkflowNumber,
        reservedWorkflows=reservedWorkflows,
        controllerAvailableForWork=controllerAvailableForWork,
        nextState=nextState,
        returnCycle=_returnCycle,
        nowEpochMs=nowEpochMs,
        createMission=createMission,
        finalizeMission=finalizeMission,
        cancelMission=cancelMission,
    )

    result = _handleHealthGate(ctx)
    if result is not None:
        return result

    if activeWorkflowNumber:
        reservedWorkflows[activeWorkflowNumber] = robotName

    # The command runner is intentionally phased. Each handler either claims the
    # cycle and returns a result or yields to the next phase.
    phaseHandlers = [
        _handleReconcile,
        _handleNoRequest,
        _handleValidation,
        _handleHolds,
    ]
    for handler in phaseHandlers:
        result = handler(ctx)
        if result is not None:
            return result

    return _handleCreate(ctx)
