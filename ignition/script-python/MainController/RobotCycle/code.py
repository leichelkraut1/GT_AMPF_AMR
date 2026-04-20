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
    lastCommandId=None
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
    owner = reservedWorkflows.get(selectedWorkflowNumber)
    controllerAvailableForWork = (
        bool(mirrorInputs.get("available_for_work"))
        or bool(currentState.get("force_robot_ready"))
    )
    nextState = _newCycleState(currentState, nowEpochMs)

    # ------------------------------------------------------------------
    # Health gating
    # ------------------------------------------------------------------
    if not plcInputs.get("healthy", True):
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
        return _returnCycle(
            False,
            "warn",
            "Robot [{}] PLC inputs are unhealthy; skipping command evaluation".format(robotName),
            robotName=robotName,
            state=nextState["state"],
            action="plc_comm_fault",
        )

    if activeWorkflowNumber:
        reservedWorkflows[activeWorkflowNumber] = robotName

    # ------------------------------------------------------------------
    # Reconcile robots that already owe a finalize/cancel outcome
    # ------------------------------------------------------------------
    if currentState["mission_needs_finalized"]:
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
                    lastCommandId=currentState["last_command_id"],
                )
                outputs = buildOutputs(
                    mirrorInputs,
                    activeWorkflowNumber,
                    requestReceived=False,
                    missionNeedsFinalized=False
                )
                return _finishCycle(
                    robotName,
                    nextState,
                    outputs,
                    _returnCycle,
                    True,
                    "info",
                    "Robot [{}] cleared request with no active mission remaining".format(robotName),
                    action="clear_cancel_pending",
                )

            outputs = buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=False,
                missionNeedsFinalized=False
            )
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
                return _finishCycle(
                    robotName,
                    nextState,
                    outputs,
                    _returnCycle,
                    True,
                    "info",
                    "Robot [{}] waiting for canceled mission to clear".format(robotName),
                    action="hold_cancel_request",
                    data={"workflow_number": activeWorkflowNumber},
                )

            result = callCancelMission(robotName, cancelMission)
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
                return _finishCycle(
                    robotName,
                    nextState,
                    outputs,
                    _returnCycle,
                    True,
                    result.get("level", "info"),
                    result.get("message", ""),
                    action="cancel_for_clear_request",
                    data={"workflow_number": activeWorkflowNumber},
                )

            _updateCycleState(
                nextState,
                stateName="failed",
                selectedWorkflowNumber=0,
                requestLatched=False,
                missionCreated=currentState["mission_created"],
                missionNeedsFinalized=False,
                lastResult=result.get("message", ""),
                lastCommandId=currentState["last_command_id"],
            )
            return _finishCycle(
                robotName,
                nextState,
                outputs,
                _returnCycle,
                False,
                result.get("level", "error"),
                result.get("message", ""),
                action="cancel_for_clear_request_failed",
                data={"workflow_number": activeWorkflowNumber},
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
                    lastCommandId=currentState["last_command_id"],
                )
                # Fall through deliberately so the newly requested workflow can be
                # evaluated and created in the same cycle after the old mission clears.
            else:
                _updateCycleState(
                    nextState,
                    stateName="idle",
                    selectedWorkflowNumber=0,
                    requestLatched=False,
                    missionCreated=False,
                    missionNeedsFinalized=False,
                    lastResult="finalize cleared; no active mission remained",
                    lastCommandId=currentState["last_command_id"],
                )
                outputs = buildOutputs(
                    mirrorInputs,
                    activeWorkflowNumber,
                    requestReceived=bool(selectedWorkflowNumber),
                    missionNeedsFinalized=False
                )
                return _finishCycle(
                    robotName,
                    nextState,
                    outputs,
                    _returnCycle,
                    True,
                    "info",
                    "Robot [{}] finalize cleared because no active mission remained".format(robotName),
                    action="clear_finalize_pending",
                )

        if activeWorkflowNumber:
            outputs = buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=bool(selectedWorkflowNumber),
                missionNeedsFinalized=True
            )
            if plcInputs["finalize_ok"]:
                if switchingWorkflow:
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
                        return _finishCycle(
                            robotName,
                            nextState,
                            outputs,
                            _returnCycle,
                            True,
                            "info",
                            "Robot [{}] waiting for canceled workflow {} to clear".format(
                                robotName,
                                activeWorkflowNumber
                            ),
                            action="hold_switch_cancel",
                            data={"workflow_number": activeWorkflowNumber},
                        )

                    result = callCancelMission(robotName, cancelMission)
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
                        return _finishCycle(
                            robotName,
                            nextState,
                            outputs,
                            _returnCycle,
                            True,
                            result.get("level", "info"),
                            result.get("message", ""),
                            action="cancel_for_switch",
                            data={"workflow_number": activeWorkflowNumber},
                        )

                    _updateCycleState(
                        nextState,
                        stateName="failed",
                        selectedWorkflowNumber=selectedWorkflowNumber,
                        requestLatched=False,
                        missionCreated=currentState["mission_created"],
                        missionNeedsFinalized=True,
                        lastResult=result.get("message", ""),
                        lastCommandId=currentState["last_command_id"],
                    )
                    return _finishCycle(
                        robotName,
                        nextState,
                        outputs,
                        _returnCycle,
                        False,
                        result.get("level", "error"),
                        result.get("message", ""),
                        action="cancel_for_switch_failed",
                        data={"workflow_number": activeWorkflowNumber},
                    )

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
                    return _finishCycle(
                        robotName,
                        nextState,
                        outputs,
                        _returnCycle,
                        True,
                        "info",
                        "Robot [{}] waiting for mission to become STARVED before finalizing".format(robotName),
                        action="hold_finalize_waiting_starved",
                        data={"workflow_number": activeWorkflowNumber},
                    )

                result = callFinalizeMission(robotName, finalizeMission)
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
                    return _finishCycle(
                        robotName,
                        nextState,
                        outputs,
                        _returnCycle,
                        True,
                        result.get("level", "info"),
                        result.get("message", ""),
                        action="finalize",
                        data={"workflow_number": activeWorkflowNumber},
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
                )
                return _finishCycle(
                    robotName,
                    nextState,
                    outputs,
                    _returnCycle,
                    False,
                    result.get("level", "error"),
                    result.get("message", ""),
                    action="finalize_failed",
                    data={"workflow_number": activeWorkflowNumber},
                )

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
                lastCommandId=currentState["last_command_id"],
            )
            return _finishCycle(
                robotName,
                nextState,
                outputs,
                _returnCycle,
                True,
                "info",
                "Robot [{}] waiting for FinalizeOk".format(robotName),
                action="hold_finalize",
                data={"workflow_number": activeWorkflowNumber},
            )

    # ------------------------------------------------------------------
    # No requested workflow: either clear active work or stay idle
    # ------------------------------------------------------------------
    if isRequestCleared(plcInputs["requested_workflow_number"]):
        if activeWorkflowNumber:
            outputs = buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=False,
                missionNeedsFinalized=False
            )
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
                return _finishCycle(
                    robotName,
                    nextState,
                    outputs,
                    _returnCycle,
                    True,
                    "info",
                    "Robot [{}] waiting for canceled mission to clear".format(robotName),
                    action="hold_cancel_request",
                    data={"workflow_number": activeWorkflowNumber},
                )

            result = callCancelMission(robotName, cancelMission)
            _updateCycleState(
                nextState,
                stateName="cancel_requested" if result.get("ok") else "failed",
                selectedWorkflowNumber=0,
                requestLatched=False,
                missionCreated=True,
                missionNeedsFinalized=False,
                lastResult=result.get("message", ""),
                lastCommandId=currentState["last_command_id"],
            )
            return _finishCycle(
                robotName,
                nextState,
                outputs,
                _returnCycle,
                result.get("ok", False),
                result.get("level", "info" if result.get("ok") else "error"),
                result.get("message", ""),
                action="cancel_for_clear_request" if result.get("ok") else "cancel_for_clear_request_failed",
                data={"workflow_number": activeWorkflowNumber},
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
        return _finishCycle(
            robotName,
            nextState,
            buildOutputs(mirrorInputs, activeWorkflowNumber),
            _returnCycle,
            True,
            "info",
            "Robot [{}] idle".format(robotName),
            action="idle",
        )

    # ------------------------------------------------------------------
    # New or changed workflow request validation
    # ------------------------------------------------------------------
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
        outputs = buildOutputs(
            mirrorInputs,
            activeWorkflowNumber,
            requestReceived=True,
            missionNeedsFinalized=True
        )
        return _finishCycle(
            robotName,
            nextState,
            outputs,
            _returnCycle,
            True,
            "info",
            "Robot [{}] requested workflow changed; finalize current workflow {} before starting {}".format(
                robotName,
                activeWorkflowNumber,
                selectedWorkflowNumber
            ),
            action="finalize_pending_changed_request",
            data={"workflow_number": activeWorkflowNumber},
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
        return _finishCycle(
            robotName,
            nextState,
            buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestInvalid=True
            ),
            _returnCycle,
            False,
            "warn",
            "Robot [{}] requested invalid workflow {}".format(robotName, selectedWorkflowNumber),
            action="request_invalid",
            data={"workflow_number": selectedWorkflowNumber},
        )

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
        return _finishCycle(
            robotName,
            nextState,
            buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestConflict=True
            ),
            _returnCycle,
            False,
            "warn",
            "Robot [{}] workflow {} conflicts with {}".format(robotName, selectedWorkflowNumber, owner),
            action="request_conflict",
            data={"workflow_number": selectedWorkflowNumber},
        )

    reservedWorkflows[selectedWorkflowNumber] = robotName

    # ------------------------------------------------------------------
    # Hold existing matching request/mission states
    # ------------------------------------------------------------------
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
        return _finishCycle(
            robotName,
            nextState,
            buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestSuccess=True
            ),
            _returnCycle,
            True,
            "info",
            "Robot [{}] active workflow {} is in progress".format(robotName, selectedWorkflowNumber),
            action="hold_active",
            data={"workflow_number": selectedWorkflowNumber},
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
        return _finishCycle(
            robotName,
            nextState,
            buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestSuccess=bool(currentState["mission_created"])
            ),
            _returnCycle,
            True,
            "info",
            "Robot [{}] holding requested workflow {}".format(robotName, selectedWorkflowNumber),
            action="hold_request",
            data={"workflow_number": selectedWorkflowNumber},
        )

    # ------------------------------------------------------------------
    # Create a new mission when the robot is ready and no hold path applies
    # ------------------------------------------------------------------
    if not controllerAvailableForWork:
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
        return _finishCycle(
            robotName,
            nextState,
            buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestRobotNotReady=True
            ),
            _returnCycle,
            False,
            "warn",
            "Robot [{}] is not available for workflow {}".format(robotName, selectedWorkflowNumber),
            action="waiting_available",
            data={"workflow_number": selectedWorkflowNumber},
        )

    commandId = currentState["last_command_id"] or str(nowEpochMs)
    result = callCreateMission(robotName, selectedWorkflowNumber, createMission)
    _updateCycleState(
        nextState,
        stateName="mission_requested" if result.get("ok") else "failed",
        selectedWorkflowNumber=selectedWorkflowNumber,
        requestLatched=result.get("ok", False),
        missionCreated=result.get("ok", False),
        missionNeedsFinalized=False,
        lastResult=result.get("message", ""),
        lastCommandId=commandId,
    )
    return _finishCycle(
        robotName,
        nextState,
        buildOutputs(
            mirrorInputs,
            activeWorkflowNumber,
            requestReceived=True,
            requestSuccess=result.get("ok", False)
        ),
        _returnCycle,
        result.get("ok", False),
        result.get("level", "info"),
        result.get("message", ""),
        action="create" if result.get("ok") else "create_failed",
        data={"workflow_number": selectedWorkflowNumber},
    )
