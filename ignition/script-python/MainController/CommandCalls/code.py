import time

from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Fleet import Get
from Otto_API.Missions import MissionSorting
from Otto_API.Missions import Post

from MainController.CommandHelpers import buildCycleResult
from MainController.CommandHelpers import buildCommandLogSignature
from MainController.CommandHelpers import buildWorkflowReservedMap
from MainController.CommandHelpers import COMMAND_HISTORY_HEADERS
from MainController.CommandHelpers import ensureRobotRunnerTags
from MainController.CommandHelpers import normalizeRobotState
from MainController.CommandHelpers import appendRuntimeDatasetRow
from MainController.CommandHelpers import readActiveMissionSummary
from MainController.CommandHelpers import readPlcInputs
from MainController.CommandHelpers import readRobotMirrorInputs
from MainController.CommandHelpers import readRobotState
from MainController.CommandHelpers import ROBOT_NAMES
from MainController.CommandHelpers import timestampString
from MainController.CommandHelpers import writePlcHealthOutputs
from MainController.CommandHelpers import writePlcOutputs
from MainController.CommandHelpers import writeRobotState
from MainController.WorkflowConfig import buildMissionName
from MainController.WorkflowConfig import getWorkflowDef
from MainController.WorkflowConfig import isWorkflowAllowedForRobot
from MainController.WorkflowConfig import normalizeWorkflowNumber
from MainController.WorkflowConfig import robotIdTagPath
from MainController.WorkflowConfig import workflowTemplateTagPath


def _robotLogger():
    return system.util.getLogger("MainController_WorkflowRunner")


def _buildOutputs(
    mirrorInputs,
    activeWorkflowNumber,
    requestReceived=False,
    requestSuccess=False,
    requestRobotNotReady=False,
    fleetFault=False,
    plcCommFault=False,
    requestConflict=False,
    requestInvalid=False,
    missionNeedsFinalized=False
):
    """Build the PLC-facing output snapshot for the current cycle."""
    plcCommFault = bool(plcCommFault)
    fleetFault = bool(fleetFault)
    return {
        "available_for_work": bool(mirrorInputs.get("available_for_work")),
        "active_workflow_number": normalizeWorkflowNumber(activeWorkflowNumber) or 0,
        "mission_ready_for_attachment": bool(mirrorInputs.get("mission_ready_for_attachment")),
        "mission_needs_finalized": bool(missionNeedsFinalized),
        "request_received": bool(requestReceived),
        "request_success": bool(requestSuccess),
        "request_robot_not_ready": bool(requestRobotNotReady),
        "fleet_fault": fleetFault,
        "plc_comm_fault": plcCommFault,
        "control_healthy": not fleetFault and not plcCommFault,
        "request_conflict": bool(requestConflict),
        "request_invalid": bool(requestInvalid),
    }


def _callCreateMission(robotName, workflowNumber, createMission=None):
    """Create a mission for the selected workflow using the configured OTTO template."""
    if createMission is None:
        createMission = Post.createMission

    templateTagPath = workflowTemplateTagPath(workflowNumber)
    missionName = buildMissionName(workflowNumber, robotName)
    return createMission(
        templateTagPath=templateTagPath,
        robotTagPath=robotIdTagPath(robotName),
        missionName=missionName,
    )


def _callFinalizeMission(robotName, finalizeMission=None):
    """Finalize the active mission for a robot."""
    if finalizeMission is None:
        finalizeMission = Post.finalizeMission
    return finalizeMission(robotName)


def _callCancelMission(robotName, cancelMission=None):
    """Cancel the active mission for a robot."""
    if cancelMission is None:
        cancelMission = Post.cancelMission
    return cancelMission(robotName)


def _buildState(
    stateName,
    nowEpochMs,
    selectedWorkflowNumber=None,
    requestLatched=None,
    missionCreated=None,
    missionNeedsFinalized=None,
    lastResult=None,
    lastCommandId=None
):
    """Build a robot-state patch for one controller decision."""
    patch = {
        "state": stateName,
        "last_command_ts": timestampString(nowEpochMs),
    }
    if requestLatched is not None:
        patch["request_latched"] = requestLatched
    if selectedWorkflowNumber is not None:
        patch["selected_workflow_number"] = normalizeWorkflowNumber(selectedWorkflowNumber) or 0
    if missionCreated is not None:
        patch["mission_created"] = missionCreated
    if missionNeedsFinalized is not None:
        patch["mission_needs_finalized"] = missionNeedsFinalized
    if lastResult is not None:
        patch["last_result"] = lastResult
    if lastCommandId is not None:
        patch["last_command_id"] = lastCommandId
    return patch


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
    # ForceRobotReady is a controller-only override for testing; it should not rewrite
    # the real Fleet AvailableForWork signal.
    controllerAvailableForWork = (
        bool(mirrorInputs.get("available_for_work"))
        or bool(currentState.get("force_robot_ready"))
    )

    if not plcInputs.get("healthy", True):
        nextState = _buildState(
            "plc_comm_fault",
            nowEpochMs,
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

    if currentState["mission_needs_finalized"]:
        switchingWorkflow = bool(
            selectedWorkflowNumber
            and currentState["selected_workflow_number"]
            and selectedWorkflowNumber != activeWorkflowNumber
        )

        if not activeWorkflowNumber:
            # Once the prior mission is gone, a switch request can fall back into the
            # normal create path on the next cycle.
            if currentState["state"] == "switch_cancel_requested" and selectedWorkflowNumber:
                currentState = _buildState(
                    "idle",
                    nowEpochMs,
                    selectedWorkflowNumber=selectedWorkflowNumber,
                    requestLatched=False,
                    missionCreated=False,
                    missionNeedsFinalized=False,
                    lastResult="prior workflow canceled; ready to request {}".format(selectedWorkflowNumber),
                    lastCommandId=currentState["last_command_id"],
                )
                writeRobotState(robotName, currentState)
            else:
                nextState = _buildState(
                    "idle",
                    nowEpochMs,
                    selectedWorkflowNumber=0,
                    requestLatched=False,
                    missionCreated=False,
                    missionNeedsFinalized=False,
                    lastResult="finalize cleared; no active mission remained",
                    lastCommandId=currentState["last_command_id"],
                )
                outputs = _buildOutputs(
                    mirrorInputs,
                    activeWorkflowNumber,
                    requestReceived=bool(selectedWorkflowNumber),
                    missionNeedsFinalized=False
                )
                writeRobotState(robotName, nextState)
                writePlcOutputs(robotName, outputs)
                return _returnCycle(
                    True,
                    "info",
                    "Robot [{}] finalize cleared because no active mission remained".format(robotName),
                    robotName=robotName,
                    state=nextState["state"],
                    action="clear_finalize_pending",
                )

        if activeWorkflowNumber:
            outputs = _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=bool(selectedWorkflowNumber),
                missionNeedsFinalized=True
            )
            if plcInputs["finalize_ok"]:
                if switchingWorkflow:
                    # For workflow mismatch we cancel-and-replace after the PLC says it is
                    # safe, rather than trying to send finalize and cancel together.
                    if currentState["state"] == "switch_cancel_requested":
                        writeRobotState(
                            robotName,
                            _buildState(
                                "switch_cancel_requested",
                                nowEpochMs,
                                selectedWorkflowNumber=selectedWorkflowNumber,
                                requestLatched=False,
                                missionCreated=True,
                                missionNeedsFinalized=True,
                                lastResult=currentState["last_result"] or "waiting for canceled mission to clear",
                                lastCommandId=currentState["last_command_id"],
                            )
                        )
                        writePlcOutputs(robotName, outputs)
                        return _returnCycle(
                            True,
                            "info",
                            "Robot [{}] waiting for canceled workflow {} to clear".format(
                                robotName,
                                activeWorkflowNumber
                            ),
                            robotName=robotName,
                            state="switch_cancel_requested",
                            action="hold_switch_cancel",
                            data={"workflow_number": activeWorkflowNumber},
                        )

                    result = _callCancelMission(robotName, cancelMission)
                    if result.get("ok"):
                        nextState = _buildState(
                            "switch_cancel_requested",
                            nowEpochMs,
                            selectedWorkflowNumber=selectedWorkflowNumber,
                            requestLatched=False,
                            missionCreated=True,
                            missionNeedsFinalized=True,
                            lastResult=result.get("message", ""),
                            lastCommandId=currentState["last_command_id"],
                        )
                        writeRobotState(robotName, nextState)
                        writePlcOutputs(robotName, outputs)
                        return _returnCycle(
                            True,
                            result.get("level", "info"),
                            result.get("message", ""),
                            robotName=robotName,
                            state=nextState["state"],
                            action="cancel_for_switch",
                            data={"workflow_number": activeWorkflowNumber},
                        )

                    nextState = _buildState(
                        "failed",
                        nowEpochMs,
                        selectedWorkflowNumber=selectedWorkflowNumber,
                        requestLatched=False,
                        missionCreated=currentState["mission_created"],
                        missionNeedsFinalized=True,
                        lastResult=result.get("message", ""),
                        lastCommandId=currentState["last_command_id"],
                    )
                    writeRobotState(robotName, nextState)
                    writePlcOutputs(robotName, outputs)
                    return _returnCycle(
                        False,
                        result.get("level", "error"),
                        result.get("message", ""),
                        robotName=robotName,
                        state=nextState["state"],
                        action="cancel_for_switch_failed",
                        data={"workflow_number": activeWorkflowNumber},
                    )

                result = _callFinalizeMission(robotName, finalizeMission)
                if result.get("ok"):
                    nextState = _buildState(
                        "success",
                        nowEpochMs,
                        selectedWorkflowNumber=currentState["selected_workflow_number"],
                        requestLatched=False,
                        missionCreated=False,
                        missionNeedsFinalized=False,
                        lastResult=result.get("message", ""),
                        lastCommandId=currentState["last_command_id"],
                    )
                    outputs["mission_needs_finalized"] = False
                    writeRobotState(robotName, nextState)
                    writePlcOutputs(robotName, outputs)
                    return _returnCycle(
                        True,
                        result.get("level", "info"),
                        result.get("message", ""),
                        robotName=robotName,
                        state=nextState["state"],
                        action="finalize",
                        data={"workflow_number": activeWorkflowNumber},
                    )

                nextState = _buildState(
                    "failed",
                    nowEpochMs,
                    selectedWorkflowNumber=currentState["selected_workflow_number"],
                    requestLatched=currentState["request_latched"],
                    missionCreated=currentState["mission_created"],
                    missionNeedsFinalized=True,
                    lastResult=result.get("message", ""),
                    lastCommandId=currentState["last_command_id"],
                )
                writeRobotState(robotName, nextState)
                writePlcOutputs(robotName, outputs)
                return _returnCycle(
                    False,
                    result.get("level", "error"),
                    result.get("message", ""),
                    robotName=robotName,
                    state=nextState["state"],
                    action="finalize_failed",
                    data={"workflow_number": activeWorkflowNumber},
                )

            pendingStateName = "switch_pending" if switchingWorkflow else "finalize_pending"
            pendingMessage = (
                "waiting for FinalizeOk before canceling workflow {} and switching to {}".format(
                    activeWorkflowNumber,
                    selectedWorkflowNumber
                )
                if switchingWorkflow
                else (currentState["last_result"] or "waiting for FinalizeOk")
            )
            writeRobotState(
                robotName,
                _buildState(
                    pendingStateName,
                    nowEpochMs,
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
            )
            writePlcOutputs(robotName, outputs)
            return _returnCycle(
                True,
                "info",
                "Robot [{}] waiting for FinalizeOk".format(robotName),
                robotName=robotName,
                state=pendingStateName,
                action="hold_finalize",
                data={"workflow_number": activeWorkflowNumber},
            )

    if not plcInputs["request_active"]:
        if activeWorkflowNumber:
            nextState = _buildState(
                "finalize_pending",
                nowEpochMs,
                selectedWorkflowNumber=activeWorkflowNumber,
                requestLatched=False,
                missionCreated=True,
                missionNeedsFinalized=True,
                lastResult="request dropped; finalize required",
                lastCommandId=currentState["last_command_id"],
            )
            outputs = _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                missionNeedsFinalized=True
            )
            writeRobotState(robotName, nextState)
            writePlcOutputs(robotName, outputs)
            return _returnCycle(
                True,
                "info",
                "Robot [{}] request dropped; finalize pending".format(robotName),
                robotName=robotName,
                state=nextState["state"],
                action="finalize_pending",
                data={"workflow_number": activeWorkflowNumber},
            )

        nextState = _buildState("idle", nowEpochMs)
        nextState["selected_workflow_number"] = 0
        nextState["request_latched"] = False
        nextState["mission_created"] = False
        nextState["mission_needs_finalized"] = False
        nextState["last_result"] = ""
        nextState["last_command_id"] = ""
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(mirrorInputs, activeWorkflowNumber)
        )
        return _returnCycle(
            True,
            "info",
            "Robot [{}] idle".format(robotName),
            robotName=robotName,
            state=nextState["state"],
            action="idle",
        )

    if activeWorkflowNumber and activeWorkflowNumber != selectedWorkflowNumber:
        # A new nonzero request against an already-active workflow is reconciled through
        # the finalize/cancel path above before a replacement mission is created.
        nextState = _buildState(
            "finalize_pending",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=True,
            missionNeedsFinalized=True,
            lastResult="requested workflow changed from {} to {}; finalize current mission first".format(
                activeWorkflowNumber,
                selectedWorkflowNumber
            ),
            lastCommandId=currentState["last_command_id"],
        )
        outputs = _buildOutputs(
            mirrorInputs,
            activeWorkflowNumber,
            requestReceived=True,
            missionNeedsFinalized=True
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(robotName, outputs)
        return _returnCycle(
            True,
            "info",
            "Robot [{}] requested workflow changed; finalize current workflow {} before starting {}".format(
                robotName,
                activeWorkflowNumber,
                selectedWorkflowNumber
            ),
            robotName=robotName,
            state=nextState["state"],
            action="finalize_pending_changed_request",
            data={"workflow_number": activeWorkflowNumber},
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, robotName):
        nextState = _buildState(
            "request_invalid",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="workflow {} invalid for {}".format(selectedWorkflowNumber, robotName),
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestInvalid=True
            )
        )
        return _returnCycle(
            False,
            "warn",
            "Robot [{}] requested invalid workflow {}".format(robotName, selectedWorkflowNumber),
            robotName=robotName,
            state=nextState["state"],
            action="request_invalid",
            data={"workflow_number": selectedWorkflowNumber},
        )

    if owner and owner != robotName:
        nextState = _buildState(
            "request_conflict",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestConflict=True
            )
        )
        return _returnCycle(
            False,
            "warn",
            "Robot [{}] workflow {} conflicts with {}".format(robotName, selectedWorkflowNumber, owner),
            robotName=robotName,
            state=nextState["state"],
            action="request_conflict",
            data={"workflow_number": selectedWorkflowNumber},
        )

    reservedWorkflows[selectedWorkflowNumber] = robotName

    if activeWorkflowNumber == selectedWorkflowNumber:
        nextState = _buildState(
            "mission_active",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=True,
            missionNeedsFinalized=False,
            lastResult="active mission matches requested workflow",
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestSuccess=True
            )
        )
        return _returnCycle(
            True,
            "info",
            "Robot [{}] active workflow {} is in progress".format(robotName, selectedWorkflowNumber),
            robotName=robotName,
            state=nextState["state"],
            action="hold_active",
            data={"workflow_number": selectedWorkflowNumber},
        )

    if currentState["request_latched"] and currentState["selected_workflow_number"] == selectedWorkflowNumber:
        nextState = _buildState(
            "mission_requested",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=True,
            missionCreated=currentState["mission_created"],
            missionNeedsFinalized=False,
            lastResult=currentState["last_result"] or "waiting for active mission reconciliation",
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestSuccess=bool(currentState["mission_created"])
            )
        )
        return _returnCycle(
            True,
            "info",
            "Robot [{}] holding requested workflow {}".format(robotName, selectedWorkflowNumber),
            robotName=robotName,
            state=nextState["state"],
            action="hold_request",
            data={"workflow_number": selectedWorkflowNumber},
        )

    if not controllerAvailableForWork:
        nextState = _buildState(
            "waiting_available",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            requestLatched=False,
            missionCreated=False,
            missionNeedsFinalized=False,
            lastResult="robot not available for work",
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestReceived=True,
                requestRobotNotReady=True
            )
        )
        return _returnCycle(
            False,
            "warn",
            "Robot [{}] is not available for workflow {}".format(robotName, selectedWorkflowNumber),
            robotName=robotName,
            state=nextState["state"],
            action="waiting_available",
            data={"workflow_number": selectedWorkflowNumber},
        )

    commandId = currentState["last_command_id"] or str(nowEpochMs)
    result = _callCreateMission(robotName, selectedWorkflowNumber, createMission)
    nextState = _buildState(
        "mission_requested" if result.get("ok") else "failed",
        nowEpochMs,
        selectedWorkflowNumber=selectedWorkflowNumber,
        requestLatched=result.get("ok", False),
        missionCreated=result.get("ok", False),
        missionNeedsFinalized=False,
        lastResult=result.get("message", ""),
        lastCommandId=commandId,
    )
    writeRobotState(robotName, nextState)
    writePlcOutputs(
        robotName,
        _buildOutputs(
            mirrorInputs,
            activeWorkflowNumber,
            requestReceived=True,
            requestSuccess=result.get("ok", False)
        )
    )
    return _returnCycle(
        result.get("ok", False),
        result.get("level", "info"),
        result.get("message", ""),
        robotName=robotName,
        state=nextState["state"],
        action="create" if result.get("ok") else "create_failed",
        data={"workflow_number": selectedWorkflowNumber},
    )


def runAllRobotWorkflowCycles(
    robotNames=None,
    nowEpochMs=None,
    createMission=None,
    finalizeMission=None,
    cancelMission=None
):
    """Run one workflow-controller pass across every configured robot."""
    if robotNames is None:
        robotNames = ROBOT_NAMES
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    reservedWorkflows = buildWorkflowReservedMap(robotNames)
    results = []

    for robotName in list(robotNames or []):
        results.append(
            runRobotWorkflowCycle(
                robotName,
                reservedWorkflows=reservedWorkflows,
                nowEpochMs=nowEpochMs,
                createMission=createMission,
                finalizeMission=finalizeMission,
                cancelMission=cancelMission,
            )
        )

    ok = all(result.get("ok", False) or result.get("level") == "warn" for result in results)
    level = "info" if ok else "error"
    return buildOperationResult(
        ok,
        level,
        "Processed workflow cycles for {} robot(s)".format(len(results)),
        data={"results": results},
        results=results,
    )


def runMainControllerCycle(nowEpochMs=None, createMission=None, finalizeMission=None, cancelMission=None):
    """
    Run the ordered controller phases for one main-loop cycle.

    The PLC workflow pass only runs after fleet state and mission state have both
    been refreshed in the same cycle.
    """
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    serverStatusResult = Get.getServerStatus()
    robotStateResult = Get.updateRobotOperationalState()
    missionSortResult = MissionSorting.run()

    canEvaluatePlc = robotStateResult.get("ok") and missionSortResult.get("ok")
    if canEvaluatePlc:
        workflowResult = runAllRobotWorkflowCycles(
            nowEpochMs=nowEpochMs,
            createMission=createMission,
            finalizeMission=finalizeMission,
            cancelMission=cancelMission,
        )
    else:
        # When fleet/mission state is stale, stop advancing commands and expose a PLC-visible fault.
        for robotName in ROBOT_NAMES:
            ensureRobotRunnerTags(robotName)
            writePlcHealthOutputs(
                robotName,
                fleetFault=True,
                plcCommFault=False,
                controlHealthy=False,
            )
        workflowResult = buildOperationResult(
            False,
            "warn",
            "Skipped PLC workflow evaluation because robot or mission state is stale",
            data=None,
        )

    ok = canEvaluatePlc and workflowResult.get("ok", False)
    if not serverStatusResult.get("ok", False):
        level = "warn"
    else:
        level = "info" if ok else "warn"

    return buildOperationResult(
        ok,
        level,
        "MainController cycle completed",
        data={
            "server_status": serverStatusResult,
            "robot_state": robotStateResult,
            "mission_sorting": missionSortResult,
            "workflow_cycles": workflowResult,
        },
        server_status=serverStatusResult,
        robot_state=robotStateResult,
        mission_sorting=missionSortResult,
        workflow_cycles=workflowResult,
    )
