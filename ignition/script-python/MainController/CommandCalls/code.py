import time

from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Fleet import Get
from Otto_API.Missions import MissionSorting
from Otto_API.Missions import Post

from MainController.CommandHelpers import buildCycleResult
from MainController.CommandHelpers import buildWorkflowReservedMap
from MainController.CommandHelpers import ensureRobotRunnerTags
from MainController.CommandHelpers import normalizeRobotState
from MainController.CommandHelpers import readActiveMissionSummary
from MainController.CommandHelpers import readPlcInputs
from MainController.CommandHelpers import readRobotMirrorInputs
from MainController.CommandHelpers import readRobotState
from MainController.CommandHelpers import ROBOT_NAMES
from MainController.CommandHelpers import timestampString
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
    requestConflict=False,
    requestInvalid=False,
    missionNeedsFinalized=False
):
    return {
        "available_for_work": bool(mirrorInputs.get("available_for_work")),
        "active_workflow_number": normalizeWorkflowNumber(activeWorkflowNumber) or 0,
        "mission_ready_for_attachment": bool(mirrorInputs.get("mission_ready_for_attachment")),
        "mission_needs_finalized": bool(missionNeedsFinalized),
        "request_conflict": bool(requestConflict),
        "request_invalid": bool(requestInvalid),
    }


def _callCreateMission(robotName, workflowNumber, createMission=None):
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
    if finalizeMission is None:
        finalizeMission = Post.finalizeMission
    return finalizeMission(robotName)


def _buildState(
    stateName,
    nowEpochMs,
    selectedWorkflowNumber=0,
    requestLatched=False,
    missionCreated=False,
    missionNeedsFinalized=False,
    lastResult="",
    lastCommandId=""
):
    return normalizeRobotState({
        "request_latched": requestLatched,
        "selected_workflow_number": normalizeWorkflowNumber(selectedWorkflowNumber) or 0,
        "state": stateName,
        "mission_created": missionCreated,
        "mission_needs_finalized": missionNeedsFinalized,
        "last_command_ts": timestampString(nowEpochMs),
        "last_result": lastResult,
        "last_command_id": lastCommandId,
    })


def runRobotWorkflowCycle(robotName, reservedWorkflows=None, nowEpochMs=None, createMission=None, finalizeMission=None):
    logger = _robotLogger()

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

    if activeWorkflowNumber:
        reservedWorkflows[activeWorkflowNumber] = robotName

    if currentState["mission_needs_finalized"]:
        if not activeWorkflowNumber:
            nextState = _buildState(
                "idle",
                nowEpochMs,
                lastResult="finalize cleared; no active mission remained",
                lastCommandId=currentState["last_command_id"],
            )
            outputs = _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                missionNeedsFinalized=False
            )
            writeRobotState(robotName, nextState)
            writePlcOutputs(robotName, outputs)
            return buildCycleResult(
                True,
                "info",
                "Robot [{}] finalize cleared because no active mission remained".format(robotName),
                robotName=robotName,
                state=nextState["state"],
                action="clear_finalize_pending",
            )

        outputs = _buildOutputs(
            mirrorInputs,
            activeWorkflowNumber,
            missionNeedsFinalized=True
        )
        if plcInputs["finalize_ok"] and activeWorkflowNumber:
            result = _callFinalizeMission(robotName, finalizeMission)
            if result.get("ok"):
                nextState = _buildState(
                    "success",
                    nowEpochMs,
                    selectedWorkflowNumber=currentState["selected_workflow_number"],
                    missionCreated=False,
                    missionNeedsFinalized=False,
                    lastResult=result.get("message", ""),
                    lastCommandId=currentState["last_command_id"],
                )
                outputs["mission_needs_finalized"] = False
                writeRobotState(robotName, nextState)
                writePlcOutputs(robotName, outputs)
                return buildCycleResult(
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
            return buildCycleResult(
                False,
                result.get("level", "error"),
                result.get("message", ""),
                robotName=robotName,
                state=nextState["state"],
                action="finalize_failed",
                data={"workflow_number": activeWorkflowNumber},
            )

        writeRobotState(
            robotName,
            _buildState(
                "finalize_pending",
                nowEpochMs,
                selectedWorkflowNumber=currentState["selected_workflow_number"] or activeWorkflowNumber,
                requestLatched=False,
                missionCreated=True,
                missionNeedsFinalized=True,
                lastResult=currentState["last_result"] or "waiting for FinalizeOk",
                lastCommandId=currentState["last_command_id"],
            )
        )
        writePlcOutputs(robotName, outputs)
        return buildCycleResult(
            True,
            "info",
            "Robot [{}] waiting for FinalizeOk".format(robotName),
            robotName=robotName,
            state="finalize_pending",
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
            return buildCycleResult(
                True,
                "info",
                "Robot [{}] request dropped; finalize pending".format(robotName),
                robotName=robotName,
                state=nextState["state"],
                action="finalize_pending",
                data={"workflow_number": activeWorkflowNumber},
            )

        nextState = _buildState("idle", nowEpochMs)
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(mirrorInputs, activeWorkflowNumber)
        )
        return buildCycleResult(
            True,
            "info",
            "Robot [{}] idle".format(robotName),
            robotName=robotName,
            state=nextState["state"],
            action="idle",
        )

    workflowDef = getWorkflowDef(selectedWorkflowNumber)
    if workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, robotName):
        nextState = _buildState(
            "request_invalid",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            lastResult="workflow {} invalid for {}".format(selectedWorkflowNumber, robotName),
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestInvalid=True
            )
        )
        return buildCycleResult(
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
            lastResult="workflow {} already reserved by {}".format(selectedWorkflowNumber, owner),
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(
                mirrorInputs,
                activeWorkflowNumber,
                requestConflict=True
            )
        )
        return buildCycleResult(
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
            _buildOutputs(mirrorInputs, activeWorkflowNumber)
        )
        return buildCycleResult(
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
            _buildOutputs(mirrorInputs, activeWorkflowNumber)
        )
        return buildCycleResult(
            True,
            "info",
            "Robot [{}] holding requested workflow {}".format(robotName, selectedWorkflowNumber),
            robotName=robotName,
            state=nextState["state"],
            action="hold_request",
            data={"workflow_number": selectedWorkflowNumber},
        )

    if not mirrorInputs["available_for_work"]:
        nextState = _buildState(
            "waiting_available",
            nowEpochMs,
            selectedWorkflowNumber=selectedWorkflowNumber,
            lastResult="robot not available for work",
            lastCommandId=currentState["last_command_id"],
        )
        writeRobotState(robotName, nextState)
        writePlcOutputs(
            robotName,
            _buildOutputs(mirrorInputs, activeWorkflowNumber)
        )
        return buildCycleResult(
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
        _buildOutputs(mirrorInputs, activeWorkflowNumber)
    )
    return buildCycleResult(
        result.get("ok", False),
        result.get("level", "info"),
        result.get("message", ""),
        robotName=robotName,
        state=nextState["state"],
        action="create" if result.get("ok") else "create_failed",
        data={"workflow_number": selectedWorkflowNumber},
    )


def runAllRobotWorkflowCycles(robotNames=None, nowEpochMs=None, createMission=None, finalizeMission=None):
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


def runMainControllerCycle(nowEpochMs=None, createMission=None, finalizeMission=None):
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
        )
    else:
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
