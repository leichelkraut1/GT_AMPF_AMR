from MainController.WorkflowConfig import normalizeWorkflowNumber


def isRequestCleared(selectedWorkflowNumber):
    """Return True when the PLC request is effectively asking for no mission."""
    return not normalizeWorkflowNumber(selectedWorkflowNumber)


def isSwitchingWorkflow(selectedWorkflowNumber, activeWorkflowNumber, currentState):
    """Return True when the requested workflow differs from the robot's active workflow."""
    return bool(
        normalizeWorkflowNumber(selectedWorkflowNumber)
        and normalizeWorkflowNumber(currentState.get("selected_workflow_number"))
        and normalizeWorkflowNumber(selectedWorkflowNumber) != normalizeWorkflowNumber(activeWorkflowNumber)
    )


def shouldHoldCancelRequest(currentState):
    """Return True when a cancel was already sent and we are waiting for the mission to clear."""
    return currentState.get("state") == "cancel_requested"


def shouldHoldSwitchCancel(currentState):
    """Return True when a switch-cancel was already sent and we are waiting for it to clear."""
    return currentState.get("state") == "switch_cancel_requested"


def shouldWaitForStarved(mirrorInputs):
    """Return True when finalize is blocked because the mission is not STARVED yet."""
    return not bool(mirrorInputs.get("mission_starved"))


def shouldClearFinalizeBecauseNoActiveMission(activeWorkflowNumber):
    """Return True when finalize-pending can be cleared because no mission remains."""
    return not normalizeWorkflowNumber(activeWorkflowNumber)


def shouldHoldActive(activeWorkflowNumber, selectedWorkflowNumber):
    """Return True when the active mission already matches the requested workflow."""
    return normalizeWorkflowNumber(activeWorkflowNumber) == normalizeWorkflowNumber(selectedWorkflowNumber)


def shouldHoldLatchedRequest(currentState, selectedWorkflowNumber):
    """Return True when create was already sent for this workflow and we should not retrigger it."""
    return bool(
        currentState.get("request_latched")
        and normalizeWorkflowNumber(currentState.get("selected_workflow_number"))
        == normalizeWorkflowNumber(selectedWorkflowNumber)
    )


def isWorkflowRequestInvalid(workflowDef, selectedWorkflowNumber, robotName, isWorkflowAllowedForRobot):
    """Return True when the requested workflow number is unknown or not allowed on this robot."""
    return workflowDef is None or not isWorkflowAllowedForRobot(selectedWorkflowNumber, robotName)


def isWorkflowConflict(owner, robotName):
    """Return True when another robot already owns the requested workflow."""
    return bool(owner and owner != robotName)


def buildSwitchPendingMessage(activeWorkflowNumber, selectedWorkflowNumber):
    """Build the stable wait message for a requested workflow change."""
    return "waiting for FinalizeOk before canceling workflow {} and switching to {}".format(
        activeWorkflowNumber,
        selectedWorkflowNumber
    )


def buildChangedRequestMessage(activeWorkflowNumber, selectedWorkflowNumber):
    """Build the stable message for a changed request that must reconcile first."""
    return "requested workflow changed from {} to {}; finalize current mission first".format(
        activeWorkflowNumber,
        selectedWorkflowNumber
    )
