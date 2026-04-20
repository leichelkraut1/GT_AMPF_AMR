from MainController.WorkflowConfig import normalizeWorkflowNumber


def isRequestCleared(selectedWorkflowNumber):
    """Return True when the PLC request is effectively asking for no mission."""
    return not normalizeWorkflowNumber(selectedWorkflowNumber)

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
