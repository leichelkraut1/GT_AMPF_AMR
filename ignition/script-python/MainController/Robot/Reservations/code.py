from MainController.WorkflowConfig import normalizeWorkflowNumber


def reserveWorkflow(reservedWorkflows, workflowNumber, robotName):
    """Reserve one normalized workflow number for one robot in the shared cycle map."""
    normalizedWorkflowNumber = normalizeWorkflowNumber(workflowNumber)
    if not normalizedWorkflowNumber:
        return reservedWorkflows
    existingOwner = str(reservedWorkflows.get(normalizedWorkflowNumber) or "")
    if existingOwner and existingOwner != str(robotName or ""):
        return reservedWorkflows
    reservedWorkflows[normalizedWorkflowNumber] = str(robotName or "")
    return reservedWorkflows


def buildReservedWorkflowsFromSnapshots(snapshots):
    """Build the whole-cycle workflow reservation map before robot decisions run."""
    reserved = {}
    for snapshot in list(snapshots or []):
        snapshot = dict(snapshot or {})
        robotName = snapshot.get("robot_name")
        activeWorkflowNumber = normalizeWorkflowNumber(
            dict(snapshot.get("active_summary") or {}).get("workflow_number")
        )
        if activeWorkflowNumber:
            reserveWorkflow(reserved, activeWorkflowNumber, robotName)
            continue

        currentState = dict(snapshot.get("current_state") or {})
        selectedWorkflowNumber = normalizeWorkflowNumber(
            currentState.get("selected_workflow_number")
        )
        if selectedWorkflowNumber and currentState.get("mission_created"):
            reserveWorkflow(reserved, selectedWorkflowNumber, robotName)
    return reserved


def attachReservedWorkflows(snapshots, reservedWorkflows):
    """Attach the same live reservation map to every snapshot in the controller batch."""
    for snapshot in list(snapshots or []):
        snapshot["reserved_workflows"] = reservedWorkflows
    return snapshots
