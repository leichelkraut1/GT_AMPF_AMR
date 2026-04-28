from MainController.Robot.Records import _coerceRobotCycleSnapshot
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
        snapshot = _coerceRobotCycleSnapshot(snapshot)
        robotName = snapshot.robot_name
        activeWorkflowNumber = normalizeWorkflowNumber(snapshot.active_summary.workflow_number)
        if activeWorkflowNumber:
            reserveWorkflow(reserved, activeWorkflowNumber, robotName)
            continue

        selectedWorkflowNumber = normalizeWorkflowNumber(
            snapshot.current_state.selected_workflow_number
        )
        if selectedWorkflowNumber and snapshot.current_state.mission_created:
            reserveWorkflow(reserved, selectedWorkflowNumber, robotName)
    return reserved


def attachReservedWorkflows(snapshots, reservedWorkflows):
    """Attach the same live reservation map to every snapshot in the controller batch."""
    for index, snapshot in enumerate(list(snapshots or [])):
        snapshot = _coerceRobotCycleSnapshot(snapshot)
        snapshot.reserved_workflows = reservedWorkflows
        snapshots[index] = snapshot
    return snapshots
