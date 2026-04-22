from MainController.WorkflowConfig import normalizeWorkflowNumber


def buildOutputs(
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
        "active_mission_count": int(mirrorInputs.get("active_mission_count") or 0),
        "charge_level": float(mirrorInputs.get("charge_level") or 0.0),
        "system_state": str(mirrorInputs.get("system_state") or ""),
        "sub_system_state": str(mirrorInputs.get("sub_system_state") or ""),
        "activity_state": str(mirrorInputs.get("activity_state") or ""),
        "place_id": str(mirrorInputs.get("place_id") or ""),
        "place_name": str(mirrorInputs.get("place_name") or ""),
        "active_workflow_number": normalizeWorkflowNumber(activeWorkflowNumber) or 0,
        "mission_starved": bool(mirrorInputs.get("mission_starved")),
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
