from Otto_API.Models.Fleet import normalizeWorkflowNumber


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
    """
    Build the PLC-facing output snapshot for the current cycle.

    Health contract:
    - `control_healthy` is the aggregate "safe to trust Ignition outputs" bit
    - it is `False` whenever either `fleet_fault` or `plc_comm_fault` is `True`
    - PLC code can use `plc_comm_fault` when it needs the specific reason for the unhealthy state
    """
    plcCommFault = bool(plcCommFault)
    fleetFault = bool(fleetFault)
    return {
        "available_for_work": bool(mirrorInputs.available_for_work),
        "active_mission_count": int(mirrorInputs.active_mission_count or 0),
        "charge_level": float(mirrorInputs.charge_level or 0.0),
        "system_state": str(mirrorInputs.system_state or ""),
        "sub_system_state": str(mirrorInputs.sub_system_state or ""),
        "activity_state": str(mirrorInputs.activity_state or ""),
        "place_id": str(mirrorInputs.place_id or ""),
        "place_name": str(mirrorInputs.place_name or ""),
        "container_present": bool(mirrorInputs.container_present),
        "container_id": str(mirrorInputs.container_id or ""),
        "active_workflow_number": normalizeWorkflowNumber(activeWorkflowNumber) or 0,
        "mission_starved": bool(mirrorInputs.mission_starved),
        "mission_ready_for_attachment": bool(mirrorInputs.mission_ready_for_attachment),
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
