from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import writeRequiredTagValues

from MainController.State.Coerce import toBool
from MainController.State.Paths import plcPaths
from MainController.WorkflowConfig import normalizeWorkflowNumber


def readPlcInputs(robotName):
    """Read PLC demand and handshake inputs."""
    paths = plcPaths(robotName)
    readResults = readTagValues([
        paths["requested_workflow_number"],
        paths["finalize_ok"],
    ])
    qualities = [qualifiedValue.quality.isGood() for qualifiedValue in readResults]
    requestedWorkflowNumber = normalizeWorkflowNumber(
        readResults[0].value if qualities[0] else 0
    ) or 0
    return {
        "requested_workflow_number": requestedWorkflowNumber,
        "finalize_ok": toBool(readResults[1].value if qualities[1] else False),
        "healthy": all(qualities),
        "fault_reason": "" if all(qualities) else "plc_input_quality_bad",
    }


def writePlcOutputs(robotName, outputs):
    """Write the PLC-facing status bits that summarize the runner's decision for this cycle."""
    paths = plcPaths(robotName)
    outputs = dict(outputs or {})
    writeRequiredTagValues(
        [
            paths["available_for_work"],
            paths["active_mission_count"],
            paths["charge_level"],
            paths["system_state"],
            paths["sub_system_state"],
            paths["activity_state"],
            paths["place_id"],
            paths["place_name"],
            paths["active_workflow_number"],
            paths["mission_starved"],
            paths["mission_ready_for_attachment"],
            paths["mission_needs_finalized"],
            paths["request_received"],
            paths["request_success"],
            paths["request_robot_not_ready"],
            paths["fleet_fault"],
            paths["plc_comm_fault"],
            paths["control_healthy"],
            paths["request_conflict"],
            paths["request_invalid"],
        ],
        [
            toBool(outputs.get("available_for_work")),
            int(outputs.get("active_mission_count") or 0),
            float(outputs.get("charge_level") or 0.0),
            str(outputs.get("system_state") or ""),
            str(outputs.get("sub_system_state") or ""),
            str(outputs.get("activity_state") or ""),
            str(outputs.get("place_id") or ""),
            str(outputs.get("place_name") or ""),
            normalizeWorkflowNumber(outputs.get("active_workflow_number")) or 0,
            toBool(outputs.get("mission_starved")),
            toBool(outputs.get("mission_ready_for_attachment")),
            toBool(outputs.get("mission_needs_finalized")),
            toBool(outputs.get("request_received")),
            toBool(outputs.get("request_success")),
            toBool(outputs.get("request_robot_not_ready")),
            toBool(outputs.get("fleet_fault")),
            toBool(outputs.get("plc_comm_fault")),
            toBool(outputs.get("control_healthy", True)),
            toBool(outputs.get("request_conflict")),
            toBool(outputs.get("request_invalid")),
        ],
        labels=["MainController PLC output"] * 20
    )


def writePlcHealthOutputs(robotName, fleetFault=False, plcCommFault=False, controlHealthy=True):
    """Update only the health subset when the loop skips normal PLC evaluation."""
    paths = plcPaths(robotName)
    writeRequiredTagValues(
        [
            paths["fleet_fault"],
            paths["plc_comm_fault"],
            paths["control_healthy"],
        ],
        [
            toBool(fleetFault),
            toBool(plcCommFault),
            toBool(controlHealthy),
        ],
        labels=["MainController PLC health output"] * 3
    )
