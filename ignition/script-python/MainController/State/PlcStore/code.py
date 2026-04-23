from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import writeRequiredTagValues

from MainController.State.Coerce import toBool
from MainController.State.Paths import plcRobotPaths
from MainController.WorkflowConfig import normalizeWorkflowNumber


def _toInt(value):
    return int(value or 0)


def _toFloat(value):
    return float(value or 0.0)


def _toText(value):
    return str(value or "")


def _toWorkflowNumber(value):
    return normalizeWorkflowNumber(value) or 0


PLC_ROBOT_OUTPUT_SPECS = [
    ("available_for_work", "available_for_work", toBool),
    ("active_mission_count", "active_mission_count", _toInt),
    ("charge_level", "charge_level", _toFloat),
    ("system_state", "system_state", _toText),
    ("sub_system_state", "sub_system_state", _toText),
    ("activity_state", "activity_state", _toText),
    ("place_id", "place_id", _toText),
    ("place_name", "place_name", _toText),
    ("container_present", "container_present", toBool),
    ("container_id", "container_id", _toText),
    ("active_workflow_number", "active_workflow_number", _toWorkflowNumber),
    ("mission_starved", "mission_starved", toBool),
    ("mission_ready_for_attachment", "mission_ready_for_attachment", toBool),
    ("mission_needs_finalized", "mission_needs_finalized", toBool),
    ("request_received", "request_received", toBool),
    ("request_success", "request_success", toBool),
    ("request_robot_not_ready", "request_robot_not_ready", toBool),
    ("fleet_fault", "fleet_fault", toBool),
    ("plc_comm_fault", "plc_comm_fault", toBool),
    ("control_healthy", "control_healthy", toBool),
    ("request_conflict", "request_conflict", toBool),
    ("request_invalid", "request_invalid", toBool),
]


def _buildRobotWritePathsAndValues(paths, outputs):
    """Translate logical PLC output keys into concrete PLC tag paths using the output spec table."""
    writePaths = []
    writeValues = []
    for outputKey, pathKey, coercer in list(PLC_ROBOT_OUTPUT_SPECS):
        writePaths.append(paths[pathKey])
        writeValues.append(coercer(outputs.get(outputKey)))
    return writePaths, writeValues


def readPlcInputs(plcTagName=None, faultReason=None):
    """Read PLC demand and handshake inputs."""
    if not plcTagName:
        return {
            "requested_workflow_number": 0,
            "finalize_ok": False,
            "healthy": False,
            "fault_reason": str(faultReason or "plc_robot_mapping_missing"),
        }

    paths = plcRobotPaths(plcTagName)
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
        "fault_reason": "" if all(qualities) else str(faultReason or "plc_input_quality_bad"),
    }


def writePlcOutputs(plcTagName, outputs):
    """Write the PLC-facing status bits that summarize the runner's decision for this cycle."""
    if not plcTagName:
        raise ValueError("PLC robot mapping missing")

    paths = plcRobotPaths(plcTagName)
    outputs = dict(outputs or {})
    if "control_healthy" not in outputs:
        outputs["control_healthy"] = True
    writePaths, writeValues = _buildRobotWritePathsAndValues(paths, outputs)
    writeRequiredTagValues(
        writePaths,
        writeValues,
        labels=["MainController PLC output"] * len(writePaths)
    )


def writePlcHealthOutputs(plcTagName, fleetFault=False, plcCommFault=False, controlHealthy=True):
    """Update only the health subset when the loop skips normal PLC evaluation."""
    if not plcTagName:
        raise ValueError("PLC robot mapping missing")

    paths = plcRobotPaths(plcTagName)
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
