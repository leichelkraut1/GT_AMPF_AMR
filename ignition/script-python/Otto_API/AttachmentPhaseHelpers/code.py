def cleanupAttachmentAcks(attachmentRows):
    """
    Return the attachment-phase rows whose DemoCompleteAck should be reset.
    """
    resetRows = []
    for row in list(attachmentRows or []):
        if row.get("ack") is True and row.get("done") is not True:
            resetRows.append(row)
    return resetRows


def evaluateStarvedPhase(runVal, runningVal, doneVal, ackVal):
    """
    Interpret STARVED attachment-phase state and return desired actions/state.

    The returned object is intended to be reusable outside the old Demo flow:
    - allow_finalize: finalize may proceed this cycle
    - request_run_demo: latch RunDemo this cycle
    - set_complete_ack: latch DemoCompleteAck this cycle
    - in_position: STARVED indicates the vehicle is in position
    - state: normalized state label
    """
    result = {
        "allow_finalize": False,
        "request_run_demo": False,
        "set_complete_ack": False,
        "in_position": True,
        "state": "pending_demo",
    }

    if doneVal is True:
        result["allow_finalize"] = True
        result["state"] = "attachment_complete"
        if ackVal is not True:
            result["set_complete_ack"] = True
        return result

    if runningVal is True:
        result["state"] = "attachment_running"
        return result

    if runVal is not True:
        result["request_run_demo"] = True
        result["state"] = "request_demo_start"
        return result

    result["state"] = "awaiting_demo_progress"
    return result
