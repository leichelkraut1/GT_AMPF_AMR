def cleanupAttachmentAcks(attachmentRows):
    """
    Return the attachment-phase rows whose DemoCompleteAck should be reset.
    """
    resetRows = []
    for row in list(attachmentRows or []):
        if row.get("ack") is True and row.get("done") is not True:
            resetRows.append(row)
    return resetRows


def deriveMissionAttachmentState(missionRecord):
    """
    Derive attachment-facing mission state from a mission row.

    This intentionally returns a small structured object instead of just a
    boolean so we have a clean place to expand later when we start resolving
    attachment place/endpoint details from richer mission/task metadata.
    """
    status = str((missionRecord or {}).get("mission_status") or "").strip().upper()
    readyForAttachment = status == "STARVED"

    return {
        "ready_for_attachment": readyForAttachment,
        "attachment_mission_id": (missionRecord or {}).get("id") if readyForAttachment else "",
        "attachment_mission_name": (missionRecord or {}).get("name") if readyForAttachment else "",
        "attachment_place": None,
        "state": "ready_for_attachment" if readyForAttachment else "not_ready_for_attachment",
        "reason": "mission_starved" if readyForAttachment else "mission_not_starved",
    }
