def cleanupAttachmentAcks(attachmentRows):
    """
    Return the attachment-phase rows whose DemoCompleteAck should be reset.
    """
    resetRows = []
    for row in list(attachmentRows or []):
        if row.get("ack") is True and row.get("done") is not True:
            resetRows.append(row)
    return resetRows


def buildMissionControlFlags(missionStarved):
    """
    Build the derived mission-phase flags consumed by MainControl.

    For now, attachment-readiness is intentionally the same as STARVED. Keeping
    this mapping in one small helper makes it obvious where the behavior should
    grow later if the two concepts diverge.
    """
    missionStarved = bool(missionStarved)
    return {
        "mission_starved": missionStarved,
        "ready_for_attachment": missionStarved,
    }


def deriveMissionAttachmentState(missionRecord):
    """
    Derive attachment-facing mission state from a mission row.

    This intentionally returns a small structured object instead of just a
    boolean so we have a clean place to expand later when we start resolving
    attachment place/endpoint details from richer mission/task metadata.
    """
    status = str((missionRecord or {}).get("mission_status") or "").strip().upper()
    controlFlags = buildMissionControlFlags(status == "STARVED")

    return {
        "mission_starved": controlFlags["mission_starved"],
        "ready_for_attachment": controlFlags["ready_for_attachment"],
        "attachment_mission_name": (
            (missionRecord or {}).get("name") if controlFlags["ready_for_attachment"] else ""
        ),
        "attachment_place": None,
        "state": (
            "ready_for_attachment"
            if controlFlags["ready_for_attachment"]
            else "not_ready_for_attachment"
        ),
        "reason": (
            "mission_starved"
            if controlFlags["mission_starved"]
            else "mission_not_starved"
        ),
    }
