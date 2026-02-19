# --- Demo Configuration ---

# --- CONSTANTS ---
WORKFLOW_IDS = [1, 2, 3, 4]   # WF1..WF4
ROBOT_IDS    = [1, 2, 3, 4]      # RV1..RV5

ROBOT_NAME_BY_ID = {
    1: "AMPF_AMR_RV1",
    2: "AMPF_AMR_RV2",
    3: "AMPF_AMR_RV3",
    4: "AMPF_AMR_RV4",
    5: "AMPF_AMR_RV5"
}

TRIGGER_BASE = "[Otto_FleetManager]Triggers/Missions/Create"
ROBOT_BASE   = "[Otto_FleetManager]Robots"

ATTACH_ENABLE_BASE = "[Otto_FleetManager]DemoMode/AttachmentEnabled"
ATTACH_PHASE_BASE = "[Otto_FleetManager]DemoMode/AttachmentPhase"


def _getNextEnabledWorkflow():
    indexPath = "[Otto_FleetManager]DemoMode/NextWorkflowIndex"
    enableBase = "[Otto_FleetManager]DemoMode/WorkflowEnabled"

    current = system.tag.readBlocking([indexPath])[0].value
    if current is None or current < 1:
        current = 1

    maxIndex = len(WORKFLOW_IDS)


    for offset in range(maxIndex):
        wfId = current + offset
        if wfId > maxIndex:
            wfId -= maxIndex

        enablePath = enableBase + "/WF" + str(wfId)
        enabled = system.tag.readBlocking([enablePath])[0].value

        if enabled is True:

            nextIndex = wfId + 1
            if nextIndex > maxIndex:
                nextIndex = 1

            system.tag.writeBlocking([indexPath], [nextIndex])
            return wfId


    return None


def _getNextEligibleRobot():
    logger = system.util.getLogger("Otto_DemoMode")

    indexPath = "[Otto_FleetManager]DemoMode/NextRobotIndex"

    qv = system.tag.readBlocking([indexPath])[0]
    current = qv.value

    logger.debug("Robot cursor raw value = {0}, quality = {1}".format(
        current, qv.quality
    ))

    if current is None or current < 1:
        current = 1

    maxIndex = len(ROBOT_IDS)

    for offset in range(maxIndex):
        rvId = current + offset
        if rvId > maxIndex:
            rvId -= maxIndex

        robotName = ROBOT_NAME_BY_ID.get(rvId)
        if robotName is None:
            continue

        # AvailableForWork
        availPath = ROBOT_BASE + "/" + robotName + "/AvailableForWork"
        avail = system.tag.readBlocking([availPath])[0].value

        if not avail:
            continue

        # Existing trigger check
        if _robotHasLatchedTrigger(rvId):
            continue

        # Advance cursor past this robot
        nextIndex = rvId + 1
        if nextIndex > maxIndex:
            nextIndex = 1

        system.tag.writeBlocking([indexPath], [nextIndex])
        return rvId

    logger.debug("No eligible robots found after full scan")
    return None


def _buildTriggerPath(wfId, rvId):
    return (
        TRIGGER_BASE
        + "/Create_WF"
        + str(wfId)
        + "_RV"
        + str(rvId)
    )


def _robotHasLatchedTrigger(rvId):
    paths = []

    for wfId in WORKFLOW_IDS:
        paths.append(_buildTriggerPath(wfId, rvId))

    results = system.tag.readBlocking(paths)

    for r in results:
        if r.value:
            return True

    return False


def _extractRobotIdFromMissionName(missionName):
    for rvId in ROBOT_IDS:
        token = "RV" + str(rvId)
        if token in missionName:
            return rvId
    return None


def _isExtendedDemoEnabled(rvId):
    logger = system.util.getLogger("Otto_DemoMode")

    token = "RV" + str(rvId)
    enablePath = ATTACH_ENABLE_BASE + "/" + token

    try:
        qv = system.tag.readBlocking([enablePath])[0]
        logger.info(
            "Extended enable read: path={0} value={1} quality={2}".format(
                enablePath, qv.value, qv.quality
            )
        )
        return (qv.value is True)

    except Exception as e:
        logger.error(
            "_isExtendedDemoEnabled() failed for path={0}: {1}".format(
                enablePath, e
            )
        )
        return False


def _cleanupAttachmentAcks():
    logger = system.util.getLogger("Otto_DemoMode")

    try:
        tokens = system.tag.browse(ATTACH_PHASE_BASE).getResults()

        for t in tokens:
            tokenPath = t["fullPath"].toString()

            donePath = tokenPath + "/DemoComplete"
            ackPath = tokenPath + "/DemoCompleteAck"

            qvs = system.tag.readBlocking([donePath, ackPath])
            doneVal = qvs[0].value
            ackVal = qvs[1].value

            if ackVal is True and doneVal is not True:
                system.tag.writeBlocking([ackPath], [False])

    except Exception as e:
        logger.error("_cleanupAttachmentAcks() failed: {0}".format(e))


def _handleStarvedPhase(rvId):
    """
    Pre-finalize attachment phase gate for a robot.

    Returns:
        True  -> finalize is allowed this cycle
        False -> block finalize this cycle
    """
    logger = system.util.getLogger("Otto_DemoMode")

    token = "RV" + str(rvId)
    base = ATTACH_PHASE_BASE + "/" + token

    runPath   = base + "/RunDemo"
    runningPath = base + "/DemoRunning"
    donePath  = base + "/DemoComplete"
    ackPath   = base + "/DemoCompleteAck"

    qvs = system.tag.readBlocking([runPath, runningPath, donePath, ackPath])
    runVal     = qvs[0].value
    runningVal = qvs[1].value
    doneVal    = qvs[2].value
    ackVal     = qvs[3].value

    # --- If complete, ack and allow finalize ---
    if doneVal is True:
        if ackVal is not True:
            system.tag.writeBlocking([ackPath], [True])
            logger.info("Attachment demo complete for {0}; set DemoCompleteAck".format(token))
        return True


    # --- If running, do nothing (avoid retriggering) ---
    if runningVal is True:
        return False

    # --- Not running and not complete: request a start once ---
    if runVal is not True:
        system.tag.writeBlocking([runPath], [True])
        logger.info("STARVED detected for {0}; latched RunDemo".format(token))

    return False



def finalizeStarvedMissions():
    """
    Scans Active mission UDT instances and responds to STARVED missions.

    Behavior:
    - Robots WITHOUT extended demo enabled: immediately latch finalize_RV# Test
    - Robots WITH extended demo enabled:
        - Run attachment phase handshake (RunDemo/DemoRunning/DemoComplete/DemoCompleteAck)
        - Only latch finalize_RV# after DemoComplete is observed (and Ack is set)
    - Also relies on _cleanupAttachmentAcks() to keep the handshake bits tidy
    """

    logger = system.util.getLogger("Otto_DemoMode")

    activeBase = "[Otto_FleetManager]Missions/Active"
    finalizeBase = "[Otto_FleetManager]Triggers/Missions/Finalize"

    try:
        # --- Keep attachment handshake clean each cycle ---
        _cleanupAttachmentAcks()

        missions = system.tag.browse(activeBase).getResults()

        for m in missions:
            missionPath = m["fullPath"].toString()
            missionName = m["name"]

            # --- Read mission_status ---
            statusPath = missionPath + "/mission_status"
            statusQV = system.tag.readBlocking([statusPath])[0]

            if statusQV.value != "STARVED":
                continue

            logger.info("Starved Mission Detected: '{0}'".format(missionName))
            rvId = _extractRobotIdFromMissionName(missionName)
            logger.info("Extracted rvId={0} from missionName='{1}'".format(rvId, missionName))
            enabled = False
            if rvId is not None:
    			enabled = _isExtendedDemoEnabled(rvId)
			logger.info("Extended enabled check: rvId={0}, enabled={1}".format(rvId, enabled))

            # --- Resolve robot from mission name ---
            rvId = _extractRobotIdFromMissionName(missionName)


            if rvId is None:
                logger.warn("STARVED mission has no RV mapping: {0}".format(missionName))
                continue

            # --- If extended demo enabled for this robot, gate finalize ---
            if _isExtendedDemoEnabled(rvId):
                if not _handleStarvedPhase(rvId):
                    continue  # block finalize until attachment phase completes


            # --- Latch finalize trigger for this RV ---
            finalizePath = finalizeBase + "/finalize_RV" + str(rvId)
            current = system.tag.readBlocking([finalizePath])[0].value

            if current:
                continue

            system.tag.writeBlocking([finalizePath], [True])
            logger.info(
                "Finalized STARVED mission '{0}' via {1}".format(
                    missionName, finalizePath
                )
            )

    except Exception as e:
        logger.error("finalizeStarvedMissions() failed: {0}".format(e))


# ---------------------------------------------------------------------------
# MAIN: assignment loop (one mission per cycle)
# ---------------------------------------------------------------------------

def run():
    logger = system.util.getLogger("Otto_DemoMode")

    try:
        # --- Select next robot (round-robin) ---
        rvId = _getNextEligibleRobot()
        if rvId is None:
            logger.debug("DemoMode: no eligible robots")
            return

        # --- Select next workflow (round-robin + enabled) ---
        wfId = _getNextEnabledWorkflow()
        if wfId is None:
            logger.debug("DemoMode: no enabled workflows")
            return

        triggerPath = _buildTriggerPath(wfId, rvId)
        system.tag.writeBlocking([triggerPath], [True])

        logger.info(
            "DemoMode assigned WF{0} to RV{1}".format(wfId, rvId)
        )

    except Exception as e:
        logger.error("DemoMode.run() failed: {0}".format(e))
