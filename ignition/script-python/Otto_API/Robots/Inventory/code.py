from Otto_API.Robots.SyncHelpers import buildInvalidRobotSyncWrites
from Otto_API.Robots.SyncHelpers import readRobotInventoryMetadata


def readRobotInventory(robotsBasePath):
    inventory = readRobotInventoryMetadata(robotsBasePath)
    return (
        inventory["browse_results"],
        inventory["robot_path_by_id"],
        inventory["invalid_robot_rows"],
        inventory["read_plan"],
    )


def collectInvalidRobotWrites(invalidRobotRows, logger):
    invalidated = []
    for invalidRow in list(invalidRobotRows or []):
        robotPath = invalidRow["robot_path"]
        reason = invalidRow["reason"]
        logger.warn("Invalid robot ID for {} - {}".format(robotPath, reason))
        try:
            invalidated.extend(list(buildInvalidRobotSyncWrites(robotPath)))
        except Exception as exc:
            logger.warn(
                "Failed to invalidate sync state for {} - {}".format(
                    robotPath,
                    str(exc)
                )
            )
    return invalidated
