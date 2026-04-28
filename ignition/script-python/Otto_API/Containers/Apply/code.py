from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagPaths import getFleetContainersPath
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.SyncHelpers import writeObservedTagDict
from Otto_API.Containers.Normalize import normalizeContainerRecord
from Otto_API.Containers.Occupancy import recomputeFleetOccupancy


def applyContainerSync(containerRecords, logger):
    """
    Apply container records to Fleet/Containers and remove stale instances.
    """
    basePath = getFleetContainersPath()
    ensureFolder(basePath)

    apiContainers = []
    writes = []

    for containerRecord in list(containerRecords or []):
        normalizedContainer = normalizeContainerRecord(containerRecord)
        if normalizedContainer is None:
            continue

        instanceName = normalizedContainer["instance_name"]
        apiContainers.append(instanceName)
        instancePath = basePath + "/" + instanceName

        if not tagExists(instancePath):
            ensureUdtInstancePath(instancePath, "api_Container")
            logger.info("Otto API - Created new container tag instance: " + instanceName)

        tagDict = {}
        for suffix, value in normalizedContainer["tag_values"].items():
            tagDict[instancePath + suffix] = value

        writeObservedTagDict(tagDict, "Otto_API.Containers.Apply container sync", logger)
        writes.extend(tagDict.items())

    cleanupStaleUdtInstances(
        basePath,
        apiContainers,
        logger,
        "Otto API - Removed stale container tag instance: ",
        cleanupWarnPrefix="Otto API - Container cleanup skipped due to error: ",
    )

    occupancyResult = recomputeFleetOccupancy(logger)
    finalOk = bool(occupancyResult.get("ok", False))
    finalLevel = "info" if finalOk else str(occupancyResult.get("level") or "warn")
    finalMessage = "Containers updated for {} instance(s)".format(len(apiContainers))
    if not finalOk:
        finalMessage = "{}; {}".format(finalMessage, occupancyResult.get("message") or "occupancy degraded")

    return buildSyncResult(
        finalOk,
        finalLevel,
        finalMessage,
        records=containerRecords,
        writes=writes + list(occupancyResult.get("writes") or []),
        occupancy=occupancyResult,
    )
