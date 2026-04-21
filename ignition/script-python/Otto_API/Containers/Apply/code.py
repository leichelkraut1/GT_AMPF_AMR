from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureUdtInstancePath
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.SyncHelpers import writeObservedTagDict
from Otto_API.Containers.Normalize import normalizeContainerRecord


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

        writeObservedTagDict(tagDict, "Otto_API.Containers.Get container sync", logger)
        writes.extend(tagDict.items())

    cleanupStaleUdtInstances(
        basePath,
        apiContainers,
        logger,
        "Otto API - Removed stale container tag instance: ",
        cleanupWarnPrefix="Otto API - Container cleanup skipped due to error: ",
    )

    return buildSyncResult(
        True,
        "info",
        "Containers updated for {} instance(s)".format(len(apiContainers)),
        records=containerRecords,
        writes=writes
    )
