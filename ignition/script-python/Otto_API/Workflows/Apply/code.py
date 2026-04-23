from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagPaths import getFleetWorkflowsPath
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.SyncHelpers import writeObservedTagDict
from Otto_API.Workflows.Normalize import buildWorkflowTagValues


def applyWorkflowSync(templateItems, logger):
    """
    Apply workflow template records to Fleet/Workflows and remove stale instances.
    """
    basePath = getFleetWorkflowsPath()
    apiTemplates = []
    writes = []

    for tmpl in list(templateItems or []):
        instanceName, missionDict = buildWorkflowTagValues(basePath, tmpl)
        if not instanceName:
            continue

        apiTemplates.append(instanceName)
        instancePath = basePath + "/" + instanceName
        exists = tagExists(instancePath)

        if not exists:
            ensureUdtInstancePath(instancePath, "api_Mission")
            logger.info("Otto API - Created Workflow: " + instanceName)

        writeObservedTagDict(missionDict, "Otto_API.Workflows.Get workflow sync", logger)
        writes.extend(missionDict.items())

    cleanupStaleUdtInstances(
        basePath,
        apiTemplates,
        logger,
        "Otto API - Removed stale workflow: ",
        cleanupWarnPrefix="Otto API - Workflow cleanup skipped: ",
    )

    return buildSyncResult(
        True,
        "info",
        "Workflows updated for {} instance(s)".format(len(apiTemplates)),
        records=templateItems,
        writes=writes
    )
