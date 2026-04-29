from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import readRequiredTagValue
from Otto_API.Common.TagPaths import getFleetMapsPath
from Otto_API.Models.Results import OperationalResult
from Otto_API.TagSync.Workflows import applyWorkflowSync
from Otto_API.WebAPI.Workflows import fetchWorkflows


def _log():
    return system.util.getLogger("Otto_API.Services.Workflows")


def updateWorkflows():
    """
    Fetch OTTO workflow templates and sync Fleet/Workflows.
    """
    mapUuid = readRequiredTagValue(getFleetMapsPath() + "/ActiveMapID", "Active map ID")
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Workflows/")
    try:
        fetchResult = fetchWorkflows(getApiBaseUrl(), str(mapUuid))
        if not fetchResult.ok:
            return fetchResult.toDict()

        return applyWorkflowSync(fetchResult.records, ottoLogger).toDict()

    except Exception as e:
        ottoLogger.error("Otto API - Workflows tag update failed: {}".format(str(e)))
        return OperationalResult(
            False,
            "error",
            "Workflow tag update failed: {}".format(str(e)),
        ).toDict()
