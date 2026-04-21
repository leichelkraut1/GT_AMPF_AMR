from Otto_API.Common.TagHelpers import getApiBaseUrl
from Otto_API.Common.TagHelpers import getFleetMapsPath
from Otto_API.Common.TagHelpers import getSystemLastResponsePath
from Otto_API.Common.TagHelpers import readRequiredTagValue
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.SyncHelpers import fetchListResource
from Otto_API.Common.TagHelpers import writeTagValueAsync
from Otto_API.Workflows.Apply import applyWorkflowSync


def _log():
    return system.util.getLogger("Otto_API.Workflows.Get")


def updateWorkflows():
    """
    Get workflow templates from OTTO and sync Fleet/Workflows.
    """
    baseUrl = getApiBaseUrl() + "/maps/mission_templates/?offset=0&map="
    mapUuid = readRequiredTagValue(getFleetMapsPath() + "/ActiveMapID", "Active map ID")
    url = baseUrl + str(mapUuid)
    responseTag = getSystemLastResponsePath()
    ottoLogger = _log()

    ottoLogger.info("Otto API - Updating /Workflows/")
    try:
        response, data, errorResult = fetchListResource(
            url,
            ottoLogger,
            "Workflows",
            responseWriter=lambda responseText: writeTagValueAsync(responseTag, responseText),
            parseErrorLabel="Workflow"
        )
        if errorResult is not None:
            return errorResult

        return applyWorkflowSync(data, ottoLogger)

    except Exception as e:
        ottoLogger.error("Otto API - Workflows tag update failed: {}".format(str(e)))
        return buildSyncResult(False, "error", "Workflow tag update failed: {}".format(str(e)))
