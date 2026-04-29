from Otto_API.Common.OperationHelpers import logOperationResult
from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import getOttoOperationsUrl
from Otto_API.Models.Results import OperationalResult
from Otto_API.TagSync.Containers import applyContainerSync
from Otto_API.TagSync.Containers import findAllContainerIds
from Otto_API.TagSync.Containers import findContainerIdsAtPlace
from Otto_API.TagSync.Containers import findContainerIdsWithoutLocation
from Otto_API.TagSync.Containers import isVerboseCleanupLoggingEnabled
from Otto_API.TagSync.Containers import readCreateContainerBaseFields
from Otto_API.WebAPI.Containers import fetchContainers
from Otto_API.WebAPI.Containers import postCreateContainerAtPlace
from Otto_API.WebAPI.Containers import postCreateContainerAtRobot
from Otto_API.WebAPI.Containers import postDeleteContainer
from Otto_API.WebAPI.Containers import postUpdateContainerPlace
from Otto_API.WebAPI.Containers import postUpdateContainerRobot


def _log():
    return system.util.getLogger("Otto_API.Services.Containers")


def _writeResponseAndLogResult(result, logger):
    return logOperationResult(result.toDict(), logger)


def _containerResult(
    ok,
    level,
    message,
    containerId=None,
    placeId=None,
    robotId=None,
    responseText=None,
    payload=None,
    extraData=None,
):
    dataFields = {
        "container_id": containerId,
        "place_id": placeId,
        "robot_id": robotId,
        "response_text": responseText,
        "payload": payload,
    }
    dataFields.update(dict(extraData or {}))
    return OperationalResult(ok, level, message, dataFields=dataFields)


def _runCreateFromTagPath(containerTagPath, targetLabel, targetId, createFunc):
    operationsUrl = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info("Creating container from tag path [{}] at {} [{}]".format(containerTagPath, targetLabel, targetId))

    try:
        containerFields = readCreateContainerBaseFields(containerTagPath)
    except Exception as e:
        msg = "Error reading container fields from [{}]: {}".format(containerTagPath, str(e))
        ottoLogger.error(msg)
        return _containerResult(False, "error", msg).toDict()

    result = createFunc(containerFields, targetId, operationsUrl)
    return _writeResponseAndLogResult(result, ottoLogger)


def _runDirectOperation(logMessage, operationFunc, *args):
    operationsUrl = getOttoOperationsUrl()
    ottoLogger = _log()

    ottoLogger.info(logMessage)
    result = operationFunc(*(args + (operationsUrl,)))
    return _writeResponseAndLogResult(result, ottoLogger)


def _deleteMatchedContainerIds(
    matchedContainerIds,
    operationsUrl,
    noMatchMessage,
    successMessageBuilder,
    partialMessageBuilder,
    resultFields=None,
):
    resultFields = dict(resultFields or {})

    if not matchedContainerIds:
        return _containerResult(
            False,
            "warn",
            noMatchMessage,
            containerId=resultFields.get("containerId"),
            placeId=resultFields.get("placeId"),
            robotId=resultFields.get("robotId"),
            extraData={
                "matched_container_ids": [],
                "deleted_container_ids": [],
                "delete_results": [],
            },
        )

    deleteResults = []
    deletedContainerIds = []
    allSucceeded = True
    for containerId in matchedContainerIds:
        result = postDeleteContainer(containerId, operationsUrl)
        deleteResults.append(result.toDict())
        if result.ok:
            deletedContainerIds.append(containerId)
        else:
            allSucceeded = False

    if allSucceeded:
        level = "info"
        message = successMessageBuilder(deletedContainerIds, matchedContainerIds)
    else:
        level = "warn"
        message = partialMessageBuilder(deletedContainerIds, matchedContainerIds)

    return _containerResult(
        allSucceeded,
        level,
        message,
        containerId=resultFields.get("containerId"),
        placeId=resultFields.get("placeId"),
        robotId=resultFields.get("robotId"),
        extraData={
            "matched_container_ids": matchedContainerIds,
            "deleted_container_ids": deletedContainerIds,
            "delete_results": deleteResults,
        },
    )


def updateContainers():
    """
    Fetch OTTO container records and sync Fleet/Containers.
    """
    ottoLogger = _log()

    try:
        fetchResult = fetchContainers(getApiBaseUrl())
        if not fetchResult.ok:
            return fetchResult.toDict()

        return applyContainerSync(fetchResult.records, ottoLogger)

    except Exception as e:
        ottoLogger.error("Otto API - Containers tag update failed: {}".format(str(e)))
        return buildSyncResult(False, "error", "Containers tag update failed: {}".format(str(e)))


def deleteContainersAtPlaceFromInputs(placeId, operationsUrl):
    """
    Delete every container currently assigned to the provided place id.
    """
    if not placeId:
        return _containerResult(False, "warn", "No place id supplied for delete-at-place")

    matchedContainerIds = findContainerIdsAtPlace(placeId)

    return _deleteMatchedContainerIds(
        matchedContainerIds,
        operationsUrl,
        "No containers found at place [{}]".format(placeId),
        lambda deletedIds, _matchedIds: "Deleted {} container(s) at place [{}]".format(
            len(deletedIds),
            placeId
        ),
        lambda deletedIds, matchedIds: "Deleted {} of {} container(s) at place [{}]".format(
            len(deletedIds),
            len(matchedIds),
            placeId
        ),
        resultFields={"placeId": placeId},
    )


def deleteAllContainersFromInputs(operationsUrl):
    """
    Delete every synced container currently present under Fleet/Containers.
    """
    matchedContainerIds = findAllContainerIds()

    return _deleteMatchedContainerIds(
        matchedContainerIds,
        operationsUrl,
        "No containers found to delete",
        lambda deletedIds, _matchedIds: "Deleted {} container(s)".format(len(deletedIds)),
        lambda deletedIds, matchedIds: "Deleted {} of {} container(s)".format(
            len(deletedIds),
            len(matchedIds)
        ),
    )


def deleteContainersWithoutLocationFromInputs(operationsUrl):
    """
    Delete every synced container that has neither a Robot nor Place id.
    """
    matchedContainerIds = findContainerIdsWithoutLocation()

    return _deleteMatchedContainerIds(
        matchedContainerIds,
        operationsUrl,
        "No containers without robot or place were found",
        lambda deletedIds, _matchedIds: "Deleted {} container(s) without robot or place".format(
            len(deletedIds)
        ),
        lambda deletedIds, matchedIds: "Deleted {} of {} container(s) without robot or place".format(
            len(deletedIds),
            len(matchedIds)
        ),
    )


def createContainerAtPlace(containerTagPath, placeId):
    """
    Create one container through the OTTO operations endpoint at an explicit place.
    """
    return _runCreateFromTagPath(
        containerTagPath,
        "place",
        placeId,
        postCreateContainerAtPlace,
    )


def createContainerAtRobot(containerTagPath, robotId):
    """
    Create one container through the OTTO operations endpoint at an explicit robot.
    """
    return _runCreateFromTagPath(
        containerTagPath,
        "robot",
        robotId,
        postCreateContainerAtRobot,
    )


def updateContainerPlaceById(containerId, placeId):
    """
    Update one container's place through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Updating container [{}] place to [{}]".format(containerId, placeId),
        postUpdateContainerPlace,
        containerId,
        placeId,
    )


def updateContainerRobotById(containerId, robotId):
    """
    Update one container's robot through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Updating container [{}] robot to [{}]".format(containerId, robotId),
        postUpdateContainerRobot,
        containerId,
        robotId,
    )


def deleteContainerById(containerId):
    """
    Delete one container through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Deleting container [{}]".format(containerId),
        postDeleteContainer,
        containerId,
    )


def deleteContainersAtPlace(placeId):
    """
    Delete every synced container currently assigned to the provided place id.
    """
    return _runDirectOperation(
        "Deleting containers at place [{}]".format(placeId),
        deleteContainersAtPlaceFromInputs,
        placeId,
    )


def deleteAllContainers():
    """
    Delete every synced container through the OTTO operations endpoint.
    """
    return _runDirectOperation(
        "Deleting all containers",
        deleteAllContainersFromInputs,
    )


def cleanupContainersWithoutLocation():
    """
    Delete every synced container that has neither a Robot nor Place id.
    """
    operationsUrl = getOttoOperationsUrl()
    ottoLogger = _log()
    verboseLogging = isVerboseCleanupLoggingEnabled()

    if verboseLogging:
        ottoLogger.info("Deleting containers without robot or place")

    result = deleteContainersWithoutLocationFromInputs(operationsUrl)

    if result.ok and result.data.get("deleted_container_ids"):
        ottoLogger.info(result.message)
    elif not result.ok and result.level == "warn":
        if verboseLogging or result.data.get("matched_container_ids"):
            ottoLogger.warn(result.message)
    elif result.level == "error":
        ottoLogger.error(result.message)
    return result.toDict()
