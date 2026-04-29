import json
import uuid

from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import browseUdtInstancePaths
from Otto_API.Common.TagIO import isWriteResultGood
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readRequiredTagValues
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagPaths import getFleetContainersPath
from Otto_API.Common.TagPaths import getFleetContainersVerboseCleanupLoggingPath
from Otto_API.Common.TagPaths import getFleetPlacesPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.SyncHelpers import writeObservedTagDict
from Otto_API.Models.Containers import ContainerCreateFields
from Otto_API.Models.Results import RecordSyncResult


def buildContainerCreateId(uuidFactory=None):
    """
    Build a generated container id as a UUID string.
    """
    if uuidFactory is None:
        uuidFactory = uuid.uuid4

    return str(uuidFactory())


def readCreateContainerBaseFields(containerTagPath, uuidFactory=None):
    """
    Read the common createContainer fields from one container UDT instance path.
    """
    requiredFieldSpecs = [
        ("container_type", containerTagPath + "/ContainerType", "Container type"),
        ("empty", containerTagPath + "/Empty", "Container empty"),
    ]
    requiredValues = readRequiredTagValues(
        [path for _, path, _ in requiredFieldSpecs],
        labels=[label for _, _, label in requiredFieldSpecs],
        allowEmptyString=False,
    )

    containerFields = {}
    containerFields["id"] = buildContainerCreateId(uuidFactory)
    for (fieldName, _path, _label), value in zip(requiredFieldSpecs, requiredValues):
        containerFields[fieldName] = value

    optionalFieldSpecs = [
        ("description", containerTagPath + "/Description"),
        ("name", containerTagPath + "/Name"),
    ]
    for fieldName, path in optionalFieldSpecs:
        value = readOptionalTagValue(path, None, allowEmptyString=True)
        if value in [None, ""]:
            continue
        containerFields[fieldName] = value
    return ContainerCreateFields.fromDict(containerFields)


def isVerboseCleanupLoggingEnabled():
    return bool(
        readOptionalTagValue(
            getFleetContainersVerboseCleanupLoggingPath(),
            False,
            allowEmptyString=False
        )
    )


def hasLocationValue(value):
    """
    Return True when a location field is meaningfully populated.
    """
    if value is None:
        return False

    text = str(value).strip()
    if not text:
        return False

    if text.lower() in ["none", "null"]:
        return False

    return True


def findContainerIdsAtPlace(placeId):
    """
    Return synced container ids currently assigned to one place id.
    """
    matchedContainerIds = []
    for instancePath in browseUdtInstancePaths(getFleetContainersPath()):
        if readOptionalTagValue(instancePath + "/Place", None) != placeId:
            continue

        containerId = readOptionalTagValue(instancePath + "/ID", None)
        if containerId:
            matchedContainerIds.append(containerId)
    return matchedContainerIds


def findAllContainerIds():
    """
    Return every synced container id under Fleet/Containers.
    """
    matchedContainerIds = []
    for instancePath in browseUdtInstancePaths(getFleetContainersPath()):
        containerId = readOptionalTagValue(instancePath + "/ID", None)
        if containerId:
            matchedContainerIds.append(containerId)
    return matchedContainerIds


def findContainerIdsWithoutLocation():
    """
    Return synced container ids that have neither a robot nor place id.
    """
    matchedContainerIds = []
    for instancePath in browseUdtInstancePaths(getFleetContainersPath()):
        containerId = readOptionalTagValue(instancePath + "/ID", None)
        placeId = readOptionalTagValue(instancePath + "/Place", None)
        robotId = readOptionalTagValue(instancePath + "/Robot", None)
        if not containerId:
            continue
        if hasLocationValue(placeId) or hasLocationValue(robotId):
            continue
        matchedContainerIds.append(containerId)
    return matchedContainerIds


def normalizeContainerRecord(containerRecord):
    """
    Normalize an OTTO container record for Fleet/Containers sync.
    """
    containerId = containerRecord.get("id")
    if containerId is None or not str(containerId).strip():
        return None

    return {
        "instance_name": str(containerId).strip(),
        "tag_values": {
            "/ID": containerRecord.get("id"),
            "/ContainerType": containerRecord.get("container_type"),
            "/Created": containerRecord.get("created"),
            "/Description": containerRecord.get("description"),
            "/Empty": containerRecord.get("empty"),
            "/Name": containerRecord.get("name"),
            "/Place": containerRecord.get("place"),
            "/ReservedAt": containerRecord.get("reserved_at"),
            "/ReservedBy": containerRecord.get("reserved_by"),
            "/Robot": containerRecord.get("robot"),
            "/State": containerRecord.get("state"),
            "/SystemCreated": containerRecord.get("system_created"),
            "/jsonString": json.dumps(containerRecord),
        }
    }


def _udtRows(basePath):
    """
    Browse a folder and return only UDT instance rows with stable path strings.
    """
    rows = []
    for browseResult in list(browseTagResults(basePath) or []):
        if str(browseResult.get("tagType") or "") != "UdtInstance":
            continue
        rowPath = normalizeTagValue(browseResult.get("fullPath"))
        if not rowPath:
            continue
        rows.append({
            "name": str(browseResult.get("name") or ""),
            "path": rowPath,
        })
    return rows


def _readIdMap(basePath):
    """
    Read Fleet row IDs once so container references can resolve to Fleet tag paths.
    """
    if not tagExists(basePath):
        return {}

    rows = _udtRows(basePath)
    readPaths = [row["path"] + "/ID" for row in rows]
    readResults = readTagValues(readPaths) if readPaths else []
    idMap = {}
    for row, qualifiedValue in zip(rows, readResults):
        if not qualifiedValue.quality.isGood():
            continue
        rowId = normalizeTagValue(qualifiedValue.value)
        if not rowId:
            continue
        idMap[rowId] = row["path"]
    return idMap


def _readContainerRefs(readResults, offset):
    """
    Read one container row's container/place/robot refs from batched results.
    """
    return (
        normalizeTagValue(
            readResults[offset].value
            if offset < len(readResults) and readResults[offset].quality.isGood()
            else ""
        ),
        normalizeTagValue(
            readResults[offset + 1].value
            if offset + 1 < len(readResults) and readResults[offset + 1].quality.isGood()
            else ""
        ),
        normalizeTagValue(
            readResults[offset + 2].value
            if offset + 2 < len(readResults) and readResults[offset + 2].quality.isGood()
            else ""
        ),
    )


def _claimOccupancyTarget(occupancyByPath, targetId, pathMap, containerId):
    """
    Stamp one Fleet place or Fleet robot with the latest container that points at it.
    """
    if not targetId or targetId not in pathMap:
        return

    targetPath = pathMap[targetId]
    occupancyByPath[targetPath] = {
        "present": True,
        "container_id": containerId,
    }


def _buildOccupancyWriteMap(logger):
    """
    Reduce Fleet/Containers into one occupancy state per Fleet place and robot.
    """
    robotsById = _readIdMap(getFleetRobotsPath())
    placesById = _readIdMap(getFleetPlacesPath())
    containerRows = _udtRows(getFleetContainersPath())
    readPaths = []
    for row in containerRows:
        readPaths.extend([
            row["path"] + "/ID",
            row["path"] + "/Place",
            row["path"] + "/Robot",
        ])
    readResults = readTagValues(readPaths) if readPaths else []

    occupancyByPath = {}
    for robotPath in list(robotsById.values()):
        occupancyByPath[robotPath] = {"present": False, "container_id": ""}
    for placePath in list(placesById.values()):
        occupancyByPath[placePath] = {"present": False, "container_id": ""}

    for index, _row in enumerate(containerRows):
        offset = index * 3
        containerId, placeId, robotId = _readContainerRefs(readResults, offset)
        if not containerId:
            continue

        _claimOccupancyTarget(
            occupancyByPath,
            placeId,
            placesById,
            containerId,
        )
        _claimOccupancyTarget(
            occupancyByPath,
            robotId,
            robotsById,
            containerId,
        )

    return occupancyByPath


def recomputeFleetOccupancy(logger):
    """
    Project raw container location refs onto Fleet place/robot occupancy tags.
    """
    writeMap = _buildOccupancyWriteMap(logger)
    if not writeMap:
        return RecordSyncResult(
            True,
            "info",
            "Fleet occupancy recomputed for 0 locations",
            writes=[],
        )

    writePaths = []
    writeValues = []
    for targetPath in sorted(writeMap.keys()):
        occupancy = writeMap[targetPath]
        writePaths.extend([
            targetPath + "/ContainerPresent",
            targetPath + "/ContainerId",
        ])
        writeValues.extend([
            bool(occupancy.get("present")),
            str(occupancy.get("container_id") or ""),
        ])

    writeResults = system.tag.writeBlocking(writePaths, writeValues)
    failedPaths = []
    for index, writeResult in enumerate(list(writeResults or [])):
        if isWriteResultGood(writeResult):
            continue
        failedPaths.append(writePaths[index])
        logger.warn(
            "Otto_API.TagSync.Containers Fleet occupancy write failed for {}: {}".format(
                writePaths[index],
                writeResult,
            )
        )

    if failedPaths:
        return RecordSyncResult(
            False,
            "warn",
            "Fleet occupancy recompute degraded for {} tag(s)".format(len(failedPaths)),
            writes=list(zip(writePaths, writeValues)),
            sharedFields={"failed_paths": failedPaths},
        )

    return RecordSyncResult(
        True,
        "info",
        "Fleet occupancy recomputed for {} location(s)".format(len(writeMap)),
        writes=list(zip(writePaths, writeValues)),
    )


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

        writeObservedTagDict(tagDict, "Otto_API.TagSync.Containers container sync", logger)
        writes.extend(tagDict.items())

    cleanupStaleUdtInstances(
        basePath,
        apiContainers,
        logger,
        "Otto API - Removed stale container tag instance: ",
        cleanupWarnPrefix="Otto API - Container cleanup skipped due to error: ",
    )

    occupancyResult = recomputeFleetOccupancy(logger)
    finalOk = bool(occupancyResult.ok)
    finalLevel = "info" if finalOk else str(occupancyResult.level or "warn")
    finalMessage = "Containers updated for {} instance(s)".format(len(apiContainers))
    if not finalOk:
        finalMessage = "{}; {}".format(
            finalMessage,
            occupancyResult.message or "occupancy degraded"
        )

    return RecordSyncResult(
        finalOk,
        finalLevel,
        finalMessage,
        records=containerRecords,
        writes=writes + list(occupancyResult.writes or []),
        sharedFields={"occupancy": occupancyResult},
    )
