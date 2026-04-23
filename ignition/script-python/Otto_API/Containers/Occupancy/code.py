from Otto_API.Common.TagIO import browseTagResults
from Otto_API.Common.TagIO import isWriteResultGood
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagPaths import getFleetContainersPath
from Otto_API.Common.TagPaths import getFleetPlacesPath
from Otto_API.Common.TagPaths import getFleetRobotsPath
from Otto_API.Common.SyncHelpers import buildSyncResult


def _udtRows(basePath):
    """Browse a folder and return only UDT instance rows with stable path strings."""
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
    """Read Fleet row IDs once so container references can resolve to Fleet tag paths."""
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
    """Read one container row's container/place/robot references from the batched read results."""
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
    """Stamp one Fleet place or Fleet robot with the latest container that points at it."""
    if not targetId or targetId not in pathMap:
        return

    targetPath = pathMap[targetId]
    occupancyByPath[targetPath] = {
        "present": True,
        "container_id": containerId,
    }


def _buildOccupancyWriteMap(logger):
    """Reduce Fleet/Containers into one occupancy state per Fleet place and Fleet robot."""
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
    """Project raw container location refs onto Fleet place/robot occupancy tags."""
    writeMap = _buildOccupancyWriteMap(logger)
    if not writeMap:
        return buildSyncResult(
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
            "Otto_API.Containers.Occupancy Fleet occupancy write failed for {}: {}".format(
                writePaths[index],
                writeResult,
            )
        )

    if failedPaths:
        return buildSyncResult(
            False,
            "warn",
            "Fleet occupancy recompute degraded for {} tag(s)".format(len(failedPaths)),
            writes=list(zip(writePaths, writeValues)),
            failed_paths=failedPaths,
        )

    return buildSyncResult(
        True,
        "info",
        "Fleet occupancy recomputed for {} location(s)".format(len(writeMap)),
        writes=list(zip(writePaths, writeValues)),
    )
