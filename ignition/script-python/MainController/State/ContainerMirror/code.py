from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import readTagValues
from Otto_API.Common.TagHelpers import tagExists
from Otto_API.Common.TagHelpers import writeRequiredTagValues

from MainController.State.Paths import plcContainersPath


def _log():
    return system.util.getLogger("MainController.State.ContainerMirror")


def _normalizedKey(value):
    return str(value or "").strip()


def _udtInstanceRows(basePath):
    rows = []
    for browseResult in list(browseTagResults(basePath) or []):
        if str(browseResult.get("tagType") or "") != "UdtInstance":
            continue
        rowPath = str(browseResult.get("fullPath") or "").strip()
        if not rowPath:
            continue
        rows.append({
            "name": str(browseResult.get("name") or ""),
            "path": rowPath,
        })
    return rows


def _containerLocationMap():
    locationMap = {}
    containersBasePath = getFleetContainersPath()
    if not tagExists(containersBasePath):
        return locationMap

    containerRows = _udtInstanceRows(containersBasePath)
    readPaths = []
    for row in containerRows:
        containerPath = row["path"]
        readPaths.extend([
            containerPath + "/ID",
            containerPath + "/Place",
            containerPath + "/Robot",
        ])
    readResults = readTagValues(readPaths) if readPaths else []

    for index, row in enumerate(containerRows):
        containerPath = row["path"]
        offset = index * 3
        containerId = _normalizedKey(
            readResults[offset].value if offset < len(readResults) and readResults[offset].quality.isGood() else ""
        )
        if not containerId:
            continue

        seenKeys = set()
        for valueIndex in [offset + 1, offset + 2]:
            locationKey = _normalizedKey(
                readResults[valueIndex].value
                if valueIndex < len(readResults) and readResults[valueIndex].quality.isGood()
                else ""
            )
            if not locationKey or locationKey in seenKeys:
                continue
            seenKeys.add(locationKey)
            locationMap.setdefault(locationKey, []).append(containerId)

    return locationMap


def mirrorPlcContainerOccupancy():
    """
    Mirror Fleet/Containers occupancy into manually provisioned PLC/Containers rows.
    """
    logger = _log()
    plcContainersBasePath = plcContainersPath()

    try:
        if not tagExists(plcContainersBasePath):
            return buildOperationResult(
                True,
                "info",
                "No PLC container rows configured",
                data={"rows": [], "writes": []},
                rows=[],
                writes=[],
            )

        plcRows = _udtInstanceRows(plcContainersBasePath)
        if not plcRows:
            return buildOperationResult(
                True,
                "info",
                "No PLC container rows configured",
                data={"rows": [], "writes": []},
                rows=[],
                writes=[],
            )

        locationMap = _containerLocationMap()
        mirroredRows = []
        writePaths = []
        writeValues = []
        locationReadResults = readTagValues(
            [row["path"] + "/LocationID" for row in plcRows]
        ) if plcRows else []

        for index, row in enumerate(plcRows):
            rowPath = row["path"]
            locationId = _normalizedKey(
                locationReadResults[index].value
                if index < len(locationReadResults) and locationReadResults[index].quality.isGood()
                else ""
            )
            matchedContainerIds = list(locationMap.get(locationId) or []) if locationId else []
            present = bool(locationId and matchedContainerIds)
            containerId = matchedContainerIds[0] if present else ""

            if len(matchedContainerIds) > 1:
                logger.warn(
                    "PLC container mirror row [{}] matched multiple containers at [{}]: {}".format(
                        row["name"] or rowPath,
                        locationId,
                        ", ".join(matchedContainerIds),
                    )
                )

            mirroredRows.append({
                "row_name": row["name"],
                "row_path": rowPath,
                "location_id": locationId,
                "present": present,
                "container_id": containerId,
                "matched_container_ids": matchedContainerIds,
            })
            writePaths.extend([
                rowPath + "/Present",
                rowPath + "/ContainerID",
            ])
            writeValues.extend([
                present,
                containerId,
            ])

        if writePaths:
            writeRequiredTagValues(
                writePaths,
                writeValues,
                labels=["MainController PLC container mirror"] * len(writePaths),
            )

        return buildOperationResult(
            True,
            "info",
            "Mirrored PLC container occupancy for {} row(s)".format(len(mirroredRows)),
            data={
                "rows": mirroredRows,
                "writes": list(zip(writePaths, writeValues)),
            },
            rows=mirroredRows,
            writes=list(zip(writePaths, writeValues)),
        )
    except Exception as e:
        logger.error("PLC container occupancy mirror failed: {}".format(str(e)))
        return buildOperationResult(
            False,
            "error",
            "PLC container occupancy mirror failed: {}".format(str(e)),
            data={"rows": [], "writes": []},
            rows=[],
            writes=[],
        )
