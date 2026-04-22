from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagHelpers import browseTagResults
from Otto_API.Common.TagHelpers import getFleetContainersPath
from Otto_API.Common.TagHelpers import readOptionalTagValue
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

    for row in _udtInstanceRows(containersBasePath):
        containerPath = row["path"]
        containerId = _normalizedKey(
            readOptionalTagValue(containerPath + "/ID", "", allowEmptyString=True)
        )
        if not containerId:
            continue

        seenKeys = set()
        for suffix in ["/Place", "/Robot"]:
            locationKey = _normalizedKey(
                readOptionalTagValue(containerPath + suffix, "", allowEmptyString=True)
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

        for row in plcRows:
            rowPath = row["path"]
            locationId = _normalizedKey(
                readOptionalTagValue(rowPath + "/LocationID", "", allowEmptyString=True)
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
