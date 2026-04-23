from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import isWriteResultGood
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagPaths import getFleetPlacesPath

from MainController.State.PlcMappingStore import readPlcMappings
from MainController.State.Paths import plcPlaceRowPath


PLC_PLACE_OUTPUT_SPECS = [
    ("container_present", "ContainerPresent", bool),
    ("container_id", "ContainerId", lambda value: str(value or "")),
]


def _log():
    return system.util.getLogger("MainController.State.ContainerMirror")


def _buildPlaceWritePathsAndValues(basePath, outputs):
    """Build one PLC place row's writes from the declarative place output spec."""
    writePaths = []
    writeValues = []
    for outputKey, suffix, coercer in list(PLC_PLACE_OUTPUT_SPECS):
        writePaths.append(basePath + "/" + suffix)
        writeValues.append(coercer(outputs.get(outputKey)))
    return writePaths, writeValues


def _fleetPlaceOccupancyByTagName(placeTagNames):
    """Read the Fleet occupancy fields once for the mapped place tag names."""
    basePath = getFleetPlacesPath()
    readPaths = []
    orderedNames = []
    for placeTagName in list(placeTagNames or []):
        placePath = basePath + "/" + str(placeTagName or "")
        if not tagExists(placePath):
            continue
        orderedNames.append(placeTagName)
        readPaths.extend([
            placePath + "/ContainerPresent",
            placePath + "/ContainerId",
        ])

    readResults = readTagValues(readPaths) if readPaths else []
    occupancyByTagName = {}
    for index, placeTagName in enumerate(orderedNames):
        offset = index * 2
        occupancyByTagName[placeTagName] = {
            "container_present": bool(
                readResults[offset].value
                if offset < len(readResults) and readResults[offset].quality.isGood()
                else False
            ),
            "container_id": str(
                readResults[offset + 1].value
                if offset + 1 < len(readResults) and readResults[offset + 1].quality.isGood()
                else ""
            ) or "",
        }
    return occupancyByTagName


def mirrorPlcPlaces(plcMappingState=None):
    """
    Mirror Fleet place occupancy into mapped PLC/Places rows.
    Unresolved place mappings are warned and skipped so last-good values remain.
    """
    logger = _log()
    plcMappingState = dict(plcMappingState or readPlcMappings() or {})
    placeMappings = dict(plcMappingState.get("place_tag_name_to_plc_tag") or {})

    if not plcMappingState.get("place_dataset_ok", True):
        return buildOperationResult(
            False,
            "warn",
            "Skipped PLC place sync because PlaceTagNameMapping is unreadable",
            data={"rows": [], "writes": [], "warnings": list(plcMappingState.get("warnings") or [])},
            rows=[],
            writes=[],
            warnings=list(plcMappingState.get("warnings") or []),
        )

    if not placeMappings:
        return buildOperationResult(
            True,
            "info",
            "No PLC place rows configured",
            data={"rows": [], "writes": [], "warnings": []},
            rows=[],
            writes=[],
            warnings=[],
        )

    fleetOccupancy = _fleetPlaceOccupancyByTagName(placeMappings.keys())
    warnings = []
    mirroredRows = []
    writePaths = []
    writeValues = []

    for placeTagName in sorted(placeMappings.keys()):
        plcTagName = str(placeMappings.get(placeTagName) or "")
        if not plcTagName:
            warnings.append("PLC place mapping for [{}] is blank".format(placeTagName))
            continue

        placeOccupancy = fleetOccupancy.get(placeTagName)
        if placeOccupancy is None:
            warning = "PLC place mapping [{} -> {}] did not resolve to a live Fleet place; holding last good value".format(
                placeTagName,
                plcTagName,
            )
            logger.warn(warning)
            warnings.append(warning)
            continue

        rowPath = plcPlaceRowPath(plcTagName)
        outputs = {
            "container_present": bool(placeOccupancy.get("container_present")),
            "container_id": str(placeOccupancy.get("container_id") or ""),
        }
        rowWritePaths, rowWriteValues = _buildPlaceWritePathsAndValues(rowPath, outputs)
        writePaths.extend(rowWritePaths)
        writeValues.extend(rowWriteValues)
        mirroredRows.append({
            "place_tag_name": placeTagName,
            "plc_tag_name": plcTagName,
            "row_path": rowPath,
            "container_present": outputs["container_present"],
            "container_id": outputs["container_id"],
        })

    failedWritePaths = []
    if writePaths:
        writeResults = system.tag.writeBlocking(writePaths, writeValues)
        for index, writeResult in enumerate(list(writeResults or [])):
            if isWriteResultGood(writeResult):
                continue
            failedWritePaths.append(writePaths[index])
            warning = "PLC place sync write failed for {}: {}".format(
                writePaths[index],
                writeResult,
            )
            logger.warn(warning)
            warnings.append(warning)

    ok = not warnings
    level = "info" if ok else "warn"
    message = "Synced PLC place occupancy for {} row(s)".format(len(mirroredRows))
    if warnings:
        message = "{} with {} warning(s)".format(message, len(warnings))

    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "rows": mirroredRows,
            "writes": list(zip(writePaths, writeValues)),
            "warnings": warnings,
            "failed_write_paths": failedWritePaths,
        },
        rows=mirroredRows,
        writes=list(zip(writePaths, writeValues)),
        warnings=warnings,
        failed_write_paths=failedWritePaths,
    )
