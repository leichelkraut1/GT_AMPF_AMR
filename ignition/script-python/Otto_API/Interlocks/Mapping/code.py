from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import deleteTagPath
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagPaths import getFleetInterlocksPath
from Otto_API.Common.TagPaths import getInterlockPlcMappingPath
from Otto_API.Common.TagPaths import getInterlockWritebackRetryMsPath
from Otto_API.Common.TagPaths import getPlcInterlocksPath
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureMemoryTag
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Interlocks.Helpers import childRowNames


INTERLOCK_PLC_MAPPING_HEADERS = ["InterlockName", "PlcTagName", "Direction"]
VALID_DIRECTIONS = ["FromFleet", "ToFleet"]
PLC_INTERLOCK_TYPE_ID = "PLC_InterlockInterface"
DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS = 30000


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Mapping")


def buildInterlockPlcMappingDataset(rows=None):
    return system.dataset.toDataSet(
        INTERLOCK_PLC_MAPPING_HEADERS,
        list(rows or []),
    )


def ensureInterlockTags():
    """
    Ensure the Fleet/Interlocks, PLC/Interlocks, and mapping dataset surfaces exist.
    """
    ensureFolder(getFleetInterlocksPath())
    ensureFolder(getPlcInterlocksPath())
    ensureMemoryTag(
        getInterlockPlcMappingPath(),
        "DataSet",
        buildInterlockPlcMappingDataset(),
    )
    ensureMemoryTag(
        getInterlockWritebackRetryMsPath(),
        "Int8",
        DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS,
    )


def _datasetRows(datasetValue, expectedHeaders):
    if datasetValue is None or not hasattr(datasetValue, "getColumnCount"):
        return None, "value is not a dataset"

    actualHeaders = []
    if hasattr(datasetValue, "getColumnNames"):
        actualHeaders = [str(header or "") for header in list(datasetValue.getColumnNames() or [])]
    else:
        for columnIndex in range(datasetValue.getColumnCount()):
            actualHeaders.append(str(datasetValue.getColumnName(columnIndex) or ""))

    if list(actualHeaders) != list(expectedHeaders):
        return None, "expected headers [{}], found [{}]".format(
            ", ".join(list(expectedHeaders)),
            ", ".join(actualHeaders),
        )

    rows = []
    for rowIndex in range(datasetValue.getRowCount()):
        row = {}
        for header in list(expectedHeaders):
            row[header] = datasetValue.getValueAt(rowIndex, header)
        rows.append(row)
    return rows, ""


def _normalizeMappingRows(datasetRows):
    mappingByName = {}
    warnings = []

    for row in list(datasetRows or []):
        interlockName = normalizeTagValue(row.get("InterlockName"))
        plcTagName = normalizeTagValue(row.get("PlcTagName"))
        direction = normalizeTagValue(row.get("Direction"))

        if not interlockName or not plcTagName or not direction:
            warnings.append("InterlockPlcMapping row has blank InterlockName, PlcTagName, or Direction")
            continue

        if direction not in VALID_DIRECTIONS:
            warnings.append(
                "InterlockPlcMapping direction [{}] is invalid for [{}]; skipping row".format(
                    direction,
                    interlockName,
                )
            )
            continue

        existing = mappingByName.get(interlockName)
        if existing is not None:
            warnings.append(
                "InterlockPlcMapping remaps [{}] from [{} / {}] to [{} / {}]; using the later row".format(
                    interlockName,
                    existing.get("PlcTagName"),
                    existing.get("Direction"),
                    plcTagName,
                    direction,
                )
            )

        mappingByName[interlockName] = {
            "InterlockName": interlockName,
            "PlcTagName": plcTagName,
            "Direction": direction,
        }

    normalizedRows = [mappingByName[name] for name in sorted(mappingByName.keys())]
    return normalizedRows, mappingByName, warnings


def readInterlockMappings():
    """
    Read, validate, and normalize Fleet/Config/InterlockPlcMapping.
    """
    results = readTagValues([getInterlockPlcMappingPath()])
    datasetResult = results[0] if results else None
    warnings = []
    datasetOk = bool(datasetResult is not None and datasetResult.quality.isGood())
    datasetRows = []

    if datasetOk:
        datasetRows, errorMessage = _datasetRows(
            datasetResult.value,
            INTERLOCK_PLC_MAPPING_HEADERS,
        )
        if datasetRows is None:
            datasetOk = False
            warnings.append("InterlockPlcMapping is invalid: {}".format(errorMessage))
            datasetRows = []
    else:
        warnings.append("InterlockPlcMapping is unreadable")

    normalizedRows, mappingByName, rowWarnings = _normalizeMappingRows(datasetRows)
    warnings.extend(list(rowWarnings or []))

    ok = datasetOk and not warnings
    level = "info"
    if not datasetOk:
        level = "error"
    elif warnings:
        level = "warn"

    message = "InterlockPlcMapping loaded"
    if warnings:
        message = "InterlockPlcMapping loaded with {} issue(s)".format(len(warnings))

    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "rows": normalizedRows,
            "mapping_by_name": mappingByName,
            "warnings": warnings,
            "dataset_ok": datasetOk,
        },
        rows=normalizedRows,
        mapping_by_name=mappingByName,
        warnings=warnings,
        dataset_ok=datasetOk,
    )

def _ensurePlcInterlockRow(rowPath):
    ensureUdtInstancePath(rowPath, PLC_INTERLOCK_TYPE_ID)
    # Keep the member explicit so the local test shim exposes State the same way
    # the gateway-backed UDT instance will.
    ensureMemoryTag(rowPath + "/State", "Int4", 0)


def syncPlcInterlockTags(mappingState=None):
    """
    Make PLC/Interlocks match the validated mapping dataset exactly.
    """
    logger = _log()
    mappingState = dict(mappingState or readInterlockMappings() or {})
    warnings = list(mappingState.get("warnings") or [])
    datasetOk = bool(mappingState.get("dataset_ok"))

    if not datasetOk:
        return buildOperationResult(
            False,
            "warn",
            "Skipped PLC interlock row sync because mapping dataset is unreadable",
            data={
                "row_names": [],
                "warnings": [],
            },
            row_names=[],
            warnings=[],
        )

    basePath = getPlcInterlocksPath()
    ensureFolder(basePath)
    wantedNames = []
    for row in list(mappingState.get("rows") or []):
        plcTagName = str(row.get("PlcTagName") or "").strip()
        if not plcTagName:
            continue
        wantedNames.append(plcTagName)
        _ensurePlcInterlockRow(basePath + "/" + plcTagName)

    wantedNameSet = set(list(wantedNames or []))
    for childName in childRowNames(basePath):
        if childName in wantedNameSet:
            continue
        deleteTagPath(basePath + "/" + childName)
        logger.info("Otto API - Removed stale PLC interlock row: " + str(childName))

    return buildOperationResult(
        True,
        "info",
        "Synced {} PLC interlock row(s)".format(len(wantedNames)),
        data={
            "row_names": wantedNames,
            "warnings": [],
        },
        row_names=wantedNames,
        warnings=[],
    )
