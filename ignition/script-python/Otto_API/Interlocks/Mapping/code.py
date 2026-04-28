from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagPaths import getFleetInterlocksPath
from Otto_API.Common.TagPaths import getInterlockMappingPath
from Otto_API.Common.TagPaths import getPlcFleetMappingPath
from Otto_API.Common.TagPaths import getInterlockWritebackRetryMsPath
from Otto_API.Common.TagPaths import getPlcInterlocksPath
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureMemoryTag
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Models.Interlocks import DuplicateInterlockMappingInfo
from Otto_API.Interlocks.Helpers import normalizeBool
from Otto_API.Models.Interlocks import InterlockMappingRow
from Otto_API.Models.Results import OperationalResult


VALID_DIRECTIONS = ["FromFleet", "ToFleet"]
PLC_INTERLOCK_TYPE_ID = "PLC_InterlockInterface"
DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS = 30000
INTERLOCK_MAPPING_HEADERS = ["FleetName", "PlcTagName", "Direction", "WriteEnable"]


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Mapping")


def _datasetWithHeaders(headers, rows=None):
    return system.dataset.toDataSet(list(headers or []), list(rows or []))


def buildInterlockMappingDataset(rows=None):
    return _datasetWithHeaders(INTERLOCK_MAPPING_HEADERS, rows)


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


def ensureInterlockTags():
    """
    Ensure the Fleet/Interlocks, PLC/Interlocks, and interlock config surfaces exist.
    """
    ensureFolder(getFleetInterlocksPath())
    ensureFolder(getPlcInterlocksPath())
    ensureFolder(getPlcFleetMappingPath())
    ensureMemoryTag(
        getInterlockMappingPath(),
        "DataSet",
        buildInterlockMappingDataset(),
    )
    ensureMemoryTag(
        getInterlockWritebackRetryMsPath(),
        "Int8",
        DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS,
    )


def _normalizeMappingRows(configRows):
    mappingByName = {}
    duplicateInfoByName = {}
    warnings = []
    issues = []

    for row in list(configRows or []):
        fleetName = normalizeTagValue(row.get("FleetName"))
        plcTagName = normalizeTagValue(row.get("PlcTagName"))
        direction = normalizeTagValue(row.get("Direction"))
        writeEnable = normalizeBool(row.get("WriteEnable"), True)

        if not fleetName or not plcTagName or not direction:
            warning = (
                "Interlock mapping row [{}] has blank FleetName, PlcTagName, or Direction"
            ).format(plcTagName or "<unknown>")
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "interlocks.mapping.blank_config.{}".format(plcTagName or "unknown"),
                "Otto_API.Interlocks.Mapping",
                "warn",
                warning,
            ))
            continue

        if direction not in VALID_DIRECTIONS:
            warning = "Interlock mapping row [{}] direction [{}] is invalid for FleetName [{}]; skipping row".format(
                plcTagName,
                direction,
                fleetName,
            )
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "interlocks.mapping.invalid_direction.{}".format(plcTagName),
                "Otto_API.Interlocks.Mapping",
                "warn",
                warning,
            ))
            continue

        normalizedRow = InterlockMappingRow(
            fleetName,
            plcTagName,
            direction,
            writeEnable,
        )

        existing = mappingByName.get(fleetName)
        if existing is not None:
            warning = (
                "Duplicate Interlock Mapping: FleetName [{}] is mapped more than once; "
                "replacing [{} / {}] with [{} / {}] and using the later row"
            ).format(
                fleetName,
                existing.PlcTagName,
                existing.Direction,
                plcTagName,
                direction,
            )
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "interlocks.mapping.duplicate_fleet_name.{}".format(fleetName),
                "Otto_API.Interlocks.Mapping",
                "warn",
                warning,
            ))
            duplicateInfoByName[fleetName] = DuplicateInterlockMappingInfo(
                fleetName,
                existing.PlcTagName,
                existing.Direction,
                plcTagName,
                direction,
            )

        mappingByName[fleetName] = normalizedRow

    normalizedRows = [mappingByName[name] for name in sorted(mappingByName.keys())]
    return normalizedRows, mappingByName, duplicateInfoByName, warnings, issues


def readInterlockMappings():
    """
    Read, validate, and normalize the interlock mapping dataset.
    """
    mappingResult = readTagValues([getInterlockMappingPath()])
    datasetResult = mappingResult[0] if len(mappingResult) > 0 else None
    warnings = []
    issues = []
    configRows = []

    if datasetResult is None or not datasetResult.quality.isGood():
        warning = "InterlockMapping is unreadable"
        warnings.append(warning)
        issues.append(buildRuntimeIssue(
            "interlocks.mapping.dataset_unreadable",
            "Otto_API.Interlocks.Mapping",
            "warn",
            warning,
        ))
    else:
        configRows, errorMessage = _datasetRows(datasetResult.value, INTERLOCK_MAPPING_HEADERS)
        if configRows is None:
            warning = "InterlockMapping is invalid: {}".format(errorMessage)
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "interlocks.mapping.dataset_invalid",
                "Otto_API.Interlocks.Mapping",
                "warn",
                warning,
            ))
            configRows = []

    normalizedRows, mappingByName, duplicateInfoByName, rowWarnings, rowIssues = _normalizeMappingRows(configRows)
    warnings.extend(list(rowWarnings or []))
    issues.extend(list(rowIssues or []))

    ok = not warnings
    level = "info"
    if warnings:
        level = "warn"

    message = "Interlock mapping loaded"
    if warnings:
        message = "Interlock mapping loaded with {} issue(s)".format(len(warnings))

    return OperationalResult(
        ok,
        level,
        message,
        typedFields={
            "rows": normalizedRows,
            "mapping_by_name": mappingByName,
            "duplicate_info_by_name": duplicateInfoByName,
        },
        sharedFields={
            "warnings": warnings,
            "issues": issues,
        },
    )


def ensurePlcInterlockRow(rowPath):
    ensureUdtInstancePath(rowPath, PLC_INTERLOCK_TYPE_ID)
    # Keep the member explicit so the local test shim exposes State the same way
    # the gateway-backed UDT instance will.
    ensureMemoryTag(rowPath + "/State", "Int4", 0)
    ensureMemoryTag(rowPath + "/ForceZero", "Boolean", False)
    ensureMemoryTag(rowPath + "/ResetIntlock_HMI_PB", "Boolean", False)
