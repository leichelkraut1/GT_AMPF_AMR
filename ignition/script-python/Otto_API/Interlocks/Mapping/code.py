from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagPaths import getFleetInterlocksPath
from Otto_API.Common.TagPaths import getInterlockWritebackRetryMsPath
from Otto_API.Common.TagPaths import getPlcInterlocksPath
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureMemoryTag
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Interlocks.Helpers import childRowNames


VALID_DIRECTIONS = ["FromFleet", "ToFleet"]
PLC_INTERLOCK_TYPE_ID = "PLC_InterlockInterface"
DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS = 30000


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Mapping")


def _normalizeBool(value, defaultValue=True):
    if isinstance(value, bool):
        return value

    text = str(value or "").strip().lower()
    if text in ["true", "1", "yes", "on"]:
        return True
    if text in ["false", "0", "no", "off"]:
        return False
    return bool(defaultValue)


def ensureInterlockTags():
    """
    Ensure the Fleet/Interlocks, PLC/Interlocks, and writeback config surfaces exist.
    """
    ensureFolder(getFleetInterlocksPath())
    ensureFolder(getPlcInterlocksPath())
    ensureMemoryTag(
        getInterlockWritebackRetryMsPath(),
        "Int8",
        DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS,
    )


def _normalizeMappingRows(configRows):
    mappingByName = {}
    warnings = []

    for row in list(configRows or []):
        fleetName = normalizeTagValue(row.get("FleetName"))
        plcTagName = normalizeTagValue(row.get("PlcTagName"))
        direction = normalizeTagValue(row.get("Direction"))
        writeEnable = _normalizeBool(row.get("WriteEnable"), True)

        if not fleetName or not plcTagName or not direction:
            warnings.append("PLC interlock row [{}] has blank Config/FleetName or Config/Direction".format(plcTagName or "<unknown>"))
            continue

        if direction not in VALID_DIRECTIONS:
            warnings.append(
                "PLC interlock row [{}] direction [{}] is invalid for FleetName [{}]; skipping row".format(
                    plcTagName,
                    direction,
                    fleetName,
                )
            )
            continue

        existing = mappingByName.get(fleetName)
        if existing is not None:
            warnings.append(
                "Duplicate Interlock Mapping: FleetName [{}] is mapped more than once; replacing [{} / {}] with [{} / {}] and using the later row".format(
                    fleetName,
                    existing.get("PlcTagName"),
                    existing.get("Direction"),
                    plcTagName,
                    direction,
                )
            )

        mappingByName[fleetName] = {
            "FleetName": fleetName,
            "PlcTagName": plcTagName,
            "Direction": direction,
            "WriteEnable": writeEnable,
        }

    normalizedRows = [mappingByName[name] for name in sorted(mappingByName.keys())]
    return normalizedRows, mappingByName, warnings


def readInterlockMappings():
    """
    Read, validate, and normalize PLC/Interlocks row values.
    """
    basePath = getPlcInterlocksPath()
    if not tagExists(basePath):
        warning = "PLC interlock root is missing: {}".format(basePath)
        return buildOperationResult(
            False,
            "warn",
            "PLC interlock config loaded with 1 issue(s)",
            data={
                "rows": [],
                "mapping_by_name": {},
                "warnings": [warning],
            },
            rows=[],
            mapping_by_name={},
            warnings=[warning],
        )
    rowNames = list(childRowNames(basePath) or [])
    warnings = []
    configRows = []

    for plcTagName in list(rowNames or []):
        rowPath = basePath + "/" + plcTagName
        configRows.append(
            {
                "FleetName": readOptionalTagValue(rowPath + "/Config/FleetName", ""),
                "PlcTagName": plcTagName,
                "Direction": readOptionalTagValue(rowPath + "/Config/Direction", ""),
                "WriteEnable": readOptionalTagValue(rowPath + "/Config/WriteEnable", True),
            }
        )

    normalizedRows, mappingByName, rowWarnings = _normalizeMappingRows(configRows)
    warnings.extend(list(rowWarnings or []))

    ok = not warnings
    level = "info"
    if warnings:
        level = "warn"

    message = "PLC interlock config loaded"
    if warnings:
        message = "PLC interlock config loaded with {} issue(s)".format(len(warnings))

    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "rows": normalizedRows,
            "mapping_by_name": mappingByName,
            "warnings": warnings,
        },
        rows=normalizedRows,
        mapping_by_name=mappingByName,
        warnings=warnings,
    )


def ensurePlcInterlockRow(rowPath):
    ensureUdtInstancePath(rowPath, PLC_INTERLOCK_TYPE_ID)
    # Keep the member explicit so the local test shim exposes State the same way
    # the gateway-backed UDT instance will.
    ensureMemoryTag(rowPath + "/State", "Int4", 0)
    ensureFolder(rowPath + "/Config")
    ensureMemoryTag(rowPath + "/Config/FleetName", "String", "")
    ensureMemoryTag(rowPath + "/Config/Direction", "String", "")
    ensureMemoryTag(rowPath + "/Config/WriteEnable", "Boolean", True)
