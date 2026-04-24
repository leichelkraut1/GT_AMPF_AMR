from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readOptionalTagValue
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
DEFAULT_INTERLOCK_MASK = 65535


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


def _normalizeMask(value, plcTagName, warnings):
    if value is None:
        warnings.append(
            "PLC interlock row [{}] has blank Config/Mask; using default [{}]".format(
                plcTagName or "<unknown>",
                DEFAULT_INTERLOCK_MASK,
            )
        )
        return DEFAULT_INTERLOCK_MASK

    text = str(value).strip()
    if not text:
        warnings.append(
            "PLC interlock row [{}] has blank Config/Mask; using default [{}]".format(
                plcTagName or "<unknown>",
                DEFAULT_INTERLOCK_MASK,
            )
        )
        return DEFAULT_INTERLOCK_MASK

    try:
        mask = int(value)
    except Exception:
        warnings.append(
            "PLC interlock row [{}] has unreadable Config/Mask [{}]; using default [{}]".format(
                plcTagName or "<unknown>",
                value,
                DEFAULT_INTERLOCK_MASK,
            )
        )
        return DEFAULT_INTERLOCK_MASK

    if mask < 0:
        warnings.append(
            "PLC interlock row [{}] has negative Config/Mask [{}]; using default [{}]".format(
                plcTagName or "<unknown>",
                mask,
                DEFAULT_INTERLOCK_MASK,
            )
        )
        return DEFAULT_INTERLOCK_MASK

    return mask


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
        mask = _normalizeMask(row.get("Mask"), plcTagName, warnings)

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
                "PLC interlock config remaps FleetName [{}] from [{} / {}] to [{} / {}]; using the later row".format(
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
            "Mask": mask,
        }

    normalizedRows = [mappingByName[name] for name in sorted(mappingByName.keys())]
    return normalizedRows, mappingByName, warnings


def readInterlockMappings():
    """
    Read, validate, and normalize PLC/Interlocks/*/Config values.
    """
    basePath = getPlcInterlocksPath()
    ensureFolder(basePath)
    rowNames = list(childRowNames(basePath) or [])
    warnings = []
    configRows = []

    for plcTagName in list(rowNames or []):
        rowPath = basePath + "/" + plcTagName
        _ensurePlcInterlockRow(rowPath)
        configRows.append(
            {
                "FleetName": readOptionalTagValue(rowPath + "/Config/FleetName", ""),
                "PlcTagName": plcTagName,
                "Direction": readOptionalTagValue(rowPath + "/Config/Direction", ""),
                "WriteEnable": readOptionalTagValue(rowPath + "/Config/WriteEnable", True),
                "Mask": readOptionalTagValue(rowPath + "/Config/Mask", DEFAULT_INTERLOCK_MASK),
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


def _ensurePlcInterlockRow(rowPath):
    ensureUdtInstancePath(rowPath, PLC_INTERLOCK_TYPE_ID)
    # Keep the member explicit so the local test shim exposes State the same way
    # the gateway-backed UDT instance will.
    ensureMemoryTag(rowPath + "/State", "Int4", 0)
    ensureFolder(rowPath + "/Config")
    ensureMemoryTag(rowPath + "/Config/FleetName", "String", "")
    ensureMemoryTag(rowPath + "/Config/Direction", "String", "")
    ensureMemoryTag(rowPath + "/Config/WriteEnable", "Boolean", True)
    ensureMemoryTag(rowPath + "/Config/Mask", "Int8", DEFAULT_INTERLOCK_MASK)
