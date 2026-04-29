from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.SyncHelpers import cleanupStaleUdtInstances
from Otto_API.Common.TagIO import normalizeTagValue
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TagProvisioning import ensureFolder
from Otto_API.Common.TagProvisioning import ensureMemoryTag
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Models.Results import OperationHealth
from Otto_API.Models.Results import OperationalResult

from MainController.State.Paths import PLC_PLACES_BASE
from MainController.State.Paths import PLC_ROBOTS_BASE
from MainController.State.Paths import plcFleetMappingPath
from MainController.State.Paths import plcPlaceTagNameMappingPath
from MainController.State.Paths import plcRobotTagNameMappingPath
from MainController.State.Paths import ROBOT_NAMES


ROBOT_TAG_NAME_MAPPING_HEADERS = ["FleetRobotName", "PlcTagName"]
PLACE_TAG_NAME_MAPPING_HEADERS = ["PlaceTagName", "PlcTagName"]


class PlcMappingState(OperationHealth):
    def __init__(
        self,
        ok=True,
        level="info",
        message="",
        robotNameToPlcTag=None,
        plcRobotTagToRobotName=None,
        placeTagNameToPlcTag=None,
        plcPlaceTagToPlaceTagName=None,
        warnings=None,
        issues=None,
        robotDatasetOk=True,
        placeDatasetOk=True,
    ):
        OperationHealth.__init__(self, ok, level, message, warnings, issues)
        self.robot_name_to_plc_tag = dict(robotNameToPlcTag or {})
        self.plc_robot_tag_to_robot_name = dict(plcRobotTagToRobotName or {})
        self.place_tag_name_to_plc_tag = dict(placeTagNameToPlcTag or {})
        self.plc_place_tag_to_place_tag_name = dict(plcPlaceTagToPlaceTagName or {})
        self.robot_dataset_ok = bool(robotDatasetOk)
        self.place_dataset_ok = bool(placeDatasetOk)

    @classmethod
    def fromDict(cls, mappingState):
        if isinstance(mappingState, cls):
            return mappingState
        mappingState = dict(mappingState or {})
        data = mappingState.get("data")
        if isinstance(data, dict):
            payload = dict(data or {})
            payload["ok"] = mappingState.get("ok")
            payload["level"] = mappingState.get("level")
            payload["message"] = mappingState.get("message")
        else:
            payload = mappingState

        warnings = list(payload.get("warnings") or [])
        robotDatasetOk = bool(payload.get("robot_dataset_ok", True))
        placeDatasetOk = bool(payload.get("place_dataset_ok", True))
        ok = payload.get("ok")
        if ok is None:
            ok = robotDatasetOk and placeDatasetOk and not warnings

        return cls(
            ok,
            payload.get("level") or ("info" if ok else "warn"),
            payload.get("message") or "",
            robotNameToPlcTag=payload.get("robot_name_to_plc_tag"),
            plcRobotTagToRobotName=payload.get("plc_robot_tag_to_robot_name"),
            placeTagNameToPlcTag=payload.get("place_tag_name_to_plc_tag"),
            plcPlaceTagToPlaceTagName=payload.get("plc_place_tag_to_place_tag_name"),
            warnings=warnings,
            issues=payload.get("issues"),
            robotDatasetOk=robotDatasetOk,
            placeDatasetOk=placeDatasetOk,
        )

    def toDict(self):
        data = self.healthDict()
        data.update({
            "robot_name_to_plc_tag": dict(self.robot_name_to_plc_tag or {}),
            "plc_robot_tag_to_robot_name": dict(self.plc_robot_tag_to_robot_name or {}),
            "place_tag_name_to_plc_tag": dict(self.place_tag_name_to_plc_tag or {}),
            "plc_place_tag_to_place_tag_name": dict(self.plc_place_tag_to_place_tag_name or {}),
            "robot_dataset_ok": self.robot_dataset_ok,
            "place_dataset_ok": self.place_dataset_ok,
        })
        return data


def _log():
    return system.util.getLogger("MainController.State.PlcMappingStore")


def _datasetWithHeaders(headers, rows=None):
    return system.dataset.toDataSet(list(headers or []), list(rows or []))


def buildRobotTagNameMappingDataset():
    """Build the default robot mapping dataset with identity mappings for every configured robot."""
    rows = []
    for robotName in list(ROBOT_NAMES or []):
        rows.append([robotName, robotName])
    return _datasetWithHeaders(ROBOT_TAG_NAME_MAPPING_HEADERS, rows)


def buildPlaceTagNameMappingDataset():
    """Build the default empty place mapping dataset."""
    return _datasetWithHeaders(PLACE_TAG_NAME_MAPPING_HEADERS, [])


def resolvePlcRobotTagName(fleetRobotName, mappingState=None):
    """Resolve one Fleet robot name to its configured PLC robot tag name."""
    fleetRobotName = normalizeTagValue(fleetRobotName)
    if not fleetRobotName:
        return ""

    if mappingState is None:
        mappingState = readPlcMappings()
    mappingState = PlcMappingState.fromDict(mappingState)
    return normalizeTagValue(
        mappingState.robot_name_to_plc_tag.get(fleetRobotName)
    )


def ensurePlcMappingTags():
    """Ensure the mapping datasets and their PLC/Fleet sync folders exist."""
    ensureFolder(PLC_ROBOTS_BASE)
    ensureFolder(PLC_PLACES_BASE)
    ensureFolder(plcFleetMappingPath())
    ensureMemoryTag(
        plcRobotTagNameMappingPath(),
        "DataSet",
        buildRobotTagNameMappingDataset(),
    )
    ensureMemoryTag(
        plcPlaceTagNameMappingPath(),
        "DataSet",
        buildPlaceTagNameMappingDataset(),
    )


def _datasetRows(datasetValue, expectedHeaders):
    """Return raw dataset rows when the value is a dataset with the expected headers."""
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


def _normalizeMappingRows(datasetRows, leftHeader, label, allowedLeftValues=None):
    """
    Normalize one mapping dataset into a simple left->right map.

    Contract:
    - exact headers are handled earlier
    - blank rows are ignored
    - later duplicate rows win with warnings
    """
    mapping = {}
    warnings = []
    issues = []
    allowedLeftValues = set(list(allowedLeftValues or []))
    enforceAllowedValues = bool(allowedLeftValues)

    for row in list(datasetRows or []):
        leftValue = normalizeTagValue(row.get(leftHeader))
        plcTagName = normalizeTagValue(row.get("PlcTagName"))
        if not leftValue or not plcTagName:
            warning = "{} row has blank {} or PlcTagName".format(label, leftHeader)
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "plc_mapping.{}.blank_row".format(label.lower()),
                "MainController.State.PlcMappingStore",
                "warn",
                warning,
            ))
            continue
        if enforceAllowedValues and leftValue not in allowedLeftValues:
            warning = "{} references unknown {} [{}]".format(label, leftHeader, leftValue)
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "plc_mapping.{}.unknown_left_value.{}".format(label.lower(), leftValue),
                "MainController.State.PlcMappingStore",
                "warn",
                warning,
            ))
            continue
        if leftValue in mapping and mapping[leftValue] != plcTagName:
            warning = "{} remaps [{}] from [{}] to [{}]; using the later row".format(
                label,
                leftValue,
                mapping[leftValue],
                plcTagName,
            )
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "plc_mapping.{}.duplicate_mapping.{}".format(label.lower(), leftValue),
                "MainController.State.PlcMappingStore",
                "warn",
                warning,
            ))
        mapping[leftValue] = plcTagName

    return mapping, warnings, issues


def readPlcMappings():
    """
    Read, validate, and normalize the PLC/Fleet mapping datasets for the current cycle.

    Returned state shape:
    - robot_name_to_plc_tag: Fleet robot name -> PLC robot row tag name
    - plc_robot_tag_to_robot_name: reverse lookup of the above
    - place_tag_name_to_plc_tag: Fleet place tag name -> PLC place row tag name
    - plc_place_tag_to_place_tag_name: reverse lookup of the above
    - robot_dataset_ok: whether RobotTagNameMapping was readable and structurally valid
    - place_dataset_ok: whether PlaceTagNameMapping was readable and structurally valid
    - warnings: non-fatal issues such as blank rows, unknown names, or duplicate remaps

    Controller code passes this typed state through several layers as plcMappingState, so this
    docstring is the contract for what callers may safely expect to exist.
    """
    results = readTagValues([
        plcRobotTagNameMappingPath(),
        plcPlaceTagNameMappingPath(),
    ])
    robotResult = results[0] if len(results) > 0 else None
    placeResult = results[1] if len(results) > 1 else None

    robotRows = []
    placeRows = []
    warnings = []
    issues = []
    robotDatasetOk = bool(robotResult is not None and robotResult.quality.isGood())
    placeDatasetOk = bool(placeResult is not None and placeResult.quality.isGood())

    if robotDatasetOk and robotResult is not None:
        robotRows, errorMessage = _datasetRows(
            robotResult.value,
            ROBOT_TAG_NAME_MAPPING_HEADERS,
        )
        if robotRows is None:
            robotDatasetOk = False
            warning = "RobotTagNameMapping is invalid: {}".format(errorMessage)
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "plc_mapping.robot_dataset_invalid",
                "MainController.State.PlcMappingStore",
                "error",
                warning,
            ))
            robotRows = []
    else:
        warning = "RobotTagNameMapping is unreadable"
        warnings.append(warning)
        issues.append(buildRuntimeIssue(
            "plc_mapping.robot_dataset_unreadable",
            "MainController.State.PlcMappingStore",
            "error",
            warning,
        ))

    if placeDatasetOk and placeResult is not None:
        placeRows, errorMessage = _datasetRows(
            placeResult.value,
            PLACE_TAG_NAME_MAPPING_HEADERS,
        )
        if placeRows is None:
            placeDatasetOk = False
            warning = "PlaceTagNameMapping is invalid: {}".format(errorMessage)
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "plc_mapping.place_dataset_invalid",
                "MainController.State.PlcMappingStore",
                "error",
                warning,
            ))
            placeRows = []
    else:
        warning = "PlaceTagNameMapping is unreadable"
        warnings.append(warning)
        issues.append(buildRuntimeIssue(
            "plc_mapping.place_dataset_unreadable",
            "MainController.State.PlcMappingStore",
            "error",
            warning,
        ))

    robotMappings, robotWarnings, robotIssues = _normalizeMappingRows(
        robotRows,
        "FleetRobotName",
        "RobotTagNameMapping",
        allowedLeftValues=ROBOT_NAMES,
    )
    placeMappings, placeWarnings, placeIssues = _normalizeMappingRows(
        placeRows,
        "PlaceTagName",
        "PlaceTagNameMapping",
    )
    warnings.extend(list(robotWarnings or []))
    warnings.extend(list(placeWarnings or []))
    issues.extend(list(robotIssues or []))
    issues.extend(list(placeIssues or []))

    ok = robotDatasetOk and placeDatasetOk and not warnings
    level = "info"
    if not robotDatasetOk or not placeDatasetOk:
        level = "error"
    elif warnings:
        level = "warn"

    message = "PLC FleetMapping loaded"
    if warnings:
        issueCount = len(warnings)
        message = "PLC FleetMapping loaded with {} issue(s)".format(issueCount)
    elif not ok:
        message = "PLC FleetMapping degraded"

    plcRobotTagToRobotName = dict([
        (plcTagName, robotName)
        for robotName, plcTagName in robotMappings.items()
    ])
    plcPlaceTagToPlaceName = dict([
        (plcTagName, placeTagName)
        for placeTagName, plcTagName in placeMappings.items()
    ])

    return PlcMappingState(
        ok,
        level,
        message,
        robotNameToPlcTag=robotMappings,
        plcRobotTagToRobotName=plcRobotTagToRobotName,
        placeTagNameToPlcTag=placeMappings,
        plcPlaceTagToPlaceTagName=plcPlaceTagToPlaceName,
        warnings=warnings,
        issues=issues,
        robotDatasetOk=robotDatasetOk,
        placeDatasetOk=placeDatasetOk,
    )


def _syncFleetTagRows(basePath, typeId, wantedNames, datasetOk, warnings, logger, label):
    if not datasetOk:
        payload = {"row_names": [], "warnings": list(warnings or [])}
        return OperationalResult(
            False,
            "warn",
            "Skipped {} row sync because mapping dataset is unreadable".format(label),
            dataFields=payload,
        ).toDict()

    ensureFolder(basePath)
    for rowName in list(wantedNames or []):
        ensureUdtInstancePath(basePath + "/" + rowName, typeId)

    cleanupStaleUdtInstances(
        basePath,
        list(wantedNames or []),
        logger,
        "Removed stale {} row: ".format(label),
        cleanupWarnPrefix="{} row cleanup skipped due to error: ".format(label),
    )

    ok = not warnings
    payload = {
        "row_names": list(wantedNames or []),
        "warnings": list(warnings or []),
    }
    return OperationalResult(
        ok,
        "info" if ok else "warn",
        "Synced {} row(s) for {}".format(len(list(wantedNames or [])), label),
        dataFields=payload,
    ).toDict()


def syncPlcFleetTags(mappingState=None):
    """Make PLC/Robots and PLC/Places match the validated PLC/Fleet mapping datasets exactly."""
    logger = _log()
    ensurePlcMappingTags()
    if mappingState is None:
        mappingState = readPlcMappings()
    mappingState = PlcMappingState.fromDict(mappingState)
    warnings = list(mappingState.warnings or [])

    robotResult = _syncFleetTagRows(
        PLC_ROBOTS_BASE,
        "PLC_RobotInterface",
        sorted(list((mappingState.robot_name_to_plc_tag or {}).values())),
        bool(mappingState.robot_dataset_ok),
        warnings,
        logger,
        "PLC robot tags",
    )
    placeResult = _syncFleetTagRows(
        PLC_PLACES_BASE,
        "PLC_PlaceInterface",
        sorted(list((mappingState.place_tag_name_to_plc_tag or {}).values())),
        bool(mappingState.place_dataset_ok),
        warnings,
        logger,
        "PLC place tags",
    )

    ok = bool(robotResult.get("ok")) and bool(placeResult.get("ok"))
    level = "info" if ok else "warn"
    payload = {
        "robot_result": robotResult,
        "place_result": placeResult,
    }
    return OperationalResult(
        ok,
        level,
        "Synced PLC Fleet tags",
        dataFields=payload,
    ).toDict()
