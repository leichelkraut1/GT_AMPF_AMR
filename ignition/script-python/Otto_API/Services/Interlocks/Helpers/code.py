from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.TagPaths import getFleetInterlocksPath
from Otto_API.Common.TagPaths import getPlcInterlocksPath
from Otto_API.Models.Interlocks import DuplicateInterlockMappingInfo
from Otto_API.Models.Interlocks import InterlockMappingRow
from Otto_API.Models.Interlocks import InterlockRecord
from Otto_API.Models.Interlocks import PlcInterlockSnapshot
from Otto_API.Models.Results import OperationalResult


SERVICE_SOURCE = "Otto_API.Services.Interlocks"


def toIntOrNone(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def fleetInterlockRowPath(interlockName, instanceNameByRawName):
    instanceName = str((instanceNameByRawName or {}).get(interlockName) or "").strip()
    if not instanceName:
        return ""
    return getFleetInterlocksPath() + "/" + instanceName


class InterlockSyncDefinition(object):
    def __init__(self, row, record, duplicateInfo, snapshot, fleetRowPath):
        self.row = InterlockMappingRow.fromDict(row)
        self.record = None if record is None else InterlockRecord.fromDict(record)
        self.duplicate_info = (
            None
            if duplicateInfo is None
            else DuplicateInterlockMappingInfo.fromDict(duplicateInfo)
        )
        if snapshot is None:
            self.snapshot = None
        elif isinstance(snapshot, PlcInterlockSnapshot):
            self.snapshot = snapshot
        else:
            self.snapshot = PlcInterlockSnapshot.fromValues(
                snapshot.get("plc_tag_name"),
                snapshot.get("state"),
                snapshot.get("force_zero"),
            )
        self.fleet_row_path = str(fleetRowPath or "").strip()
        self.fleet_name = self.row.FleetName
        self.plc_tag_name = self.row.PlcTagName
        self.direction = self.row.Direction
        self.from_fleet = self.row.isFromFleet()
        self.to_fleet = self.row.isToFleet()
        self.write_enabled = self.row.isWritable()
        self.has_fleet_row = bool(self.fleet_row_path)
        self.interlock_id = "" if self.record is None else str(self.record.id or "").strip()
        self.fleet_state = None if self.record is None else toIntOrNone(self.record.state)
        self.plc_state = None if self.snapshot is None else toIntOrNone(self.snapshot.state)
        self.force_zero_active = False if self.snapshot is None else self.snapshot.forceZeroActive()
        self.fleet_state_path = self.fleet_row_path + "/State" if self.fleet_row_path else ""
        self.plc_state_path = getPlcInterlocksPath() + "/" + self.plc_tag_name + "/State"

    def targetState(self, desiredState=None, plcStateOverride=None):
        if desiredState is not None:
            return desiredState
        if plcStateOverride is not None:
            return plcStateOverride
        return self.plc_state

    def duplicateWinnerMessage(self):
        if self.duplicate_info is None:
            return ""
        return "; Duplicate Interlock Mapping is also present and the later row [{} / {}] won".format(
            self.duplicate_info.winning_plc_tag_name,
            self.duplicate_info.winning_direction,
        )

    def pendingPath(self, leafName):
        return self.fleet_row_path + "/" + str(leafName or "").strip()


def buildInterlockSyncDefinition(
    rowOrDefinition,
    recordsByName=None,
    instanceNameByRawName=None,
    duplicateInfoByName=None,
    plcSnapshotByTagName=None,
):
    """
    Build one complete interlock sync definition from typed or raw inputs.
    """
    return coerceInterlockSyncDefinition(
        rowOrDefinition,
        recordsByName=recordsByName,
        instanceNameByRawName=instanceNameByRawName,
        duplicateInfoByName=duplicateInfoByName,
        plcSnapshotByTagName=plcSnapshotByTagName,
    )


def coerceInterlockSyncDefinition(
    rowOrDefinition,
    recordsByName=None,
    instanceNameByRawName=None,
    duplicateInfoByName=None,
    plcSnapshotByTagName=None,
):
    if isinstance(rowOrDefinition, InterlockSyncDefinition):
        return rowOrDefinition

    row = InterlockMappingRow.fromDict(rowOrDefinition)
    fleetName = row.FleetName
    plcTagName = row.PlcTagName
    record = None if recordsByName is None else (recordsByName or {}).get(fleetName)
    duplicateInfo = None if duplicateInfoByName is None else (duplicateInfoByName or {}).get(fleetName)
    snapshot = None if plcSnapshotByTagName is None else (plcSnapshotByTagName or {}).get(plcTagName)
    fleetRowPath = fleetInterlockRowPath(fleetName, instanceNameByRawName)
    return InterlockSyncDefinition(
        row,
        record,
        duplicateInfo,
        snapshot,
        fleetRowPath,
    )


def extendWarningsAndIssues(targetWarnings, targetIssues, result):
    targetWarnings.extend(list(result.warnings or []))
    targetIssues.extend(list(result.issues or []))


def extendIssues(targetIssues, result):
    targetIssues.extend(list(result.issues or []))


class InterlockFleetSyncResult(OperationalResult):
    def __init__(self, ok, level, message, getResult=None, applyResult=None, warnings=None, issues=None):
        self.get_result = getResult
        self.apply_result = applyResult
        self.warnings = list(warnings or [])
        self.issues = list(issues or [])
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            typedFields={
                "get_result": self.get_result,
                "apply_result": self.apply_result,
            },
            sharedFields={
                "warnings": self.warnings,
                "issues": self.issues,
            },
        )


class InterlockSyncResult(OperationalResult):
    def __init__(
        self,
        ok,
        level,
        message,
        getResult=None,
        applyResult=None,
        mappingResult=None,
        directionalResults=None,
        warnings=None,
        issues=None,
    ):
        self.get_result = getResult
        self.apply_result = applyResult
        self.mapping_result = mappingResult
        self.directional_results = list(directionalResults or [])
        self.warnings = list(warnings or [])
        self.issues = list(issues or [])
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            typedFields={
                "get_result": self.get_result,
                "apply_result": self.apply_result,
                "mapping_result": self.mapping_result,
                "directional_results": self.directional_results,
            },
            sharedFields={
                "warnings": self.warnings,
                "issues": self.issues,
            },
        )


class InterlockDirectionalResult(OperationalResult):
    def __init__(self, ok, level, message, postResult=None, issues=None):
        self.post_result = postResult
        self.issues = list(issues or [])
        typedFields = {}
        if self.post_result is not None:
            typedFields["post_result"] = self.post_result
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            typedFields=typedFields,
            sharedFields={"issues": self.issues},
        )


class InterlockRuntimeResult(OperationalResult):
    def __init__(self, ok, level, message, issues=None):
        self.issues = list(issues or [])
        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            sharedFields={"issues": self.issues},
        )


def syncIssue(issueId, level, message):
    return buildRuntimeIssue(
        issueId,
        SERVICE_SOURCE,
        level,
        message,
    )


def syncIssueResult(issueId, level, message, postResult=None):
    return InterlockDirectionalResult(
        False,
        level,
        message,
        postResult=postResult,
        issues=[syncIssue(issueId, level, message)],
    )


def statusFromResult(result):
    if result is not None and result.ok:
        return "Healthy"

    level = "" if result is None else str(result.level or "").lower()
    if level == "error":
        return "Error"
    return "Warn"


def messageFromResult(result):
    return "" if result is None else str(result.message or "")
