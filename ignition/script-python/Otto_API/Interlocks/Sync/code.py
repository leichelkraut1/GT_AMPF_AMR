import time

from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import getOttoOperationsUrl
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readOptionalTagValues
from Otto_API.Common.TagIO import writeObservedTagValue
from Otto_API.Common.TagIO import writeObservedTagValues
from Otto_API.Common.TagPaths import getFleetInterlocksPath
from Otto_API.Common.TagPaths import getInterlockWritebackRetryMsPath
from Otto_API.Common.TagPaths import getPlcInterlocksPath
from Otto_API.Interlocks.Apply import applyInterlockSync
from Otto_API.Interlocks.Mapping import DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS
from Otto_API.Interlocks.Mapping import readInterlockMappings
from Otto_API.Models.Interlocks import DuplicateInterlockMappingInfo
from Otto_API.Models.Interlocks import InterlockMappingRow
from Otto_API.Models.Interlocks import InterlockRecord
from Otto_API.Models.Interlocks import PlcInterlockSnapshot
from Otto_API.Models.Results import RecordSyncResult
from Otto_API.Models.Results import TypedOperationResult
from Otto_API.WebAPI.Interlocks import fetchInterlocks
from Otto_API.WebAPI.Interlocks import postInterlockState


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Sync")


def _toIntOrNone(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _nowEpochMs():
    return int(time.time() * 1000)


def _fleetInterlockRowPath(interlockName, instanceNameByRawName):
    instanceName = str((instanceNameByRawName or {}).get(interlockName) or "").strip()
    if not instanceName:
        return ""
    return getFleetInterlocksPath() + "/" + instanceName


class _ResolvedInterlockSyncRow(object):
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
        self.fleet_state_path = self.fleet_row_path + "/State" if self.fleet_row_path else ""
        self.plc_state_path = getPlcInterlocksPath() + "/" + self.plc_tag_name + "/State"

    def direction(self):
        return self.row.Direction

    def isFromFleet(self):
        return self.row.isFromFleet()

    def isToFleet(self):
        return self.row.isToFleet()

    def remoteState(self):
        return None if self.record is None else _toIntOrNone(self.record.state)

    def fleetState(self):
        return self.remoteState()

    def hasFleetRow(self):
        return bool(self.fleet_row_path)

    def interlockId(self):
        return "" if self.record is None else str(self.record.id or "").strip()

    def hasInterlockId(self):
        return bool(self.interlockId())

    def plcState(self):
        return None if self.snapshot is None else _toIntOrNone(self.snapshot.state)

    def targetState(self, desiredState=None, plcStateOverride=None):
        if desiredState is not None:
            return desiredState
        if plcStateOverride is not None:
            return plcStateOverride
        return self.plcState()

    def forceZeroActive(self):
        return False if self.snapshot is None else self.snapshot.forceZeroActive()

    def writeEnabled(self):
        return self.row.isWritable()

    def shouldWriteToFleet(self, ignoreWriteEnable=False):
        return bool(ignoreWriteEnable or self.writeEnabled())

    def duplicateWinnerMessage(self):
        if self.duplicate_info is None:
            return ""
        return "; Duplicate Interlock Mapping is also present and the later row [{} / {}] won".format(
            self.duplicate_info.winning_plc_tag_name,
            self.duplicate_info.winning_direction,
        )

    def pendingPath(self, leafName):
        return self.fleet_row_path + "/" + str(leafName or "").strip()


def buildResolvedSyncRow(
    rowOrResolved,
    recordsByName=None,
    instanceNameByRawName=None,
    duplicateInfoByName=None,
    plcSnapshotByTagName=None,
):
    """
    Build one resolved interlock sync row from typed or raw inputs.

    This keeps the resolved-row contract stable for callers that need a
    concrete sync-row object without reaching into private coercion helpers.
    """
    return _coerceResolvedSyncRow(
        rowOrResolved,
        recordsByName=recordsByName,
        instanceNameByRawName=instanceNameByRawName,
        duplicateInfoByName=duplicateInfoByName,
        plcSnapshotByTagName=plcSnapshotByTagName,
    )


def _coerceResolvedSyncRow(
    rowOrResolved,
    recordsByName=None,
    instanceNameByRawName=None,
    duplicateInfoByName=None,
    plcSnapshotByTagName=None,
):
    if isinstance(rowOrResolved, _ResolvedInterlockSyncRow):
        return rowOrResolved

    row = InterlockMappingRow.fromDict(rowOrResolved)
    fleetName = row.FleetName
    plcTagName = row.PlcTagName
    record = None if recordsByName is None else (recordsByName or {}).get(fleetName)
    duplicateInfo = None if duplicateInfoByName is None else (duplicateInfoByName or {}).get(fleetName)
    snapshot = None if plcSnapshotByTagName is None else (plcSnapshotByTagName or {}).get(plcTagName)
    fleetRowPath = _fleetInterlockRowPath(fleetName, instanceNameByRawName)
    return _ResolvedInterlockSyncRow(
        row,
        record,
        duplicateInfo,
        snapshot,
        fleetRowPath,
    )


def _readRetryMs():
    retryMs = _toIntOrNone(
        readOptionalTagValue(
            getInterlockWritebackRetryMsPath(),
            DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS,
        )
    )
    if retryMs is None or retryMs < 0:
        return DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS
    return retryMs


def _writePendingState(
    rowPath,
    pendingWrite,
    pendingState,
    pendingStartedMs,
    lastWriteAttemptMs,
    lastCommandedState,
    lastCommandedMs,
    logger,
):
    _writePendingFields(
        rowPath,
        [
            ("PendingWriteToFleet", bool(pendingWrite)),
            ("PendingWriteState", int(pendingState or 0)),
            ("PendingWriteStartedMs", int(pendingStartedMs or 0)),
            ("LastWriteAttemptMs", int(lastWriteAttemptMs or 0)),
            ("LastCommandedState", int(lastCommandedState or 0)),
            ("LastCommandedMs", int(lastCommandedMs or 0)),
        ],
        logger,
    )


def _clearPendingState(rowPath, logger):
    _writePendingFields(
        rowPath,
        [
            ("PendingWriteToFleet", False),
            ("PendingWriteState", 0),
            ("PendingWriteStartedMs", 0),
        ],
        logger,
    )


def _writePendingFields(rowPath, fieldValues, logger):
    tagPaths = []
    values = []
    labels = []
    for fieldName, value in list(fieldValues or []):
        tagPaths.append(rowPath + "/" + str(fieldName or "").strip())
        values.append(value)
        labels.append("Interlock pending-write sync")
    writeObservedTagValues(tagPaths, values, labels=labels, logger=logger)


def _readPlcSnapshot(rows):
    snapshotByPlcTagName = {}
    plcTagNames = []
    tagPaths = []
    defaultValues = []

    for row in list(rows or []):
        row = InterlockMappingRow.fromDict(row)
        plcTagName = str(row.PlcTagName or "").strip()
        if not plcTagName or plcTagName in snapshotByPlcTagName:
            continue

        snapshotByPlcTagName[plcTagName] = None
        plcTagNames.append(plcTagName)
        rowPath = getPlcInterlocksPath() + "/" + plcTagName
        tagPaths.extend([
            rowPath + "/State",
            rowPath + "/ForceZero",
        ])
        defaultValues.extend([None, False])

    values = readOptionalTagValues(tagPaths, defaultValues)
    for index, plcTagName in enumerate(plcTagNames):
        stateValue = values[(index * 2)] if (index * 2) < len(values) else None
        forceZeroValue = values[(index * 2) + 1] if ((index * 2) + 1) < len(values) else False
        snapshotByPlcTagName[plcTagName] = PlcInterlockSnapshot.fromValues(
            plcTagName,
            stateValue,
            forceZeroValue,
        )

    return snapshotByPlcTagName


def _extendWarningsAndIssues(targetWarnings, targetIssues, result):
    targetWarnings.extend(list(getattr(result, "warnings", []) or []))
    targetIssues.extend(list(getattr(result, "issues", []) or []))


def _extendIssues(targetIssues, result):
    targetIssues.extend(list(getattr(result, "issues", []) or []))


def _syncIssueResult(issueId, level, message, postResult=None):
    typedFields = {}
    if postResult is not None:
        typedFields["post_result"] = postResult
    return TypedOperationResult(
        False,
        level,
        message,
        typedFields=typedFields,
        sharedFields={
            "issues": [
                buildRuntimeIssue(
                    issueId,
                    "Otto_API.Interlocks.Sync",
                    level,
                    message,
                )
            ],
        },
    )


def _collectDirectionalResult(warnings, issues, directionalResult, logger):
    if not directionalResult.ok:
        warnings.append(directionalResult.message)
    _extendIssues(issues, directionalResult)
    level = str(directionalResult.level or "").lower()
    message = str(directionalResult.message or "")
    if level == "error":
        logger.error("Otto API - " + message)


def _runDirectionalSync(resolved, logger):
    if resolved.forceZeroActive():
        return _applyToFleet(
            resolved,
            logger=logger,
            desiredState=0,
            actionKey="forcezero",
            actionLabel="ForceZero",
            ignoreWriteEnable=True,
            forceZeroActive=True,
        )

    if resolved.isFromFleet():
        return _applyFromFleet(resolved, logger=logger)
    if resolved.isToFleet():
        return _applyToFleet(resolved, logger=logger)
    return None


def _applyFromFleet(resolved, logger=None):
    fleetName = resolved.fleet_name
    if not resolved.hasFleetRow():
        message = "FromFleet skipped [{}] because the Fleet interlock row was not found".format(fleetName)
        return _syncIssueResult("interlocks.sync.fromfleet.missing_row.{}".format(fleetName), "warn", message)

    fleetState = _toIntOrNone(readOptionalTagValue(resolved.fleet_state_path, None))
    if fleetState is None:
        message = "FromFleet skipped [{}] because Fleet state is unreadable".format(fleetName)
        return _syncIssueResult("interlocks.sync.fromfleet.unreadable_state.{}".format(fleetName), "warn", message)

    writeObservedTagValue(resolved.plc_state_path, fleetState, label="Interlock FromFleet sync", logger=logger)
    return TypedOperationResult(
        True,
        "info",
        "FromFleet synced [{}] -> [{}]".format(fleetName, resolved.plc_tag_name),
        sharedFields={"issues": []},
    )


def _applyToFleet(
    resolved,
    logger=None,
    desiredState=None,
    actionKey="tofleet",
    actionLabel="ToFleet",
    ignoreWriteEnable=False,
    forceZeroActive=False,
    plcStateOverride=None,
):
    fleetName = resolved.fleet_name
    if not resolved.shouldWriteToFleet(ignoreWriteEnable):
        return TypedOperationResult(
            True,
            "info",
            "{} disabled [{}] because WriteEnable is false".format(actionLabel, fleetName),
            sharedFields={"issues": []},
        )

    fleetState = resolved.fleetState()
    plcState = resolved.targetState(plcStateOverride=plcStateOverride)
    if plcStateOverride is None and desiredState is None:
        plcState = _toIntOrNone(readOptionalTagValue(resolved.plc_state_path, None))
    nowEpochMs = _nowEpochMs()
    retryMs = _readRetryMs()
    targetState = resolved.targetState(desiredState=desiredState, plcStateOverride=plcState)

    if not resolved.hasInterlockId():
        message = "{} skipped [{}] because no OTTO interlock record was available".format(actionLabel, fleetName)
        message += resolved.duplicateWinnerMessage()
        return _syncIssueResult(
            "interlocks.sync.{}.missing_interlock_id.{}".format(actionKey, fleetName),
            "warn",
            message,
        )
    if not resolved.hasFleetRow():
        message = "{} skipped [{}] because the Fleet interlock row was not found".format(actionLabel, fleetName)
        return _syncIssueResult("interlocks.sync.{}.missing_row.{}".format(actionKey, fleetName), "warn", message)
    if fleetState is None:
        message = "{} skipped [{}] because Fleet state is unreadable".format(actionLabel, fleetName)
        return _syncIssueResult(
            "interlocks.sync.{}.unreadable_fleet_state.{}".format(actionKey, fleetName),
            "warn",
            message,
        )
    if targetState is None:
        message = "{} skipped [{}] because PLC state is unreadable".format(actionLabel, fleetName)
        return _syncIssueResult(
            "interlocks.sync.{}.unreadable_plc_state.{}".format(actionKey, fleetName),
            "warn",
            message,
        )

    # During ForceZero override, keep best-effort driving the PLC mirror to zero
    # even when the most recent PLC State read was bad or unavailable.
    if forceZeroActive and plcState != 0:
        writeObservedTagValue(resolved.plc_state_path, 0, label="Interlock ForceZero PLC sync", logger=logger)

    pendingWrite = bool(readOptionalTagValue(resolved.pendingPath("PendingWriteToFleet"), False))
    pendingWriteState = _toIntOrNone(readOptionalTagValue(resolved.pendingPath("PendingWriteState"), 0))
    pendingWriteStartedMs = _toIntOrNone(readOptionalTagValue(resolved.pendingPath("PendingWriteStartedMs"), 0)) or 0
    lastWriteAttemptMs = _toIntOrNone(readOptionalTagValue(resolved.pendingPath("LastWriteAttemptMs"), 0)) or 0

    if targetState == resolved.remoteState():
        _clearPendingState(resolved.fleet_row_path, logger)
        return TypedOperationResult(
            True,
            "info",
            "{} no-op [{}]; target already matches Fleet".format(actionLabel, fleetName),
            sharedFields={"issues": []},
        )

    if pendingWrite and pendingWriteState == targetState and (nowEpochMs - lastWriteAttemptMs) < retryMs:
        return TypedOperationResult(
            True,
            "info",
            "{} waiting [{}] for retry backoff".format(actionLabel, fleetName),
            sharedFields={"issues": []},
        )

    if not pendingWrite or pendingWriteState != targetState:
        pendingWriteStartedMs = nowEpochMs

    postResult = postInterlockState(
        getOttoOperationsUrl(),
        resolved.interlockId(),
        targetState,
        mask=65535,
        postFunc=httpPost,
    )
    _writePendingState(
        resolved.fleet_row_path,
        True,
        targetState,
        pendingWriteStartedMs,
        nowEpochMs,
        targetState,
        nowEpochMs,
        logger,
    )
    message = postResult.message or "{} posted [{}]".format(actionLabel, fleetName)
    issues = []
    if not postResult.ok:
        issues = _syncIssueResult(
            "interlocks.sync.{}.post_failed.{}".format(actionKey, fleetName),
            postResult.level,
            message,
            postResult=postResult,
        ).issues
    return TypedOperationResult(
        bool(postResult.ok),
        postResult.level,
        message,
        typedFields={"post_result": postResult},
        sharedFields={"issues": issues},
    )


def updateInterlocks():
    """
    Full OTTO interlock read/mirror/sync pass.
    """
    logger = _log()

    getResult = fetchInterlocks(getApiBaseUrl(), httpGet)
    if not getResult.ok and str(getResult.level) == "error":
        return getResult

    records = list(getResult.records or [])
    recordsByName = getResult.records_by_name or {}
    instanceNameByRawName = getResult.instance_name_by_name or {}

    applyResult = applyInterlockSync(
        records,
        instanceNameByRawName,
        logger
    )
    mappingState = readInterlockMappings()

    warnings = []
    issues = []
    _extendWarningsAndIssues(warnings, issues, getResult)
    _extendWarningsAndIssues(warnings, issues, mappingState)
    duplicateInfoByName = mappingState.duplicate_info_by_name or {}
    mappingRows = list(mappingState.rows or [])
    plcSnapshotByTagName = _readPlcSnapshot(mappingRows)
    resolvedRows = [
        _coerceResolvedSyncRow(
            row,
            recordsByName,
            instanceNameByRawName,
            duplicateInfoByName,
            plcSnapshotByTagName,
        )
        for row in list(mappingRows or [])
    ]
    if not applyResult.ok:
        warnings.append(applyResult.message)
        issues.append(buildRuntimeIssue(
            "interlocks.sync.apply_result",
            "Otto_API.Interlocks.Sync",
            applyResult.level,
            applyResult.message,
        ))

    directionalResults = []
    for resolved in resolvedRows:
        directionalResult = _runDirectionalSync(resolved, logger)
        if directionalResult is not None:
            directionalResults.append(directionalResult)

    for directionalResult in list(directionalResults or []):
        _collectDirectionalResult(warnings, issues, directionalResult, logger)

    hasError = False
    for result in [getResult, applyResult, mappingState] + list(directionalResults or []):
        if str(result.level or "").lower() == "error":
            hasError = True
            break

    ok = (not hasError) and (not warnings)
    level = "info"
    if hasError:
        level = "error"
    elif warnings:
        level = "warn"

    message = "Interlocks sync completed"
    if warnings:
        message = "Interlocks sync completed with {} issue(s)".format(len(warnings))
    if hasError:
        message = "Interlocks sync failed"

    return TypedOperationResult(
        ok,
        level,
        message,
        typedFields={
            "get_result": getResult,
            "apply_result": applyResult,
            "mapping_result": mappingState,
            "directional_results": directionalResults,
        },
        sharedFields={
            "warnings": warnings,
            "issues": issues,
        },
    )
