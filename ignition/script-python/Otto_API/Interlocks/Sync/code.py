import time

from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readOptionalTagValues
from Otto_API.Common.TagIO import writeObservedTagValue
from Otto_API.Common.TagPaths import getFleetInterlocksPath
from Otto_API.Common.TagPaths import getInterlockWritebackRetryMsPath
from Otto_API.Common.TagPaths import getPlcInterlocksPath
from Otto_API.Interlocks.Apply import applyInterlockSync
from Otto_API.Interlocks.Get import getInterlocks
from Otto_API.Interlocks.Mapping import DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS
from Otto_API.Interlocks.Mapping import readInterlockMappings
from Otto_API.Interlocks.Post import setInterlockState
from Otto_API.Interlocks.Records import DuplicateInterlockMappingInfo
from Otto_API.Interlocks.Records import InterlockMappingRow
from Otto_API.Interlocks.Records import InterlockRecord
from Otto_API.Interlocks.Records import PlcInterlockSnapshot


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


def _coerceMappingRow(row):
    if isinstance(row, InterlockMappingRow):
        return row
    return InterlockMappingRow.fromDict(row)


def _coerceInterlockRecord(record):
    if isinstance(record, InterlockRecord):
        return record
    return InterlockRecord.fromDict(record)


def _coerceDuplicateInfo(duplicateInfo):
    if isinstance(duplicateInfo, DuplicateInterlockMappingInfo):
        return duplicateInfo
    return DuplicateInterlockMappingInfo.fromDict(duplicateInfo)


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


def _writePendingState(rowPath, pendingWrite, pendingState, pendingStartedMs, lastWriteAttemptMs, lastCommandedState, lastCommandedMs, logger):
    writeObservedTagValue(rowPath + "/PendingWriteToFleet", bool(pendingWrite), label="Interlock pending-write sync", logger=logger)
    writeObservedTagValue(rowPath + "/PendingWriteState", int(pendingState or 0), label="Interlock pending-write sync", logger=logger)
    writeObservedTagValue(rowPath + "/PendingWriteStartedMs", int(pendingStartedMs or 0), label="Interlock pending-write sync", logger=logger)
    writeObservedTagValue(rowPath + "/LastWriteAttemptMs", int(lastWriteAttemptMs or 0), label="Interlock pending-write sync", logger=logger)
    writeObservedTagValue(rowPath + "/LastCommandedState", int(lastCommandedState or 0), label="Interlock pending-write sync", logger=logger)
    writeObservedTagValue(rowPath + "/LastCommandedMs", int(lastCommandedMs or 0), label="Interlock pending-write sync", logger=logger)


def _clearPendingState(rowPath, logger):
    writeObservedTagValue(rowPath + "/PendingWriteToFleet", False, label="Interlock pending-write sync", logger=logger)
    writeObservedTagValue(rowPath + "/PendingWriteState", 0, label="Interlock pending-write sync", logger=logger)
    writeObservedTagValue(rowPath + "/PendingWriteStartedMs", 0, label="Interlock pending-write sync", logger=logger)


def _readPlcSnapshot(rows):
    snapshotByPlcTagName = {}
    plcTagNames = []
    tagPaths = []
    defaultValues = []

    for row in list(rows or []):
        row = _coerceMappingRow(row)
        plcTagName = str(row.get("PlcTagName") or "").strip()
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


def _recordObjectsByName(recordsByName):
    recordObjectsByName = {}
    for fleetName, recordValue in list(dict(recordsByName or {}).items()):
        recordObjectsByName[fleetName] = _coerceInterlockRecord(recordValue)
    return recordObjectsByName


def _duplicateInfoObjectsByName(duplicateInfoByName):
    duplicateInfoObjectsByName = {}
    for fleetName, duplicateInfoValue in list(dict(duplicateInfoByName or {}).items()):
        duplicateInfoObjectsByName[fleetName] = _coerceDuplicateInfo(duplicateInfoValue)
    return duplicateInfoObjectsByName


def _mappingRowObjects(rows):
    return [_coerceMappingRow(row) for row in list(rows or [])]


def _extendWarningsAndIssues(targetWarnings, targetIssues, result):
    targetWarnings.extend(list((result or {}).get("warnings") or []))
    targetIssues.extend(list((result or {}).get("issues") or []))


def _extendIssues(targetIssues, result):
    targetIssues.extend(list((result or {}).get("issues") or []))


def _applyFromFleet(row, instanceNameByRawName, logger):
    row = _coerceMappingRow(row)
    fleetName = row.get("FleetName")
    plcTagName = row.get("PlcTagName")
    fleetRowPath = _fleetInterlockRowPath(fleetName, instanceNameByRawName)
    if not fleetRowPath:
        message = "FromFleet skipped [{}] because the Fleet interlock row was not found".format(fleetName)
        return {
            "ok": False,
            "level": "warn",
            "message": message,
            "issues": [
                buildRuntimeIssue(
                    "interlocks.sync.fromfleet.missing_row.{}".format(fleetName),
                    "Otto_API.Interlocks.Sync",
                    "warn",
                    message,
                )
            ],
        }
    fleetStatePath = fleetRowPath + "/State"
    plcStatePath = getPlcInterlocksPath() + "/" + plcTagName + "/State"
    fleetState = _toIntOrNone(readOptionalTagValue(fleetStatePath, None))
    if fleetState is None:
        message = "FromFleet skipped [{}] because Fleet state is unreadable".format(fleetName)
        return {
            "ok": False,
            "level": "warn",
            "message": message,
            "issues": [
                buildRuntimeIssue(
                    "interlocks.sync.fromfleet.unreadable_state.{}".format(fleetName),
                    "Otto_API.Interlocks.Sync",
                    "warn",
                    message,
                )
            ],
        }

    writeObservedTagValue(plcStatePath, fleetState, label="Interlock FromFleet sync", logger=logger)
    return {
        "ok": True,
        "level": "info",
        "message": "FromFleet synced [{}] -> [{}]".format(fleetName, plcTagName),
        "issues": [],
    }


def _applyToFleet(
    row,
    recordsByName,
    instanceNameByRawName,
    duplicateInfoByName,
    logger,
    desiredState=None,
    actionKey="tofleet",
    actionLabel="ToFleet",
    ignoreWriteEnable=False,
    forceZeroActive=False,
    plcStateOverride=None,
):
    row = _coerceMappingRow(row)
    fleetName = row.get("FleetName")
    plcTagName = row.get("PlcTagName")
    writeEnabled = row.isWritable()
    if not ignoreWriteEnable and not writeEnabled:
        return {
            "ok": True,
            "level": "info",
            "message": "{} disabled [{}] because WriteEnable is false".format(actionLabel, fleetName),
            "issues": [],
        }

    recordValue = recordsByName.get(fleetName)
    record = None if recordValue is None else _coerceInterlockRecord(recordValue)
    interlockId = "" if record is None else str(record.get("id") or "").strip()
    fleetState = None if record is None else _toIntOrNone(record.get("state"))
    fleetRowPath = _fleetInterlockRowPath(fleetName, instanceNameByRawName)
    plcStatePath = getPlcInterlocksPath() + "/" + plcTagName + "/State"
    plcState = plcStateOverride
    if plcState is None and desiredState is None:
        plcState = _toIntOrNone(readOptionalTagValue(plcStatePath, None))
    nowEpochMs = _nowEpochMs()
    retryMs = _readRetryMs()
    targetState = desiredState if desiredState is not None else plcState

    if not interlockId:
        duplicateInfoValue = (duplicateInfoByName or {}).get(fleetName)
        duplicateInfo = None if duplicateInfoValue is None else _coerceDuplicateInfo(duplicateInfoValue)
        message = "{} skipped [{}] because no OTTO interlock record was available".format(actionLabel, fleetName)
        if duplicateInfo is not None:
            message += "; Duplicate Interlock Mapping is also present and the later row [{} / {}] won".format(
                duplicateInfo.get("winning_plc_tag_name"),
                duplicateInfo.get("winning_direction"),
            )
        return {
            "ok": False,
            "level": "warn",
            "message": message,
            "issues": [
                buildRuntimeIssue(
                    "interlocks.sync.{}.missing_interlock_id.{}".format(actionKey, fleetName),
                    "Otto_API.Interlocks.Sync",
                    "warn",
                    message,
                )
            ],
        }
    if not fleetRowPath:
        message = "{} skipped [{}] because the Fleet interlock row was not found".format(actionLabel, fleetName)
        return {
            "ok": False,
            "level": "warn",
            "message": message,
            "issues": [
                buildRuntimeIssue(
                    "interlocks.sync.{}.missing_row.{}".format(actionKey, fleetName),
                    "Otto_API.Interlocks.Sync",
                    "warn",
                    message,
                )
            ],
        }
    if fleetState is None:
        message = "{} skipped [{}] because Fleet state is unreadable".format(actionLabel, fleetName)
        return {
            "ok": False,
            "level": "warn",
            "message": message,
            "issues": [
                buildRuntimeIssue(
                    "interlocks.sync.{}.unreadable_fleet_state.{}".format(actionKey, fleetName),
                    "Otto_API.Interlocks.Sync",
                    "warn",
                    message,
                )
            ],
        }
    if targetState is None:
        message = "{} skipped [{}] because PLC state is unreadable".format(actionLabel, fleetName)
        return {
            "ok": False,
            "level": "warn",
            "message": message,
            "issues": [
                buildRuntimeIssue(
                    "interlocks.sync.{}.unreadable_plc_state.{}".format(actionKey, fleetName),
                    "Otto_API.Interlocks.Sync",
                    "warn",
                    message,
                )
            ],
        }

    # During ForceZero override, keep best-effort driving the PLC mirror to zero
    # even when the most recent PLC State read was bad or unavailable.
    if forceZeroActive and plcState != 0:
        writeObservedTagValue(plcStatePath, 0, label="Interlock ForceZero PLC sync", logger=logger)

    pendingWrite = bool(readOptionalTagValue(fleetRowPath + "/PendingWriteToFleet", False))
    pendingWriteState = _toIntOrNone(readOptionalTagValue(fleetRowPath + "/PendingWriteState", 0))
    pendingWriteStartedMs = _toIntOrNone(readOptionalTagValue(fleetRowPath + "/PendingWriteStartedMs", 0)) or 0
    lastWriteAttemptMs = _toIntOrNone(readOptionalTagValue(fleetRowPath + "/LastWriteAttemptMs", 0)) or 0

    if targetState == fleetState:
        _clearPendingState(fleetRowPath, logger)
        return {
            "ok": True,
            "level": "info",
            "message": "{} no-op [{}]; target already matches Fleet".format(actionLabel, fleetName),
            "issues": [],
        }

    if pendingWrite and pendingWriteState == targetState and (nowEpochMs - lastWriteAttemptMs) < retryMs:
        return {
            "ok": True,
            "level": "info",
            "message": "{} waiting [{}] for retry backoff".format(actionLabel, fleetName),
            "issues": [],
        }

    if not pendingWrite or pendingWriteState != targetState:
        pendingWriteStartedMs = nowEpochMs

    postResult = setInterlockState(interlockId, targetState, mask=65535)
    _writePendingState(
        fleetRowPath,
        True,
        targetState,
        pendingWriteStartedMs,
        nowEpochMs,
        targetState,
        nowEpochMs,
        logger,
    )
    message = postResult.get("message") or "{} posted [{}]".format(actionLabel, fleetName)
    return {
        "ok": bool(postResult.get("ok")),
        "level": postResult.get("level"),
        "message": message,
        "post_result": postResult,
        "issues": [] if bool(postResult.get("ok")) else [
            buildRuntimeIssue(
                "interlocks.sync.{}.post_failed.{}".format(actionKey, fleetName),
                "Otto_API.Interlocks.Sync",
                postResult.get("level"),
                message,
            )
        ],
    }


def updateInterlocks():
    """
    Full OTTO interlock read/mirror/sync pass.
    """
    logger = _log()

    getResult = getInterlocks()
    if not getResult.get("ok") and str(getResult.get("level")) == "error":
        return getResult

    records = list(getResult.get("records") or [])
    recordsByName = _recordObjectsByName(getResult.get("records_by_name") or {})
    instanceNameByRawName = getResult.get("instance_name_by_name") or {}

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
    duplicateInfoByName = _duplicateInfoObjectsByName(mappingState.get("duplicate_info_by_name") or {})
    mappingRows = _mappingRowObjects(mappingState.get("rows") or [])
    plcSnapshotByTagName = _readPlcSnapshot(mappingRows)
    if not applyResult.get("ok"):
        warnings.append(applyResult.get("message"))
        issues.append(buildRuntimeIssue(
            "interlocks.sync.apply_result",
            "Otto_API.Interlocks.Sync",
            applyResult.get("level"),
            applyResult.get("message"),
        ))

    directionalResults = []
    for row in mappingRows:
        row = _coerceMappingRow(row)
        direction = str(row.get("Direction") or "").strip()
        plcTagName = str(row.get("PlcTagName") or "").strip()
        plcSnapshot = plcSnapshotByTagName.get(plcTagName)
        plcState = None if plcSnapshot is None else _toIntOrNone(plcSnapshot.get("state"))
        forceZeroActive = False if plcSnapshot is None else plcSnapshot.forceZeroActive()

        if forceZeroActive:
            directionalResults.append(
                _applyToFleet(
                    row,
                    recordsByName,
                    instanceNameByRawName,
                    duplicateInfoByName,
                    logger,
                    desiredState=0,
                    actionKey="forcezero",
                    actionLabel="ForceZero",
                    ignoreWriteEnable=True,
                    forceZeroActive=True,
                    plcStateOverride=plcState,
                )
            )
        elif direction == "FromFleet":
            directionalResults.append(_applyFromFleet(row, instanceNameByRawName, logger))
        elif direction == "ToFleet":
            directionalResults.append(
                _applyToFleet(
                    row,
                    recordsByName,
                    instanceNameByRawName,
                    duplicateInfoByName,
                    logger,
                    plcStateOverride=plcState,
                )
            )

    for directionalResult in list(directionalResults or []):
        if not directionalResult.get("ok"):
            warnings.append(directionalResult.get("message"))
        _extendIssues(issues, directionalResult)
        level = str(directionalResult.get("level") or "").lower()
        message = str(directionalResult.get("message") or "")
        if level == "error":
            logger.error("Otto API - " + message)

    hasError = False
    for result in [getResult, applyResult, mappingState] + list(directionalResults or []):
        if str(result.get("level") or "").lower() == "error":
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

    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "get_result": getResult,
            "apply_result": applyResult,
            "mapping_result": mappingState,
            "directional_results": directionalResults,
            "warnings": warnings,
            "issues": issues,
        },
        get_result=getResult,
        apply_result=applyResult,
        mapping_result=mappingState,
        directional_results=directionalResults,
        warnings=warnings,
        issues=issues,
    )
