import time

from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.RuntimeHistory import recordRuntimeIssues
from Otto_API.Common.RuntimeHistory import timestampString
from Otto_API.Common.RuntimeHistory import writeRuntimeFields
from Otto_API.Common.TagIO import getOttoOperationsUrl
from Otto_API.Common.TagIO import readOptionalTagValue
from Otto_API.Common.TagIO import readOptionalTagValues
from Otto_API.Common.TagIO import writeObservedTagValue
from Otto_API.Common.TagIO import writeObservedTagValues
from Otto_API.Common.TagPaths import getInterlockWritebackRetryMsPath
from Otto_API.Common.TagPaths import getPlcInterlocksPath
from Otto_API.Models.Interlocks import InterlockMappingRow
from Otto_API.Models.Interlocks import PlcInterlockSnapshot
from Otto_API.Services.Interlocks.FleetSync import syncFleetInterlocks
from Otto_API.Services.Interlocks.Helpers import coerceResolvedSyncRow
from Otto_API.Services.Interlocks.Helpers import extendIssues
from Otto_API.Services.Interlocks.Helpers import extendWarningsAndIssues
from Otto_API.Services.Interlocks.Helpers import InterlockDirectionalResult
from Otto_API.Services.Interlocks.Helpers import InterlockRuntimeResult
from Otto_API.Services.Interlocks.Helpers import InterlockSyncResult
from Otto_API.Services.Interlocks.Helpers import messageFromResult
from Otto_API.Services.Interlocks.Helpers import SERVICE_SOURCE
from Otto_API.Services.Interlocks.Helpers import statusFromResult
from Otto_API.Services.Interlocks.Helpers import syncIssue
from Otto_API.Services.Interlocks.Helpers import syncIssueResult
from Otto_API.Services.Interlocks.Helpers import toIntOrNone
from Otto_API.TagSync.Interlocks.Mapping import DEFAULT_INTERLOCK_WRITEBACK_RETRY_MS
from Otto_API.TagSync.Interlocks.Mapping import readInterlockMappings
from Otto_API.WebAPI.Interlocks import InterlockFetchResult
from Otto_API.WebAPI.Interlocks import postInterlockState


def _log():
    return system.util.getLogger("Otto_API.Services.Interlocks")


def _nowEpochMs():
    return int(time.time() * 1000)


def _readRetryMs():
    retryMs = toIntOrNone(
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


def _collectDirectionalResult(warnings, issues, directionalResult, logger):
    if not directionalResult.ok:
        warnings.append(directionalResult.message)
    extendIssues(issues, directionalResult)
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
        return syncIssueResult("interlocks.sync.fromfleet.missing_row.{}".format(fleetName), "warn", message)

    fleetState = toIntOrNone(readOptionalTagValue(resolved.fleet_state_path, None))
    if fleetState is None:
        message = "FromFleet skipped [{}] because Fleet state is unreadable".format(fleetName)
        return syncIssueResult("interlocks.sync.fromfleet.unreadable_state.{}".format(fleetName), "warn", message)

    writeObservedTagValue(resolved.plc_state_path, fleetState, label="Interlock FromFleet sync", logger=logger)
    return InterlockDirectionalResult(
        True,
        "info",
        "FromFleet synced [{}] -> [{}]".format(fleetName, resolved.plc_tag_name),
        issues=[],
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
        return InterlockDirectionalResult(
            True,
            "info",
            "{} disabled [{}] because WriteEnable is false".format(actionLabel, fleetName),
            issues=[],
        )

    fleetState = resolved.fleetState()
    plcState = resolved.targetState(plcStateOverride=plcStateOverride)
    if plcStateOverride is None and desiredState is None:
        plcState = toIntOrNone(readOptionalTagValue(resolved.plc_state_path, None))
    nowEpochMs = _nowEpochMs()
    retryMs = _readRetryMs()
    targetState = resolved.targetState(desiredState=desiredState, plcStateOverride=plcState)

    if not resolved.hasInterlockId():
        message = "{} skipped [{}] because no OTTO interlock record was available".format(actionLabel, fleetName)
        message += resolved.duplicateWinnerMessage()
        return syncIssueResult(
            "interlocks.sync.{}.missing_interlock_id.{}".format(actionKey, fleetName),
            "warn",
            message,
        )
    if not resolved.hasFleetRow():
        message = "{} skipped [{}] because the Fleet interlock row was not found".format(actionLabel, fleetName)
        return syncIssueResult("interlocks.sync.{}.missing_row.{}".format(actionKey, fleetName), "warn", message)
    if fleetState is None:
        message = "{} skipped [{}] because Fleet state is unreadable".format(actionLabel, fleetName)
        return syncIssueResult(
            "interlocks.sync.{}.unreadable_fleet_state.{}".format(actionKey, fleetName),
            "warn",
            message,
        )
    if targetState is None:
        message = "{} skipped [{}] because PLC state is unreadable".format(actionLabel, fleetName)
        return syncIssueResult(
            "interlocks.sync.{}.unreadable_plc_state.{}".format(actionKey, fleetName),
            "warn",
            message,
        )

    # During ForceZero override, keep best-effort driving the PLC mirror to zero
    # even when the most recent PLC State read was bad or unavailable.
    if forceZeroActive and plcState != 0:
        writeObservedTagValue(resolved.plc_state_path, 0, label="Interlock ForceZero PLC sync", logger=logger)

    pendingWrite = bool(readOptionalTagValue(resolved.pendingPath("PendingWriteToFleet"), False))
    pendingWriteState = toIntOrNone(readOptionalTagValue(resolved.pendingPath("PendingWriteState"), 0))
    pendingWriteStartedMs = toIntOrNone(readOptionalTagValue(resolved.pendingPath("PendingWriteStartedMs"), 0)) or 0
    lastWriteAttemptMs = toIntOrNone(readOptionalTagValue(resolved.pendingPath("LastWriteAttemptMs"), 0)) or 0

    if targetState == resolved.remoteState():
        _clearPendingState(resolved.fleet_row_path, logger)
        return InterlockDirectionalResult(
            True,
            "info",
            "{} no-op [{}]; target already matches Fleet".format(actionLabel, fleetName),
            issues=[],
        )

    if pendingWrite and pendingWriteState == targetState and (nowEpochMs - lastWriteAttemptMs) < retryMs:
        return InterlockDirectionalResult(
            True,
            "info",
            "{} waiting [{}] for retry backoff".format(actionLabel, fleetName),
            issues=[],
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
        issues = [syncIssue(
            "interlocks.sync.{}.post_failed.{}".format(actionKey, fleetName),
            postResult.level,
            message,
        )]
    return InterlockDirectionalResult(
        bool(postResult.ok),
        postResult.level,
        message,
        postResult=postResult,
        issues=issues,
    )


def updateInterlocks():
    """
    Full OTTO interlock read/mirror/sync pass.
    """
    logger = _log()

    fleetSyncResult = syncFleetInterlocks(logger=logger)
    getResult = fleetSyncResult.get_result
    applyResult = fleetSyncResult.apply_result
    if not isinstance(getResult, InterlockFetchResult):
        return InterlockRuntimeResult(
            False,
            "error",
            "Interlocks sync failed before fetch result was available",
            issues=[
                buildRuntimeIssue(
                    "interlocks.sync.missing_fetch_result",
                    SERVICE_SOURCE,
                    "error",
                    "Interlocks sync failed before fetch result was available",
                )
            ],
        )
    if getResult is not None and not getResult.ok and str(getResult.level) == "error":
        return getResult

    recordsByName = getResult.records_by_name or {}
    instanceNameByRawName = getResult.instance_name_by_name or {}
    mappingState = readInterlockMappings()

    warnings = []
    issues = []
    extendWarningsAndIssues(warnings, issues, getResult)
    extendWarningsAndIssues(warnings, issues, mappingState)
    duplicateInfoByName = mappingState.duplicate_info_by_name or {}
    mappingRows = list(mappingState.rows or [])
    plcSnapshotByTagName = _readPlcSnapshot(mappingRows)
    resolvedRows = [
        coerceResolvedSyncRow(
            row,
            recordsByName,
            instanceNameByRawName,
            duplicateInfoByName,
            plcSnapshotByTagName,
        )
        for row in list(mappingRows or [])
    ]
    if applyResult is not None and not applyResult.ok:
        warnings.append(applyResult.message)
        issues.append(buildRuntimeIssue(
            "interlocks.sync.apply_result",
            SERVICE_SOURCE,
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
        if result is not None and str(result.level or "").lower() == "error":
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

    return InterlockSyncResult(
        ok,
        level,
        message,
        getResult=getResult,
        applyResult=applyResult,
        mappingResult=mappingState,
        directionalResults=directionalResults,
        warnings=warnings,
        issues=issues,
    )


def runInterlockSyncCycle(nowEpochMs=None):
    """
    Run one interlock sync pass while updating shared MainControl/Runtime telemetry.
    """
    logger = _log()
    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    startEpochMs = int(nowEpochMs)
    result = None

    try:
        writeRuntimeFields({
            "interlock_sync_is_running": True,
            "interlock_sync_last_start_ts": timestampString(startEpochMs),
        })
    except Exception as exc:
        logger.error("Interlock sync runtime start telemetry failed: {}".format(str(exc)))

    try:
        result = updateInterlocks()
    except Exception as exc:
        message = "Interlock sync runtime wrapper failed: {}".format(str(exc))
        logger.error(message)
        result = InterlockRuntimeResult(
            False,
            "error",
            message,
            issues=[
                buildRuntimeIssue(
                    "interlocks.runtime.wrapper_exception",
                    SERVICE_SOURCE,
                    "error",
                    message,
                )
            ],
        )
    finally:
        endEpochMs = int(time.time() * 1000)
        durationMs = max(0, endEpochMs - startEpochMs)
        status = statusFromResult(result)
        message = messageFromResult(result)
        lastResult = "unknown"
        if result is not None:
            lastResult = str(result.level or "info") + ":" + str(result.message or "")

        recordRuntimeIssues([result], nowEpochMs=endEpochMs, logger=logger)

        try:
            writeRuntimeFields({
                "interlock_sync_is_running": False,
                "interlock_sync_last_end_ts": timestampString(endEpochMs),
                "interlock_sync_last_duration_ms": durationMs,
                "interlock_sync_last_result": lastResult,
                "interlock_sync_status": status,
                "interlock_sync_message": message,
            })
        except Exception as exc:
            logger.error("Interlock sync runtime end telemetry failed: {}".format(str(exc)))

    if result is None:
        result = InterlockRuntimeResult(
            False,
            "error",
            "Interlock sync did not produce a result",
            issues=[
                buildRuntimeIssue(
                    "interlocks.runtime.missing_result",
                    SERVICE_SOURCE,
                    "error",
                    "Interlock sync did not produce a result",
                )
            ],
        )

    return result.toDict()
