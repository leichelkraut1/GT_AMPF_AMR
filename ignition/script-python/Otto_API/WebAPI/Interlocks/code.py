import time

from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.JsonRpc import postJsonRpcPayload
from Otto_API.Common.ParseHelpers import parseJsonResponse
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Models.Interlocks import buildInterlockInstanceMap
from Otto_API.Models.Interlocks import InterlockRecord
from Otto_API.Models.Results import OperationalResult
from Otto_API.Models.Results import RecordSyncResult


INTERLOCK_FETCH_LIMIT = 100
ISSUE_SOURCE = "Otto_API.WebAPI.Interlocks"


class InterlockFetchResult(RecordSyncResult):
    def __init__(
        self,
        ok,
        level,
        message,
        records=None,
        recordsByName=None,
        responseText=None,
        warnings=None,
        issues=None,
        instanceNameByRawName=None,
        errors=None,
    ):
        self.warnings = list(warnings or [])
        self.issues = list(issues or [])
        self.response_text = responseText
        self.instance_name_by_name = dict(instanceNameByRawName or {})
        self.errors = list(errors or [])

        sharedFields = {}
        sharedFields["warnings"] = self.warnings
        sharedFields["issues"] = self.issues
        if responseText is not None:
            sharedFields["response_text"] = self.response_text
        if instanceNameByRawName is not None:
            sharedFields["instance_name_by_name"] = self.instance_name_by_name
        if errors is not None:
            sharedFields["errors"] = self.errors

        RecordSyncResult.__init__(
            self,
            ok,
            level,
            message,
            records=records,
            recordsByName=recordsByName,
            sharedFields=sharedFields,
        )


def _log():
    return system.util.getLogger("Otto_API.WebAPI.Interlocks")


def _normalizeInterlockRecord(rawRecord, warnings, issues):
    rawRecord = dict(rawRecord or {})
    interlockId = str(rawRecord.get("id") or "").strip()
    name = str(rawRecord.get("name") or "").strip()
    created = str(rawRecord.get("created") or "").strip()
    rawState = rawRecord.get("state", 0)

    if not interlockId or not name:
        warning = "Interlocks response row missing id or name; skipping row"
        warnings.append(warning)
        issues.append(buildRuntimeIssue(
            "interlocks.get.missing_id_or_name",
            ISSUE_SOURCE,
            "warn",
            warning,
        ))
        return None

    try:
        state = int(rawState)
    except Exception:
        warning = "Interlock [{}] has non-numeric state [{}]; defaulting to 0".format(
            name,
            rawState,
        )
        warnings.append(warning)
        issues.append(buildRuntimeIssue(
            "interlocks.get.non_numeric_state.{}".format(name),
            ISSUE_SOURCE,
            "warn",
            warning,
        ))
        state = 0

    return InterlockRecord(interlockId, created, name, state)


def fetchInterlocks(apiBaseUrl, getFunc=httpGet):
    """
    Fetch and normalize OTTO interlocks keyed by interlock name.
    Duplicate names are allowed but later rows win with warnings.
    """
    logger = _log()
    url = "{}/interlocks/?fields=%2A&offset=0&limit={}".format(
        str(apiBaseUrl or "").strip().rstrip("/"),
        INTERLOCK_FETCH_LIMIT,
    )

    try:
        response = getFunc(url=url, headerValues=jsonHeaders())
        if not response:
            logger.error("Otto API - HTTP GET failed for /Interlocks/")
            return InterlockFetchResult(
                False,
                "error",
                "HTTP GET failed for /Interlocks/",
                records=[],
                recordsByName={},
                issues=[
                    buildRuntimeIssue(
                        "interlocks.get.http_get_failed",
                        ISSUE_SOURCE,
                        "error",
                        "HTTP GET failed for /Interlocks/",
                    )
                ],
            )

        payload = parseJsonResponse(response)
        data = payload
        reportedCount = None
        if isinstance(payload, dict):
            data = payload.get("results", [])
            reportedCount = payload.get("count")

        if not isinstance(data, list):
            logger.error("Otto API - Interlocks JSON decode error: results was not a list")
            return InterlockFetchResult(
                False,
                "error",
                "Interlocks JSON decode error - results was not a list",
                records=[],
                recordsByName={},
                issues=[
                    buildRuntimeIssue(
                        "interlocks.get.results_not_list",
                        ISSUE_SOURCE,
                        "error",
                        "Interlocks JSON decode error - results was not a list",
                    )
                ],
            )

        warnings = []
        issues = []
        if reportedCount is not None:
            try:
                reportedCount = int(reportedCount)
            except Exception:
                warning = "Interlocks response count [{}] is not numeric".format(reportedCount)
                warnings.append(warning)
                issues.append(buildRuntimeIssue(
                    "interlocks.get.non_numeric_count",
                    ISSUE_SOURCE,
                    "warn",
                    warning,
                ))
                reportedCount = None

        if reportedCount is not None and reportedCount > INTERLOCK_FETCH_LIMIT:
            warning = "Interlocks response count [{}] exceeds supported limit [{}]; results are truncated".format(
                reportedCount,
                INTERLOCK_FETCH_LIMIT,
            )
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "interlocks.get.limit_exceeded",
                ISSUE_SOURCE,
                "warn",
                warning,
            ))

        recordsByName = {}

        for rawRecord in list(data or []):
            normalized = _normalizeInterlockRecord(rawRecord, warnings, issues)
            if normalized is None:
                continue

            name = normalized.name
            if name in recordsByName:
                warning = "Interlocks response duplicates name [{}]; using the later row".format(name)
                warnings.append(warning)
                issues.append(buildRuntimeIssue(
                    "interlocks.get.duplicate_name.{}".format(name),
                    ISSUE_SOURCE,
                    "warn",
                    warning,
                ))
            recordsByName[name] = normalized

        records = [recordsByName[name] for name in sorted(recordsByName.keys())]
        instanceNameByRawName, _rawNameByInstanceName, instanceErrors = buildInterlockInstanceMap(records)
        if instanceErrors:
            errorIssues = []
            for errorText in list(instanceErrors or []):
                logger.error("Otto API - " + str(errorText))
                errorIssues.append(buildRuntimeIssue(
                    "interlocks.get.instance_collision",
                    ISSUE_SOURCE,
                    "error",
                    str(errorText),
                ))
            return InterlockFetchResult(
                False,
                "error",
                "Interlock instance-name collision detected",
                records=records,
                recordsByName=recordsByName,
                responseText=response,
                warnings=warnings,
                instanceNameByRawName=instanceNameByRawName,
                errors=instanceErrors,
                issues=list(issues or []) + errorIssues,
            )

        ok = not warnings
        level = "info" if ok else "warn"
        message = "Fetched {} interlock(s)".format(len(records))
        if warnings:
            message = "Fetched {} interlock(s) with {} warning(s)".format(
                len(records),
                len(warnings),
            )

        return InterlockFetchResult(
            ok,
            level,
            message,
            records=records,
            recordsByName=recordsByName,
            responseText=response,
            warnings=warnings,
            instanceNameByRawName=instanceNameByRawName,
            issues=issues,
        )
    except Exception as exc:
        logger.error("Otto API - /Interlocks/ fetch failed - " + str(exc))
        return InterlockFetchResult(
            False,
            "error",
            "Interlocks fetch failed - " + str(exc),
            records=[],
            recordsByName={},
            issues=[
                buildRuntimeIssue(
                    "interlocks.get.fetch_exception",
                    ISSUE_SOURCE,
                    "error",
                    "Interlocks fetch failed - " + str(exc),
                )
            ],
        )


def _buildRpcPayload(interlockId, state, mask):
    return {
        "id": int(time.time() * 1000),
        "jsonrpc": "2.0",
        "method": "setInterlockState",
        "params": {
            "id": str(interlockId),
            "mask": int(mask),
            "state": int(state),
        },
    }


def postInterlockState(
    operationsUrl,
    interlockId,
    state,
    mask=65535,
    postFunc=httpPost,
):
    """
    Set one OTTO interlock state through the shared operations endpoint.
    """
    if not interlockId:
        return OperationalResult(
            False,
            "warn",
            "No interlock id supplied for setInterlockState",
            dataFields={
                "interlock_id": interlockId,
                "state": state,
                "mask": mask,
            },
        )

    try:
        payload = _buildRpcPayload(interlockId, state, mask)
        response = postJsonRpcPayload(operationsUrl, payload, postFunc)
        responsePayload = system.util.jsonDecode(response)
        if isinstance(responsePayload, dict) and responsePayload.get("error") is not None:
            errorText = responsePayload.get("error")
            return OperationalResult(
                False,
                "error",
                "setInterlockState failed for [{}]: {}".format(interlockId, errorText),
                dataFields={
                    "interlock_id": interlockId,
                    "state": int(state),
                    "mask": int(mask),
                    "response_text": response,
                    "payload": payload,
                },
            )

        return OperationalResult(
            True,
            "info",
            "setInterlockState queued for [{}] -> {}".format(interlockId, int(state)),
            dataFields={
                "interlock_id": interlockId,
                "state": int(state),
                "mask": int(mask),
                "response_text": response,
                "payload": payload,
            },
        )
    except Exception as exc:
        return OperationalResult(
            False,
            "error",
            "setInterlockState failed for [{}]: {}".format(interlockId, exc),
            dataFields={
                "interlock_id": interlockId,
                "state": state,
                "mask": mask,
            },
        )
