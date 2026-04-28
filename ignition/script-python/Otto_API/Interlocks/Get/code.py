from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseJsonResponse
from Otto_API.Common.ResultHelpers import buildRecordSyncResult
from Otto_API.Common.RuntimeHistory import buildRuntimeIssue
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Interlocks.Helpers import buildInterlockInstanceMap
from Otto_API.Interlocks.Records import InterlockRecord


INTERLOCK_FETCH_LIMIT = 100


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Get")


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
            "Otto_API.Interlocks.Get",
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
            "Otto_API.Interlocks.Get",
            "warn",
            warning,
        ))
        state = 0

    return InterlockRecord(interlockId, created, name, state)


def getInterlocks():
    """
    Fetch and normalize OTTO interlocks keyed by interlock name.
    Duplicate names are allowed but later rows win with warnings.
    """
    logger = _log()
    url = getApiBaseUrl().rstrip("/") + "/interlocks/?fields=%2A&offset=0&limit={}".format(INTERLOCK_FETCH_LIMIT)

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        if not response:
            logger.error("Otto API - HTTP GET failed for /Interlocks/")
            return buildRecordSyncResult(
                False,
                "error",
                "HTTP GET failed for /Interlocks/",
                records=[],
                recordsByName={},
                sharedFields={
                    "warnings": [],
                    "issues": [
                        buildRuntimeIssue(
                            "interlocks.get.http_get_failed",
                            "Otto_API.Interlocks.Get",
                            "error",
                            "HTTP GET failed for /Interlocks/",
                        )
                    ],
                },
            )

        payload = parseJsonResponse(response)
        data = payload
        reportedCount = None
        if isinstance(payload, dict):
            data = payload.get("results", [])
            reportedCount = payload.get("count")

        if not isinstance(data, list):
            logger.error("Otto API - Interlocks JSON decode error: results was not a list")
            return buildRecordSyncResult(
                False,
                "error",
                "Interlocks JSON decode error - results was not a list",
                records=[],
                recordsByName={},
                sharedFields={
                    "warnings": [],
                    "issues": [
                        buildRuntimeIssue(
                            "interlocks.get.results_not_list",
                            "Otto_API.Interlocks.Get",
                            "error",
                            "Interlocks JSON decode error - results was not a list",
                        )
                    ],
                },
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
                    "Otto_API.Interlocks.Get",
                    "warn",
                    warning,
                ))
                reportedCount = None

        # Warn here because "count" is the total matching interlock count across OTTO,
        # while this request only asks for the first page up to INTERLOCK_FETCH_LIMIT.
        # When count exceeds the requested limit, more matching records may exist than
        # what we actually fetched in this response.
        if reportedCount is not None and reportedCount > INTERLOCK_FETCH_LIMIT:
            warning = "Interlocks response count [{}] exceeds supported limit [{}]; results are truncated".format(
                reportedCount,
                INTERLOCK_FETCH_LIMIT,
            )
            warnings.append(warning)
            issues.append(buildRuntimeIssue(
                "interlocks.get.limit_exceeded",
                "Otto_API.Interlocks.Get",
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
                    "Otto_API.Interlocks.Get",
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
                    "Otto_API.Interlocks.Get",
                    "error",
                    str(errorText),
                ))
            return buildRecordSyncResult(
                False,
                "error",
                "Interlock instance-name collision detected",
                records=records,
                recordsByName=recordsByName,
                sharedFields={
                    "response_text": response,
                    "warnings": warnings,
                    "instance_name_by_name": instanceNameByRawName,
                    "errors": instanceErrors,
                    "issues": list(issues or []) + errorIssues,
                },
            )

        ok = not warnings
        level = "info" if ok else "warn"
        message = "Fetched {} interlock(s)".format(len(records))
        if warnings:
            message = "Fetched {} interlock(s) with {} warning(s)".format(
                len(records),
                len(warnings),
            )

        return buildRecordSyncResult(
            ok,
            level,
            message,
            records=records,
            recordsByName=recordsByName,
            sharedFields={
                "response_text": response,
                "warnings": warnings,
                "instance_name_by_name": instanceNameByRawName,
                "issues": issues,
            },
        )
    except Exception as exc:
        logger.error("Otto API - /Interlocks/ fetch failed - " + str(exc))
        return buildRecordSyncResult(
            False,
            "error",
            "Interlocks fetch failed - " + str(exc),
            records=[],
            recordsByName={},
            sharedFields={
                "warnings": [],
                "issues": [
                    buildRuntimeIssue(
                        "interlocks.get.fetch_exception",
                        "Otto_API.Interlocks.Get",
                        "error",
                        "Interlocks fetch failed - " + str(exc),
                    )
                ],
            },
        )
