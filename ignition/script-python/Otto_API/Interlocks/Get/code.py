from Otto_API.Common.SyncHelpers import buildSyncResult
from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseJsonResponse
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Common.TagIO import writeLastSystemResponse
from Otto_API.Interlocks.Helpers import buildInterlockInstanceMap


INTERLOCK_FETCH_LIMIT = 100


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Get")


def _normalizeInterlockRecord(rawRecord, warnings, logger):
    rawRecord = dict(rawRecord or {})
    interlockId = str(rawRecord.get("id") or "").strip()
    name = str(rawRecord.get("name") or "").strip()
    created = str(rawRecord.get("created") or "").strip()
    rawState = rawRecord.get("state", 0)

    if not interlockId or not name:
        warnings.append("Interlocks response row missing id or name; skipping row")
        return None

    try:
        state = int(rawState)
    except Exception:
        warnings.append(
            "Interlock [{}] has non-numeric state [{}]; defaulting to 0".format(
                name,
                rawState,
            )
        )
        state = 0

    return {
        "id": interlockId,
        "created": created,
        "name": name,
        "state": state,
    }


def getInterlocks():
    """
    Fetch and normalize OTTO interlocks keyed by interlock name.
    Duplicate names are allowed but later rows win with warnings.
    """
    logger = _log()
    url = getApiBaseUrl().rstrip("/") + "/interlocks/?fields=%2A&offset=0&limit={}".format(INTERLOCK_FETCH_LIMIT)

    logger.info("Otto API - Updating /Interlocks/")

    try:
        response = httpGet(url=url, headerValues=jsonHeaders())
        writeLastSystemResponse(response)
        if not response:
            logger.error("Otto API - HTTP GET failed for /Interlocks/")
            return buildSyncResult(
                False,
                "error",
                "HTTP GET failed for /Interlocks/",
                warnings=[],
                records_by_name={},
            )

        payload = parseJsonResponse(response)
        data = payload
        reportedCount = None
        if isinstance(payload, dict):
            data = payload.get("results", [])
            reportedCount = payload.get("count")

        if not isinstance(data, list):
            logger.error("Otto API - Interlocks JSON decode error: results was not a list")
            return buildSyncResult(
                False,
                "error",
                "Interlocks JSON decode error - results was not a list",
                warnings=[],
                records_by_name={},
            )

        warnings = []
        if reportedCount is not None:
            try:
                reportedCount = int(reportedCount)
            except Exception:
                warnings.append("Interlocks response count [{}] is not numeric".format(reportedCount))
                reportedCount = None

        if reportedCount is not None and reportedCount > INTERLOCK_FETCH_LIMIT:
            warnings.append(
                "Interlocks response count [{}] exceeds supported limit [{}]; results are truncated".format(
                    reportedCount,
                    INTERLOCK_FETCH_LIMIT,
                )
            )

        recordsByName = {}

        for rawRecord in list(data or []):
            normalized = _normalizeInterlockRecord(rawRecord, warnings, logger)
            if normalized is None:
                continue

            name = normalized["name"]
            if name in recordsByName:
                warnings.append(
                    "Interlocks response duplicates name [{}]; using the later row".format(
                        name
                    )
                )
            recordsByName[name] = normalized

        records = [recordsByName[name] for name in sorted(recordsByName.keys())]
        instanceNameByRawName, _rawNameByInstanceName, instanceErrors = buildInterlockInstanceMap(records)
        if instanceErrors:
            for errorText in list(instanceErrors or []):
                logger.error("Otto API - " + str(errorText))
            return buildSyncResult(
                False,
                "error",
                "Interlock instance-name collision detected",
                records=records,
                response_text=response,
                warnings=warnings,
                records_by_name=recordsByName,
                instance_name_by_name=instanceNameByRawName,
                errors=instanceErrors,
            )

        ok = not warnings
        level = "info" if ok else "warn"
        message = "Fetched {} interlock(s)".format(len(records))
        if warnings:
            message = "Fetched {} interlock(s) with {} warning(s)".format(
                len(records),
                len(warnings),
            )
            for warning in list(warnings or []):
                logger.warn("Otto API - " + str(warning))

        return buildSyncResult(
            ok,
            level,
            message,
            records=records,
            response_text=response,
            warnings=warnings,
            records_by_name=recordsByName,
            instance_name_by_name=instanceNameByRawName,
        )
    except Exception as exc:
        logger.error("Otto API - /Interlocks/ fetch failed - " + str(exc))
        return buildSyncResult(
            False,
            "error",
            "Interlocks fetch failed - " + str(exc),
            warnings=[],
            records_by_name={},
        )
