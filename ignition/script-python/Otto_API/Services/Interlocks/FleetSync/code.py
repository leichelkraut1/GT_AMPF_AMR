from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Services.Interlocks.Helpers import extendWarningsAndIssues
from Otto_API.Services.Interlocks.Helpers import InterlockFleetSyncResult
from Otto_API.TagSync.Interlocks.Apply import applyInterlockSync
from Otto_API.WebAPI.Interlocks import fetchInterlocks


def _log():
    return system.util.getLogger("Otto_API.Services.Interlocks")


def syncFleetInterlocks(logger=None):
    """
    Fetch OTTO interlocks and mirror them into Fleet/Interlocks tags.
    """
    logger = logger or _log()
    getResult = fetchInterlocks(getApiBaseUrl(), httpGet)

    warnings = []
    issues = []
    extendWarningsAndIssues(warnings, issues, getResult)

    if not getResult.ok and str(getResult.level) == "error":
        return InterlockFleetSyncResult(
            False,
            getResult.level,
            getResult.message,
            getResult=getResult,
            applyResult=None,
            warnings=warnings,
            issues=issues,
        )

    applyResult = applyInterlockSync(
        list(getResult.records or []),
        getResult.instance_name_by_name or {},
        logger,
    )
    if not applyResult.ok:
        warnings.append(applyResult.message)

    ok = bool(getResult.ok and applyResult.ok)
    level = "info" if ok else "warn"
    message = "Fleet interlock mirror synced"
    if warnings:
        message = "Fleet interlock mirror synced with {} issue(s)".format(len(warnings))

    return InterlockFleetSyncResult(
        ok,
        level,
        message,
        getResult=getResult,
        applyResult=applyResult,
        warnings=warnings,
        issues=issues,
    )
