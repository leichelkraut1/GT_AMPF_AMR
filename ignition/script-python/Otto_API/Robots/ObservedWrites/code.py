from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import writeObservedTagValues


def buildRobotSyncResult(ok, level, message, records=None, writes=None, data=None, issues=None):
    records = list(records or [])
    writes = list(writes or [])
    issues = list(issues or [])
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "records": records,
            "writes": writes,
            "value": data,
            "issues": issues,
        },
        records=records,
        writes=writes,
        issues=issues,
    )


def writeObservedPairs(writes, label, logger):
    writePairs = list(writes or [])
    if not writePairs:
        return
    writeObservedTagValues(
        [path for path, _ in writePairs],
        [value for _, value in writePairs],
        labels=[label] * len(writePairs),
        logger=logger
    )
