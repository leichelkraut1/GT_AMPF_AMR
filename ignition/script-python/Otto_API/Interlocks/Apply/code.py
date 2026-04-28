from Otto_API.Common.ResultHelpers import buildTypedOperationResult
from Otto_API.Common.TagIO import deleteTagPath
from Otto_API.Common.TagIO import tagExists
from Otto_API.Common.TagIO import writeObservedTagValues
from Otto_API.Common.TagPaths import getFleetInterlocksPath
from Otto_API.Common.TagProvisioning import ensureMemoryTag
from Otto_API.Common.TagProvisioning import ensureUdtInstancePath
from Otto_API.Interlocks.Helpers import childRowNames


INTERLOCK_FIELD_SPECS = [
    ("ID", "String"),
    ("Created", "String"),
    ("Name", "String"),
    ("State", "Int4"),
]
INTERLOCK_METADATA_FIELD_SPECS = [
    ("LastCommandedState", "Int4", 0),
    ("LastCommandedMs", "Int8", 0),
    ("PendingWriteToFleet", "Boolean", False),
    ("PendingWriteState", "Int4", 0),
    ("PendingWriteStartedMs", "Int8", 0),
    ("LastWriteAttemptMs", "Int8", 0),
]
INTERLOCK_TYPE_ID = "api_Interlock"


def _recordField(record, fieldName, defaultValue=None):
    if isinstance(record, dict):
        return record.get(fieldName, defaultValue)
    return getattr(record, fieldName, defaultValue)


def _ensureInterlockRow(rowPath):
    ensureUdtInstancePath(rowPath, INTERLOCK_TYPE_ID)
    # Keep child members explicit so the local test shim and gateway both expose
    # the expected leaf tags even before real UDT definitions are expanded.
    for fieldName, dataType in list(INTERLOCK_FIELD_SPECS or []):
        initialValue = 0 if dataType == "Int4" else ""
        ensureMemoryTag(rowPath + "/" + fieldName, dataType, initialValue)
    for fieldName, dataType, initialValue in list(INTERLOCK_METADATA_FIELD_SPECS or []):
        ensureMemoryTag(rowPath + "/" + fieldName, dataType, initialValue)


def applyInterlockSync(records, instanceNameByRawName=None, logger=None):
    """
    Mirror normalized interlock rows into Fleet/Interlocks/<Name>.
    """
    logger = logger or system.util.getLogger("Otto_API.Interlocks.Apply")
    basePath = getFleetInterlocksPath()
    if not tagExists(basePath):
        return buildTypedOperationResult(
            False,
            "warn",
            "Fleet interlock root is missing: {}".format(basePath),
            sharedFields={"row_names": []},
        )

    wantedNames = []
    tagPaths = []
    values = []
    labels = []

    for record in list(records or []):
        rawName = str(_recordField(record, "name") or "").strip()
        instanceName = str(dict(instanceNameByRawName or {}).get(rawName) or "").strip()
        if not rawName or not instanceName:
            continue

        wantedNames.append(instanceName)
        rowPath = basePath + "/" + instanceName
        if not tagExists(rowPath):
            _ensureInterlockRow(rowPath)

        for fieldName, _dataType in list(INTERLOCK_FIELD_SPECS or []):
            key = fieldName.lower()
            tagPaths.append(rowPath + "/" + fieldName)
            values.append(_recordField(record, key))
            labels.append("Interlock sync")

    if tagPaths:
        writeObservedTagValues(tagPaths, values, labels=labels, logger=logger)

    wantedNameSet = set(list(wantedNames or []))
    for childName in childRowNames(basePath):
        if childName in wantedNameSet:
            continue
        deleteTagPath(basePath + "/" + childName)
        logger.info("Otto API - Removed stale interlock tag row: " + str(childName))

    return buildTypedOperationResult(
        True,
        "info",
        "Synced {} Fleet interlock row(s)".format(len(wantedNames)),
        sharedFields={"row_names": list(wantedNames)},
    )
