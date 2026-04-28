from Otto_API.Common.RecordHelpers import coerceBool
from Otto_API.Common.SyncHelpers import sanitizeTagName
from Otto_API.Common.TagIO import browseTagResults


def buildInterlockInstanceName(rawName):
    """
    Convert an OTTO interlock name into a safe Ignition tag instance name.
    """
    return sanitizeTagName(rawName)


def normalizeBool(value, defaultValue=False):
    return coerceBool(value, defaultValue)


def childRowNames(basePath):
    """
    Return child folder or UDT-instance names under one collection path.
    """
    names = []
    for row in list(browseTagResults(basePath) or []):
        tagType = str(row.get("tagType") or "").strip().lower()
        if tagType in ["folder", "udtinstance"]:
            names.append(str(row.get("name") or ""))
    return sorted(names)


def buildInterlockInstanceMap(records):
    """
    Build a raw-name -> sanitized-instance-name map and detect collisions.
    """
    instanceNameByRawName = {}
    rawNameByInstanceName = {}
    errors = []

    for record in list(records or []):
        if isinstance(record, dict):
            rawName = str(record.get("name") or "").strip()
        else:
            rawName = str(getattr(record, "name", "") or "").strip()
        if not rawName:
            continue

        instanceName = buildInterlockInstanceName(rawName)
        existingRawName = rawNameByInstanceName.get(instanceName)
        if existingRawName is not None and existingRawName != rawName:
            errors.append(
                "Interlock names [{}] and [{}] both map to Fleet/Interlocks/[{}]".format(
                    existingRawName,
                    rawName,
                    instanceName,
                )
            )
            continue

        rawNameByInstanceName[instanceName] = rawName
        instanceNameByRawName[rawName] = instanceName

    return instanceNameByRawName, rawNameByInstanceName, errors
